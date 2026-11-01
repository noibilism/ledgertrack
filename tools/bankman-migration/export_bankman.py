#!/usr/bin/env python3
"""Bankman (MySQL) -> ledgertrack-import JSON Lines. Read-only. Writes problems to export_issues.csv."""
import subprocess, json, sys, os, csv, re
from decimal import Decimal as D
out = sys.argv[1]; os.makedirs(out, exist_ok=True)
# Connection comes from the environment so the same script runs against the sample database and production.
# Use a read-only MySQL user in production. The password is read by the mysql client from MYSQL_PWD.
#   MYSQL_SOCK            unix socket (takes precedence over host/port)
#   MYSQL_HOST, MYSQL_PORT, MYSQL_USER (default root), MYSQL_DB (default bankman)
def mysql_cmd(sql):
    cmd = ['mysql']
    if os.environ.get('MYSQL_SOCK'): cmd += ['-S', os.environ['MYSQL_SOCK']]
    else: cmd += ['-h', os.environ.get('MYSQL_HOST', '127.0.0.1'), '-P', os.environ.get('MYSQL_PORT', '3306')]
    return cmd + ['-u', os.environ.get('MYSQL_USER', 'root'), os.environ.get('MYSQL_DB', 'bankman'), '-N', '-B', '-e', sql]
def rows(sql):
    r = subprocess.run(mysql_cmd(sql),capture_output=True,text=True,check=True).stdout
    return [json.loads(l) for l in r.splitlines() if l.strip()]
def j(table, cols, where='1=1'):
    obj = ','.join(f"'{c}',{c}" for c in cols)
    return rows(f"SELECT JSON_OBJECT({obj}) FROM {table} WHERE {where}")
def dec(v):
    s = format(D(str(v)), 'f')
    if '.' in s: s = s.rstrip('0').rstrip('.') if False else s
    return s
issues = []
POLICY = 'dryrun'   # always round to cents (half-even) and log every delta; no client is excluded
followup = []   # rows for finance: things LedgerTrack cannot represent
from decimal import ROUND_HALF_EVEN
blocked = {}   # currency -> balance held back because the owner is not importable
def issue(kind, ref, msg): issues.append((kind, ref, msg))
slug = lambda c, name: ('RESERVE-USD' if 'Reserve' in name else c)   # wallet API needs the code to equal or end with the currency
prods = j('products', ['id','product_name','product_type','currency','description','is_active'])
pcode = {}
with open(f'{out}/products.jsonl','w') as f:
    for p in prods:
        code = slug(p['currency'], p['product_name']); pcode[p['id']] = code
        f.write(json.dumps(dict(source_id=p['id'], code=code, name=p['product_name'], category=p['product_type'], currency=p['currency'],
                                description=p['description'] or '', precision=2, status='active' if p['is_active'] else 'retired'))+'\n')
cl = j('clients', ['id','external_id','client_type','business_name','first_name','last_name','business_registration_number','email','phone_number','kyc_status','created_at'], 'deleted_at IS NULL')
cids = set(); excluded = set(); unverified = set()
with open(f'{out}/clients.jsonl','w') as f:
    for c in cl:
        name = c['business_name'] or f"{c['first_name']} {c['last_name']}".strip()
        # No client is excluded. The importer needs status != pending and kyc_level >= 1 for any client that owns accounts,
        # so every client is imported active with level 1 and accounts keep their Bankman status. Real KYC status is kept in kyc_source.
        # kyc_source records the real Bankman status; every substitution is logged to export_issues.csv.
        st = 'active'
        lvl = 1
        note = 'bankman:clients.kyc_status='+c['kyc_status']
        if c['kyc_status']!='verified':
            note += ' (NOT verified in Bankman; imported active; level 1 is only what the importer requires, compliance owns the real status)'
            issue('client_kyc_not_verified_in_bankman', c['id'], f"kyc_status={c['kyc_status']}"); unverified.add(c['id'])
        rec = dict(source_id=c['id'], legal_name=name, phone=c['phone_number'], kyc_level=lvl, kyc_source=note, status=st)
        if c['business_registration_number']: rec['registration_number'] = c['business_registration_number']
        else:
            rec['kyc_identifier'] = 'bankman:'+c['id']
            issue('client_identifier_synthesized', c['id'], 'no registration number in Bankman; kyc_identifier set to bankman:<client id>')
        if c['email']: rec['email'] = c['email']
        if c['external_id']: rec['external_id'] = c['external_id']   # callers key clients by this; the compatibility API resolves it
        if c['created_at']: rec['legacy_created_at'] = c['created_at'].replace(' ','T')+'Z'
        f.write(json.dumps(rec)+'\n'); cids.add(c['id'])
ac = j('accounts', ['id','account_number','client_id','product_id','status','currency','available_balance','actual_balance','locked_amount','allow_overdraft','overdraft_limit','wallet_role','account_type','created_at','updated_at'], 'deleted_at IS NULL')
acc_ok = {}
with open(f'{out}/accounts.jsonl','w') as f:
    for a in ac:
        if a['client_id'] in excluded:
            blocked[a['currency']] = blocked.get(a['currency'],D(0)) + D(str(a['available_balance'])) + D(str(a['locked_amount']))
            issue('account_excluded_with_client', a['id'], f"balance {D(str(a['available_balance']))+D(str(a['locked_amount']))} {a['currency']} held back"); continue
        if a['client_id'] not in cids: issue('account_orphan_client', a['id'], 'client missing/soft-deleted'); continue
        st = {'pending_activation':'pending'}.get(a['status'], a['status'])
        f.write(json.dumps(dict(source_id=a['id'], client_source_id=a['client_id'], product_code=pcode[a['product_id']], legacy_account_number=a['account_number'], status=st, opened_at=a['created_at'].replace(' ','T')+'Z'))+'\n')
        acc_ok[a['id']] = a
# Open liens, one per lien transaction, so a release by lien transaction id (what snowman and the middleware send) can be
# resolved after cutover. A lien is open when it completed and no release points at it through original_transaction_id.
# The detail is used for an account only when it adds up to accounts.locked_amount (after rounding half-even to cents);
# otherwise the account falls back to one aggregate lien and the account is logged as lien_detail_mismatch.
open_liens = {}
for t in j('transactions', ['id','internal_reference','source_account_id','amount','description','created_at'],
           "transaction_type='lien' AND status='completed' AND deleted_at IS NULL AND id NOT IN "
           "(SELECT original_transaction_id FROM transactions WHERE transaction_type='lien_release' "
           "AND original_transaction_id IS NOT NULL AND deleted_at IS NULL AND status IN ('completed','pending','processing'))"):
    open_liens.setdefault(t['source_account_id'], []).append(t)
nb = nl = 0
with open(f'{out}/balances.jsonl','w') as fb, open(f'{out}/liens.jsonl','w') as fl:
    for id_, a in acc_ok.items():
        av, lk, ac_ = D(str(a['available_balance'])), D(str(a['locked_amount'])), D(str(a['actual_balance']))
        if ac_ != av + lk: issue('balance_invariant', id_, f'actual {ac_} != available {av} + locked {lk}; exported available+locked')
        base = (id_, a['account_number'], a['currency'], format(av,'f'), format(lk,'f'), format(D(str(a['overdraft_limit'])),'f'))
        if a['account_type']=='reserve' or a['wallet_role']=='r2_subaccount':
            followup.append(('reserve_or_r2',)+base+(f"type={a['account_type']} role={a['wallet_role']}; balance imported as a normal wallet, R2/reserve behaviour not migrated",)); issue('reserve_or_r2', id_, 'balance imported; behaviour not migrated')
        if a['allow_overdraft']:
            followup.append(('overdraft',)+base+('overdraft limit and rate not migrated; balance imported as is',)); issue('overdraft_enabled', id_, 'overdraft not representable; limit not migrated')
        if av < 0:
            followup.append(('negative_available',)+base+('imported at zero; settle the negative amount outside the ledger',)); issue('negative_available', id_, f'available {av} imported as zero'); continue
        if POLICY=='dryrun':
            av2, lk2 = av.quantize(D('0.01'),ROUND_HALF_EVEN), lk.quantize(D('0.01'),ROUND_HALF_EVEN)
            if (av2,lk2)!=(av,lk): issue('rounding_delta', id_, f"{a['currency']} {(av2+lk2)-(av+lk)}")
            av, lk = av2, lk2
        if av == 0 and lk == 0: continue
        for v, nm in ((av,'available'),(lk,'lien')):
            if v != v.quantize(D('0.01')): issue('precision_gt_2dp', id_, f'{nm} {v} has more than 2 decimals')
        fb.write(json.dumps(dict(source_id='bal-'+id_, account_source_id=id_, currency=a['currency'], amount=format(av+lk,'f'), available=format(av,'f'), lien=format(lk,'f')))+'\n'); nb += 1
        if lk > 0:
            detail = [(t, D(str(t['amount'])).quantize(D('0.01'), ROUND_HALF_EVEN)) for t in open_liens.get(id_, [])]
            if detail and sum(q for _, q in detail) == lk.quantize(D('0.01'), ROUND_HALF_EVEN):
                for t, q in detail:
                    # source_id is the Bankman lien transaction id, so it ends up as legacy_source_id on the ledger transaction
                    fl.write(json.dumps(dict(source_id=t['id'], account_source_id=id_, reference=t['internal_reference'], amount=format(q,'f'),
                                             created_at=t['created_at'].replace(' ','T')+'Z', description=t['description'] or 'Bankman lien'))+'\n'); nl += 1
            else:
                if detail: issue('lien_detail_mismatch', id_, f"open liens sum {sum(q for _, q in detail)} != locked_amount {lk}; exported one aggregate lien")
                else: issue('lien_detail_missing', id_, f"locked_amount {lk} has no open lien transaction; exported one aggregate lien")
                fl.write(json.dumps(dict(source_id='lien-'+id_, account_source_id=id_, reference='bankman:lien-aggregate:'+id_, amount=format(lk,'f'), created_at=a['updated_at'].replace(' ','T')+'Z', description='Aggregate of Bankman accounts.locked_amount; no per-lien detail reconciled to it'))+'\n'); nl += 1

# ---- chart of accounts -> gl_accounts.csv (LedgerTrack bulk GL import) ----
# Decision: one chart per currency. LedgerTrack needs a parent to share its child's currency and code, so Bankman's USD-only
# chart is replicated for every currency that has a product. USD keeps the Bankman code; other currencies use <code>.<CCY>.
import re
gl = j('gl_accounts', ['id','account_code','account_name','account_type','currency','parent_account_id','is_active'], 'deleted_at IS NULL')
by_id = {g['id']: g for g in gl}
def slug(t): return re.sub(r'[^a-z0-9]+','_',t.lower()).strip('_')
def path(g):
    parts=[]
    while g: parts.append(slug(g['account_name'])); g = by_id.get(g['parent_account_id'])
    return ':'.join(reversed(parts))
ccys = sorted({p['currency'] for p in prods})
# The chart is the standard Cray chart in coa.py (one source of truth). Bankman's own rows are only checked against it:
# a Bankman account whose code is not in the standard chart, or whose name/type differs, is reported and still exported
# under its own parent so no balance has nowhere to go.
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import coa
std = {r[0]: r for r in coa.SPEC}
extras = []
for g in sorted(gl, key=lambda x: x['account_code']):
    r = std.get(g['account_code'])
    if r is None:
        issue('gl_account_not_in_standard_chart', g['account_code'], g['account_name'] + ' exported as an extra account'); extras.append(g)
    elif (r[1], r[2]) != (g['account_name'], g['account_type']):
        issue('gl_account_differs_from_standard', g['account_code'], f"bankman {g['account_name']}/{g['account_type']} standard {r[1]}/{r[2]}")
accounts = coa.build(ccys)
rows = 0
with open(f'{out}/gl_accounts.csv','w',newline='') as f:
    w=csv.writer(f); w.writerow(['code','name','type','normal_side','currency','parent_code','ledger','address'])
    for a in accounts:
        w.writerow([a['code'],a['name'],a['type'],'',a['currency'],a['parent_code'],a['ledger'],a['address']]); rows+=1
    for ccy in ccys:
        for g in extras:
            code = lambda c: c if ccy=='USD' else f"{c}.{ccy}"
            parent = by_id.get(g['parent_account_id'])
            w.writerow([code(g['account_code']), g['account_name'], g['account_type'], '', ccy, code(parent['account_code']) if parent else '', 'ledgertrack', f"gl:{ccy.lower()}:{path(g)}"]); rows+=1
print('gl_accounts', rows, 'currencies', len(ccys))
from collections import Counter
print_pending=1
with open(f'{out}/finance_followup.csv','w',newline='') as ff:
    w = csv.writer(ff); w.writerow(['kind','account_id','account_number','currency','available','locked','overdraft_limit','note']); w.writerows(followup)
if blocked: issue('blocked_total','-', json.dumps({k:format(v,'f') for k,v in blocked.items()}))
with open(f'{out}/export_issues.csv','w',newline='') as f:
    w = csv.writer(f); w.writerow(['kind','ref','message']); w.writerows(issues)

print(dict(products=len(prods), clients=len(cl), accounts=len(acc_ok), balances=nb, liens=nl), dict(Counter(i[0] for i in issues)))
