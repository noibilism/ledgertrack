import random, uuid, datetime as dt, hashlib, json
from decimal import Decimal as D
random.seed(42)
NOW=dt.datetime(2026,10,31,12,0,0)
def q(v):
    if v is None: return 'NULL'
    if isinstance(v,(int,float,D)): return str(v)
    if isinstance(v,dt.datetime): return "'"+v.strftime('%Y-%m-%d %H:%M:%S')+"'"
    if isinstance(v,dt.date): return "'"+v.isoformat()+"'"
    return "'"+str(v).replace("\\","\\\\").replace("'","''")+"'"
rows={}
def add(t,**kw): rows.setdefault(t,[]).append(kw)
uid=lambda: str(uuid.uuid4())
# ---- GL (real 23 USD accounts from staging)
gl=[l.split('\t') for l in open('/home/claude/bankman_dump.txt').read().split('== GL\n')[1].split('== PRODUCTS')[0].strip().split('\n')[1:]]
GL={}
gl.append(['5','102000','Customer Accounts','asset','USD','1','Customer account balances'])
for r in sorted(gl,key=lambda x:int(x[0])):
    add('gl_accounts',id=int(r[0]),account_code=r[1],account_name=r[2],account_type=r[3],currency=r[4],parent_account_id=None if r[5]=='NULL' else int(r[5]),description=r[6],is_active=1,current_balance=D('0'),created_at=NOW,updated_at=NOW)
    GL[r[1]]=int(r[0])
# ---- products (real: names/currencies)
prods=[('Default Wallet','wallet','USD','United States Dollar'),('Rolling Reserve','escrow','USD','United States Dollar'),
('Default NGN Wallet','wallet','NGN','Nigerian Naira'),('Default XAF Wallet','wallet','XAF',None),('Default MZN Wallet','wallet','MZN',None),
('Default ZAR Wallet','wallet','ZAR',None),('Default KES Wallet','wallet','KES','Kenyan Shilling'),('Default GHS Wallet','wallet','GHS','Ghanaian Cedi'),
('Default TZS Wallet','wallet','TZS','Tanzanian Shilling'),('Default XOF Wallet','wallet','XOF','West African CFA Franc'),('Default ZMW Wallet','wallet','ZMW','Zambian Kwacha')]
P={}
for n,t,c,cn in prods:
    pid=uid(); P[n]=(pid,c)
    add('products',id=pid,product_name=n,product_type=t,currency=c,currency_name=cn,minimum_amount=D('0'),is_active=1,description=n,created_at=NOW,updated_at=NOW)
wallet_for={c:n for n,t,c,cn in prods if t=='wallet'}
# ---- clients
CUR=['USD','NGN','KES','GHS','ZAR','XOF','TZS','ZMW']
clients=[]
for i in range(32):
    cid=uid(); org=i<30
    owner=clients[random.randrange(0,10)]['id'] if (org and i>=24) else None
    c=dict(id=cid,external_id=uid(),client_type='organization' if org else 'individual',owner_client_id=owner,
      business_name=f'Merchant {i:03d} Ltd' if org else None,business_registration_number=f'RC{100000+i}' if org else None,
      first_name=None if org else f'Person{i}',last_name=None if org else 'Test',
      email=f'merchant{i}@example.test',phone_number=f'+23480000{i:05d}',kyc_status=random.choice(['verified']*8+['pending','rejected']),
      created_at=NOW-dt.timedelta(days=random.randint(60,400)),updated_at=NOW)
    clients.append(c); add('clients',**c)
# ---- accounts
accts=[]
for c in clients:
    curs=random.sample(CUR,random.randint(1,4))
    if 'USD' not in curs and random.random()<.5: curs.append('USD')
    for cur in curs:
        pid=P[wallet_for[cur]][0]
        st=random.choices(['active','dormant','suspended','closed','pending_activation'],[88,4,3,2,3])[0]
        a=dict(id=uid(),account_number=str(random.randint(10**9,10**10-1)),client_id=c['id'],product_id=pid,account_name=f"{c.get('business_name') or c['first_name']} {cur}",
          currency=cur,country_code='NG',account_type='main',wallet_role='merchant_main',available_balance=D('0'),actual_balance=D('0'),locked_amount=D('0'),
          status=st,allow_overdraft=0,overdraft_limit=D('0'),overdraft_interest_rate=D('0'),created_at=NOW-dt.timedelta(days=random.randint(30,300)),updated_at=NOW)
        accts.append(a)
        if cur=='USD' and random.random()<.35:
            r=dict(a); r.update(id=uid(),account_number=str(random.randint(10**9,10**10-1)),product_id=P['Rolling Reserve'][0],account_type='reserve',wallet_role=None,account_name=a['account_name']+' reserve')
            accts.append(r)
for a in random.sample(accts,3): a['allow_overdraft']=1; a['overdraft_limit']=D('5000.0000')
active=[a for a in accts if a['status']=='active']
byc={}
for a in active: byc.setdefault(a['currency'],[]).append(a)
# ---- simulate transactions
seq=0; txns=[]; journals=[]; items=[]; ops=[]
glnet={}
def post_journal(t,dr,cr,amt,cur):
    jid=uid(); en=f'JE-{len(journals)+1:07d}'
    journals.append(dict(id=jid,entry_number=en,entry_date=t['created_at'].date(),reference_type='transaction',reference_id=t['id'],currency=cur,description=t['transaction_type'],status='posted',posted_at=t['created_at'],created_at=t['created_at'],updated_at=t['created_at']))
    items.append(dict(id=uid(),journal_entry_id=jid,gl_account_id=GL[dr],debit_amount=amt,credit_amount=D('0'),created_at=t['created_at'],updated_at=t['created_at']))
    items.append(dict(id=uid(),journal_entry_id=jid,gl_account_id=GL[cr],debit_amount=D('0'),credit_amount=amt,created_at=t['created_at'],updated_at=t['created_at']))
    glnet[dr]=glnet.get(dr,D(0))+amt; glnet[cr]=glnet.get(cr,D(0))-amt
    return jid
N=16000
# time curve: 60 days, one 20-minute spike
base=NOW-dt.timedelta(days=60)
times=sorted(base+dt.timedelta(seconds=random.random()*60*86400) for _ in range(N-1500))
spike=NOW-dt.timedelta(days=9,hours=3)
times+=[spike+dt.timedelta(seconds=random.random()*1200) for _ in range(1500)]
times.sort()
hot=lambda lst: lst[min(int(random.paretovariate(1.2))-1,len(lst)-1)]
completed_dep=[]
for ts in times:
    cur=random.choices(list(byc),[30,35,15,6,5,5,2,2][:len(byc)] if len(byc)==8 else None)[0]
    pool=byc[cur]; src=hot(pool)
    typ=random.choices(['deposit','withdrawal','transfer','charge','lien','lien_release','reversal'],[40,22,18,8,5,4,3])[0]
    amt=D(random.choice([10,25,50,100,250,500,1000,2500,10000]))*D(random.choice([1,1,1,1.5,2.5]))+D(random.randint(0,99))/100
    if cur=='NGN': amt*=1000
    amt=amt.quantize(D('0.01'))
    if random.random()<.004: amt=amt+D('0.0050')   # >2dp anomaly
    seq+=1; tid=uid(); dest=None; orig=None; status='completed'
    b=dict(l=src['actual_balance'],k=src['locked_amount'],a=src['available_balance'])
    if typ=='deposit': src['available_balance']+=amt; src['actual_balance']+=amt
    elif typ in('withdrawal','charge'):
        if src['available_balance']<amt: typ='deposit'; src['available_balance']+=amt; src['actual_balance']+=amt
        else: src['available_balance']-=amt; src['actual_balance']-=amt
    elif typ=='transfer':
        dest=hot([x for x in pool if x is not src] or pool)
        if dest is src or src['available_balance']<amt: typ='deposit'; dest=None; src['available_balance']+=amt; src['actual_balance']+=amt
        else:
            src['available_balance']-=amt; src['actual_balance']-=amt; dest['available_balance']+=amt; dest['actual_balance']+=amt
    elif typ=='lien':
        if src['available_balance']<amt: typ='deposit'; src['available_balance']+=amt; src['actual_balance']+=amt
        else: src['available_balance']-=amt; src['locked_amount']+=amt
    elif typ=='lien_release':
        if src['locked_amount']<=0: typ='deposit'; src['available_balance']+=amt; src['actual_balance']+=amt
        else: amt=min(amt,src['locked_amount']); src['locked_amount']-=amt; src['available_balance']+=amt
    elif typ=='reversal':
        if not completed_dep: typ='deposit'; src['available_balance']+=amt; src['actual_balance']+=amt
        else:
            o=completed_dep.pop(random.randrange(len(completed_dep)))
            oa=next(x for x in accts if x['id']==o['source_account_id']); amt=o['amount']; cur=o['currency']; src=oa; orig=o['id']
            b=dict(l=src['actual_balance'],k=src['locked_amount'],a=src['available_balance'])
            src['available_balance']-=amt; src['actual_balance']-=amt
    r=random.random()
    if r<.02 and typ in('deposit','withdrawal'): status='failed'
    elif r<.025: status=random.choice(['pending','processing','awaiting_compliance'])
    t=dict(id=tid,sequence_number=seq,internal_reference=f'BNK{seq:010d}',external_reference=f'EXT{random.randint(1,N//2):08d}' if random.random()<.8 else None,
      transaction_type=typ,processing_type='intra' if typ=='transfer' else random.choice(['inter','inter','intra']),processing_channel=random.choice(['card','bank_transfer','mobile_money','wallet','payout']),
      source_account_id=src['id'],destination_account_id=dest['id'] if dest else None,currency=cur,amount=amt,
      source_ledger_balance_before=b['l'],source_locked_balance_before=b['k'],source_available_balance_before=b['a'],
      source_ledger_balance_after=src['actual_balance'],source_locked_balance_after=src['locked_amount'],source_available_balance_after=src['available_balance'],
      status=status,original_transaction_id=orig,created_by_user_id=uid(),created_at=ts,updated_at=ts)
    if status=='failed':  # failed: no effect
        src['available_balance']=b['a'];src['actual_balance']=b['l'];src['locked_amount']=b['k']
        if dest: pass
        t['source_ledger_balance_after']=b['l'];t['source_available_balance_after']=b['a'];t['source_locked_balance_after']=b['k']
    txns.append(t)
    if status=='completed':
        if typ=='deposit': completed_dep.append(dict(id=tid,source_account_id=src['id'],amount=amt,currency=cur))
        if random.random()>.03:   # 3% missing journal = anomaly
            pairs={'deposit':('101200','201100'),'withdrawal':('201100','101200'),'transfer':('201100','201100'),'charge':('201100','401100'),'lien':('201100','201100'),'lien_release':('201100','201100'),'reversal':('201100','101200')}
            dr,cr=pairs[typ]
            if dr!=cr or typ=='transfer': post_journal(t,dr,cr,amt,cur)
# failed transfers also touched dest -> keep simple: undo handled above only for src (dest unchanged since we don't apply on failure)
# NOTE: for failed transfer we applied dest credit; undo it
for t in txns:
    pass
# wallet ops (idempotency table)
for i in range(2500):
    t=random.choice(txns)
    ops.append(dict(id=uid(),caller_service=random.choice(['gateman','payman','opsman']),idempotency_key_hash=hashlib.sha256(f'k{i}'.encode()).hexdigest(),fingerprint=hashlib.sha256(f'f{i}'.encode()).hexdigest(),
      owner_external_id=uid(),currency=t['currency'],amount=t['amount'],state='completed',direction=random.choice(['debit','credit']),account_id=t['source_account_id'],transaction_id=t['id'],created_at=t['created_at'],updated_at=t['created_at']))
for o in random.sample(ops,4): o['state']='reconciliation_required'; o['transaction_id']=None
for o in random.sample(ops,6): 
    if o['state']=='completed': o['state']='failed_no_effect'
# anomalies on accounts
bad=random.sample(active,2)
bad[0]['actual_balance']+=D('12.3400'); bad[1]['locked_amount']+=D('5.0000')
neg=random.choice([a for a in active if not a['allow_overdraft'] and a not in bad]); neg['available_balance']=D('-250.0000'); neg['actual_balance']=neg['available_balance']+neg['locked_amount']
# daily aggregates
agg={}
for t in txns:
    if t['status']!='completed': continue
    k=(t['source_account_id'],t['created_at'].date()); a=agg.setdefault(k,[D(0),D(0)])
    if t['transaction_type']=='deposit': a[0]+=t['amount']
    elif t['transaction_type'] in('withdrawal','transfer'): a[1]+=t['amount']
for (acc,d),(c_,d_) in agg.items(): add('transaction_aggregates',id=uid(),account_id=acc,aggregated_daily_amount=c_+d_,collections_amount=c_,disbursements_amount=d_,date=d,created_at=NOW,updated_at=NOW)
for a in accts: add('accounts',**a)
for t in txns: add('transactions',**t)
for j in journals: add('journal_entries',**j)
for i in items: add('journal_entry_items',**i)
for o in ops: add('internal_wallet_operations',**o)
for code,net in glnet.items():
    g=next(x for x in rows['gl_accounts'] if x['account_code']==code); g['current_balance']=net
rows['gl_accounts'][0]['current_balance']+=D('10.0000')   # GL control anomaly
# workflows
for s in ['completed']*50+['running']*3+['failed']*2: add('workflows',status=s,created_at=NOW)
# R2 tables stay empty; charges
cg=uid(); add('charges',id=cg,name='Transaction fee flat',charge_type='flat',txn_type='transfer',amount=D('25'),currency='NGN',is_active=1,gl_income_account_id=GL['401100'],created_at=NOW,updated_at=NOW)
cg2=uid(); add('charges',id=cg2,name='Deposit fee pct',charge_type='percentage',txn_type='deposit',percentage=D('1.5'),currency='USD',is_active=1,gl_income_account_id=GL['401100'],created_at=NOW,updated_at=NOW)
add('charges',id=uid(),name='Tiered withdrawal',charge_type='tiered',txn_type='withdrawal',currency='KES',is_active=1,gl_income_account_id=GL['401100'],created_at=NOW,updated_at=NOW)
add('account_charges',id=uid(),account_id=accts[0]['id'],charge_id=cg,is_active=1,created_by_user_id=uid(),created_at=NOW,updated_at=NOW)
with open('/home/claude/bm/seed.sql','w') as f:
    f.write('SET FOREIGN_KEY_CHECKS=0;\n')
    for t,rs in rows.items():
        ks=list(rs[0].keys())
        # normalise keys across rows
        allk=[]
        for r in rs:
            for k in r:
                if k not in allk: allk.append(k)
        for i in range(0,len(rs),500):
            f.write(f'INSERT INTO `{t}` ('+','.join(f'`{k}`' for k in allk)+') VALUES\n'+',\n'.join('('+','.join(q(r.get(k)) for k in allk)+')' for r in rs[i:i+500])+';\n')
print({t:len(r) for t,r in rows.items()})
