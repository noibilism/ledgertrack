"""Cray standard chart of accounts for LedgerTrack: one source of truth.

Every currency gets the same chart (decision of 1 Nov 2026). USD keeps the plain code, other currencies use
<code>.<CCY>. Address is gl:<ccy>:<path> except the migration clearing account, whose address is fixed by the importer
(gl:migration:opening:<CCY>).

Each row: code, name, type, parent, posting, owner, reconcile, purpose
  posting   True when postings may be made to the account; False for headers and control accounts.
  owner     the function accountable for the balance.
  reconcile what the balance is proved against ("none" = P&L line, reviewed through the margin report).
"""
import csv
import json
import re
import sys

ASSET, LIAB, EQ, INC, EXP = "asset", "liability", "equity", "income", "expense"
CLASS_DIGIT = {ASSET: "1", LIAB: "2", EQ: "3", INC: "4", EXP: "5"}

# Loaded on Railway on 1 Nov 2026 (23 accounts per currency). Their name, type, parent and address must never change.
EXISTING = {
    "100000", "101000", "101100", "101200", "102000", "102100", "102200", "103000", "103100",
    "200000", "201000", "201100", "201200", "400000", "401000", "401100", "401200", "401300",
    "402000", "402100", "500000", "501000", "501100",
}

H = "header"
# (code, name, type, parent, posting, owner, reconcile, purpose)
SPEC = [
    # ---------------- 1 ASSETS ----------------
    ("100000", "Assets", ASSET, None, False, "Finance", "none", "Class header"),
    ("101000", "Cash and Bank", ASSET, "100000", False, "Treasury", "none", "Group header"),
    ("101100", "Cash in Hand", ASSET, "101000", True, "Treasury", "cash_count", "Physical cash held"),
    ("101200", "Bank Account - Operations", ASSET, "101000", True, "Treasury", "bank_statement", "Default counter-account for deposits and payouts"),
    ("101300", "Bank Account - Collections", ASSET, "101000", True, "Treasury", "bank_statement", "Customer and merchant funds received"),
    ("101400", "Bank Account - Payouts", ASSET, "101000", True, "Treasury", "bank_statement", "Disbursement float for payouts"),
    ("101500", "Bank Account - Settlement", ASSET, "101000", True, "Treasury", "bank_statement", "Merchant settlement account"),
    ("101600", "Safeguarding Account", ASSET, "101000", True, "Treasury", "bank_statement", "Customer funds held in trust, ring-fenced from operations"),
    ("101700", "Regulatory Reserve Balance", ASSET, "101000", True, "Treasury", "bank_statement", "Balance held with the central bank or regulator as a reserve"),
    ("102000", "Customer Accounts", ASSET, "100000", False, "Finance", "none", "Group header"),
    ("102100", "Customer Deposits", ASSET, "102000", False, "Finance", "none", "Duplicate of liability 201000. Do not post until Finance confirms its purpose"),
    ("102200", "Customer Overdrafts", ASSET, "102000", True, "Risk", "overdraft_register", "Overdraft advances to customers"),
    ("103000", "Receivables", ASSET, "100000", False, "Finance", "none", "Group header"),
    ("103100", "Fee Receivables", ASSET, "103000", True, "Finance", "ageing_report", "Fees earned but not yet collected from the wallet"),
    ("103200", "Interest Receivable", ASSET, "103000", True, "Finance", "ageing_report", "Overdraft interest accrued, not yet capitalised"),
    ("103300", "Partner Receivables", ASSET, "103000", True, "Finance", "partner_statement", "Amounts due from payout and collection partners"),
    ("103400", "Chargeback Receivables", ASSET, "103000", True, "Risk", "ageing_report", "Chargebacks recoverable from merchants"),
    ("103500", "Input Tax Recoverable", ASSET, "103000", True, "Tax", "tax_return", "Tax charged to Cray on provider invoices and recoverable"),
    ("103900", "Other Receivables", ASSET, "103000", True, "Finance", "ageing_report", "Anything not covered above, reviewed monthly"),
    ("104000", "Processor Clearing", ASSET, "100000", False, "Treasury", "none", "Money owed to Cray by a processor before it settles to the bank"),
    ("104100", "Card Processor Clearing", ASSET, "104000", True, "Treasury", "processor_statement", "Card acquirer and scheme settlements"),
    ("104200", "Mobile Money Clearing", ASSET, "104000", True, "Treasury", "processor_statement", "Mobile money operator settlements"),
    ("104300", "Bank Transfer Clearing", ASSET, "104000", True, "Treasury", "processor_statement", "Instant payment and bank transfer scheme settlements"),
    ("104400", "Stablecoin Settlement Clearing", ASSET, "104000", True, "Treasury", "processor_statement", "Fiat leg awaiting settlement on the stablecoin rail"),
    ("104900", "Other Processor Clearing", ASSET, "104000", True, "Treasury", "processor_statement", "Processors without their own account yet"),
    ("105000", "Suspense", ASSET, "100000", False, "Operations", "none", "Items awaiting investigation. Every balance needs an owner and an age"),
    ("105100", "Unreconciled Debits", ASSET, "105000", True, "Operations", "suspense_review", "Money left Cray with no matching event"),
    ("105200", "Failed Payout Returns Pending", ASSET, "105000", True, "Operations", "suspense_review", "Returned payouts waiting to be re-credited or refunded"),
    ("106000", "FX Clearing", ASSET, "100000", False, "Treasury", "none", "Currency conversion legs. A credit balance is abnormal and flagged"),
    ("106100", "FX Conversion Clearing", ASSET, "106000", True, "Treasury", "zero_balance", "Customer conversion leg, cleared against the treasury trade"),
    ("106200", "FX Settlement Clearing", ASSET, "106000", True, "Treasury", "zero_balance", "Treasury trade leg awaiting settlement"),
    ("107000", "Partner Float and Prefunding", ASSET, "100000", False, "Treasury", "none", "Funds Cray has placed with partners to pay out"),
    ("107100", "Mobile Money Float", ASSET, "107000", True, "Treasury", "partner_statement", "Float held on mobile money operator wallets"),
    ("107200", "Payout Partner Prefunding", ASSET, "107000", True, "Treasury", "partner_statement", "Prefunded balances with payout partners"),
    ("107300", "Correspondent Bank Balances", ASSET, "107000", True, "Treasury", "bank_statement", "Nostro style balances with correspondent banks"),
    # ---------------- 2 LIABILITIES ----------------
    ("200000", "Liabilities", LIAB, None, False, "Finance", "none", "Class header"),
    ("201000", "Customer Deposits", LIAB, "200000", False, "Finance", "none", "Group header"),
    ("201100", "Current Accounts", LIAB, "201000", False, "Finance", "wallet_subledger", "Control total of customer wallets (available plus lien, including rolling reserve holds). Never posted to directly"),
    ("201200", "Savings Accounts", LIAB, "201000", False, "Finance", "wallet_subledger", "Control total of savings wallets. Never posted to directly"),
    ("202000", "Payables", LIAB, "200000", False, "Finance", "none", "Group header"),
    ("202100", "Provider Payables", LIAB, "202000", True, "Finance", "provider_invoice", "Processing cost accrued, not yet paid"),
    ("202200", "Tax Payable", LIAB, "202000", False, "Tax", "none", "Tax collected on fees and transactions. One child per tax so each is remitted from its own balance"),
    ("202210", "VAT Payable", LIAB, "202200", True, "Tax", "tax_return", "VAT collected on fees"),
    ("202220", "Withholding Tax Payable", LIAB, "202200", True, "Tax", "tax_return", "Withholding tax deducted"),
    ("202230", "Stamp Duty Payable", LIAB, "202200", True, "Tax", "tax_return", "Stamp duty collected"),
    ("202240", "Excise Duty Payable", LIAB, "202200", True, "Tax", "tax_return", "Excise duty collected on fees"),
    ("202250", "Transaction Levy Payable", LIAB, "202200", True, "Tax", "tax_return", "Levies charged on the transaction amount"),
    ("202290", "Other Taxes Payable", LIAB, "202200", True, "Tax", "tax_return", "Taxes without their own account yet"),
    ("202300", "Merchant Settlements Payable", LIAB, "202000", True, "Finance", "settlement_report", "Merchant funds held outside wallets (R2 settlement), if kept"),
    ("202400", "Partner Commissions Payable", LIAB, "202000", True, "Finance", "partner_statement", "Commissions and revenue share owed to partners and agents"),
    ("202500", "Refunds and Chargebacks Payable", LIAB, "202000", True, "Operations", "dispute_register", "Refunds and chargebacks agreed, not yet paid"),
    ("202600", "Interest Payable", LIAB, "202000", True, "Finance", "interest_accrual_report", "Interest accrued on customer deposits, not yet capitalised"),
    ("202700", "Regulatory Fees Payable", LIAB, "202000", True, "Compliance", "regulator_invoice", "Levies and fees owed to regulators"),
    ("203000", "Other Liabilities", LIAB, "200000", False, "Finance", "none", "Group header"),
    ("203100", "Unidentified Receipts", LIAB, "203000", True, "Operations", "suspense_review", "Funds received with no identifiable account. Owed back until claimed"),
    ("203200", "Unclaimed Funds Payable", LIAB, "203000", True, "Compliance", "unclaimed_register", "Dormant balances moved out of wallets, payable to the owner or the regulator"),
    # ---------------- 3 EQUITY ----------------
    ("300000", "Equity", EQ, None, False, "Finance", "none", "Class header"),
    ("301000", "Share Capital", EQ, "300000", True, "Finance", "none", "Paid-in capital"),
    ("302000", "Retained Earnings", EQ, "300000", True, "Finance", "none", "Prior period results, filled by period close"),
    ("303000", "Current Period Earnings", EQ, "300000", True, "Finance", "none", "Result of the open period, filled by period close"),
    ("304000", "Opening Balance Equity", EQ, "300000", False, "Finance", "none", "Group header"),
    ("304100", "Migration Opening Clearing", EQ, "304000", True, "Finance", "zero_balance", "Bankman opening position. Must net to zero at sign-off"),
    # ---------------- 4 INCOME ----------------
    ("400000", "Income", INC, None, False, "Finance", "none", "Class header"),
    ("401000", "Fee Income", INC, "400000", False, "Product", "none", "Group header"),
    ("401100", "Transaction Fees", INC, "401000", False, "Product", "none", "Per-transaction fees, split by line so margin can be read per line"),
    ("401110", "Collection Fees", INC, "401100", True, "Product", "margin_report", "Fees on money coming in"),
    ("401120", "Payout Fees", INC, "401100", True, "Product", "margin_report", "Fees on money going out"),
    ("401130", "Wallet Transfer Fees", INC, "401100", True, "Product", "margin_report", "Fees on wallet to wallet transfers"),
    ("401140", "Cross-Border Remittance Fees", INC, "401100", True, "Product", "margin_report", "Fees on cross-border sends"),
    ("401150", "Card Fees", INC, "401100", True, "Product", "margin_report", "Card transaction fees"),
    ("401190", "Other Transaction Fees", INC, "401100", True, "Product", "margin_report", "Transaction fees without their own line yet"),
    ("401200", "Service Charges", INC, "401000", False, "Product", "none", "Periodic and account fees"),
    ("401210", "Account Maintenance Fees", INC, "401200", True, "Product", "fee_run_report", "Maintenance fee run"),
    ("401290", "Other Service Charges", INC, "401200", True, "Product", "none", "Account and service fees without their own line yet"),
    ("401300", "Overdraft Fees", INC, "401000", True, "Risk", "overdraft_register", "Overdraft fees"),
    ("401400", "Penalty Fees", INC, "401000", True, "Product", "none", "Dormancy, returned item and other penalty fees"),
    ("401500", "Chargeback and Dispute Fees", INC, "401000", True, "Risk", "dispute_register", "Fees charged to merchants for chargebacks and disputes"),
    ("402000", "Interest Income", INC, "400000", False, "Finance", "none", "Group header"),
    ("402100", "Overdraft Interest", INC, "402000", True, "Risk", "overdraft_register", "Overdraft interest"),
    ("402200", "Interest on Bank Balances", INC, "402000", True, "Treasury", "bank_statement", "Interest earned on bank and safeguarding balances"),
    ("403000", "FX Income", INC, "400000", False, "Treasury", "none", "Group header"),
    ("403100", "FX Conversion Margin", INC, "403000", True, "Treasury", "fx_report", "Spread earned on customer conversions"),
    ("403200", "FX Realised Gains", INC, "403000", True, "Treasury", "fx_report", "Gains on settled treasury trades"),
    ("404000", "Partner Income", INC, "400000", False, "Finance", "none", "Group header"),
    ("404100", "Partner Commission Income", INC, "404000", True, "Finance", "partner_statement", "Commissions earned from partners and providers"),
    ("405000", "Other Income", INC, "400000", False, "Finance", "none", "Group header"),
    ("405100", "Recoveries", INC, "405000", True, "Finance", "none", "Amounts recovered after write-off"),
    ("405900", "Miscellaneous Income", INC, "405000", True, "Finance", "none", "Reviewed monthly, anything recurring gets its own account"),
    # ---------------- 5 EXPENSES ----------------
    ("500000", "Expenses", EXP, None, False, "Finance", "none", "Class header"),
    ("501000", "Transaction Costs", EXP, "500000", False, "Finance", "none", "Group header"),
    ("501100", "Payment Processing Fees", EXP, "501000", False, "Finance", "none", "Provider and bank costs, split by line to match fee income 4011xx"),
    ("501110", "Collection Costs", EXP, "501100", True, "Finance", "provider_invoice", "Cost of money coming in"),
    ("501120", "Payout Costs", EXP, "501100", True, "Finance", "provider_invoice", "Cost of money going out"),
    ("501130", "Wallet Transfer Costs", EXP, "501100", True, "Finance", "provider_invoice", "Cost of wallet transfers, normally zero"),
    ("501140", "Cross-Border Remittance Costs", EXP, "501100", True, "Finance", "provider_invoice", "Cost of cross-border sends"),
    ("501150", "Card Costs", EXP, "501100", True, "Finance", "provider_invoice", "Card scheme and acquirer costs"),
    ("501190", "Other Processing Costs", EXP, "501100", True, "Finance", "provider_invoice", "Costs without their own line yet"),
    ("501200", "Bank Charges", EXP, "501000", True, "Treasury", "bank_statement", "Charges the banks debit to Cray"),
    ("501300", "Verification and KYC Costs", EXP, "501000", True, "Compliance", "provider_invoice", "Per-check costs of identity and screening providers"),
    ("502000", "Adjustments and Concessions", EXP, "500000", False, "Finance", "none", "Group header"),
    ("502100", "Fee Waivers and Concessions", EXP, "502000", True, "Product", "approval_log", "Fees waived for a customer, so the revenue effect stays visible"),
    ("502200", "Promotions and Cashback", EXP, "502000", True, "Product", "approval_log", "Credits given to wallets as incentives"),
    ("502300", "Customer Compensation", EXP, "502000", True, "Operations", "approval_log", "Goodwill and service failure payments"),
    ("502900", "Rounding Differences", EXP, "502000", True, "Finance", "zero_balance", "Sub-cent deltas from migration rounding. Later drift is a control failure"),
    ("503000", "Interest Expense", EXP, "500000", False, "Finance", "none", "Group header"),
    ("503100", "Interest on Customer Deposits", EXP, "503000", True, "Finance", "interest_accrual_report", "Interest paid or accrued to customers"),
    ("504000", "Losses and Write-offs", EXP, "500000", False, "Risk", "none", "Group header"),
    ("504100", "Fraud Losses", EXP, "504000", True, "Risk", "incident_register", "Confirmed fraud losses"),
    ("504200", "Chargeback Losses", EXP, "504000", True, "Risk", "dispute_register", "Chargebacks not recovered"),
    ("504300", "Fee Write-offs", EXP, "504000", True, "Finance", "ageing_report", "Fee receivables written off (status writeoff_required)"),
    ("504400", "Overdraft Write-offs", EXP, "504000", True, "Risk", "overdraft_register", "Overdrafts written off"),
    ("504500", "Reconciliation Break Write-offs", EXP, "504000", True, "Finance", "suspense_review", "Suspense items written off after investigation, with approval"),
    ("504600", "FX Realised Losses", EXP, "504000", True, "Treasury", "fx_report", "Losses on settled treasury trades"),
    ("505000", "Regulatory Costs", EXP, "500000", False, "Compliance", "none", "Group header"),
    ("505100", "Regulatory Levies and Fees", EXP, "505000", True, "Compliance", "regulator_invoice", "Levies and fees expensed to regulators"),
]

MIGRATION_CODE = "304100"


def slug(text):
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def code_for(code, ccy):
    return code if ccy == "USD" else f"{code}.{ccy}"


def build(ccys):
    """Return one dict per account, in parent-before-child order, for every currency."""
    by_code = {row[0]: row for row in SPEC}

    def path(code):
        parts = []
        while code:
            row = by_code[code]
            parts.append(slug(row[1]))
            code = row[3]
        return ":".join(reversed(parts))

    def level(code):
        n = 0
        while by_code[code][3]:
            code = by_code[code][3]
            n += 1
        return n + 1

    out = []
    for ccy in ccys:
        for code, name, typ, parent, posting, owner, reconcile, purpose in SPEC:
            if code == MIGRATION_CODE:
                address = f"gl:migration:opening:{ccy}"
            else:
                address = f"gl:{ccy.lower()}:{path(code)}"
            out.append({
                "code": code_for(code, ccy), "name": name, "type": typ, "currency": ccy,
                "parent_code": code_for(parent, ccy) if parent else "",
                "ledger": "ledgertrack", "address": address,
                "existing": code in EXISTING,
                "metadata": {"level": level(code), "posting": posting, "owner": owner,
                             "reconcile": reconcile, "purpose": purpose},
            })
    return out


CODE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
SEG_RE = re.compile(r"^[a-zA-Z0-9_-]+$")


def check(accounts):
    """Structural checks that the registry also enforces, plus the numbering and accountability rules."""
    errors = []
    by_code = {a["code"]: a for a in accounts}
    seen_addr = {}
    children = {}
    for a in accounts:
        if not CODE_RE.match(a["code"]):
            errors.append(f"{a['code']}: bad code")
        if a["address"] in seen_addr:
            errors.append(f"{a['code']}: address {a['address']} also used by {seen_addr[a['address']]}")
        seen_addr[a["address"]] = a["code"]
        if not a["address"].startswith("gl:") or not all(SEG_RE.match(s) for s in a["address"].split(":")):
            errors.append(f"{a['code']}: bad address {a['address']}")
        base = a["code"].split(".")[0]
        if base[0] != CLASS_DIGIT[a["type"]]:
            errors.append(f"{a['code']}: first digit does not match type {a['type']}")
        p = a["parent_code"]
        if p:
            if p not in by_code:
                errors.append(f"{a['code']}: parent {p} missing")
                continue
            par = by_code[p]
            if par["type"] != a["type"] or par["currency"] != a["currency"]:
                errors.append(f"{a['code']}: parent type/currency differs")
            if int(par["code"].split(".")[0]) >= int(base):
                errors.append(f"{a['code']}: code is not above its parent {p}")
            children.setdefault(p, []).append(a["code"])
    for a in accounts:
        m = a["metadata"]
        has_kids = a["code"] in children
        if has_kids and m["posting"]:
            errors.append(f"{a['code']}: has children but allows posting")
        if not has_kids and not m["posting"] and a["code"].split(".")[0] not in ("102100", "201100", "201200"):
            errors.append(f"{a['code']}: leaf that does not allow posting and is not a known control/hold account")
        if m["posting"] and m["owner"] == "":
            errors.append(f"{a['code']}: no owner")
        if m["posting"] and m["reconcile"] == "none" and a["type"] in ("asset", "liability"):
            errors.append(f"{a['code']}: balance-sheet posting account with nothing to reconcile to")
    names = {}
    for a in accounts:
        key = (a["currency"], a["parent_code"], a["name"].lower())
        if key in names:
            errors.append(f"{a['code']}: duplicate sibling name {a['name']}")
        names[key] = a["code"]
    return errors


if __name__ == "__main__":
    ccys = sys.argv[1].split(",") if len(sys.argv) > 1 else ["USD"]
    acc = build(ccys)
    errs = check(acc)
    print(len(SPEC), "per currency;", len(acc), "total;", len(errs), "errors")
    for e in errs:
        print(" ", e)
