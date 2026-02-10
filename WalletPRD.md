Act as

A Senior Fintech Ledger Architect with deep experience designing high‑scale, concurrency‑safe wallet systems on top of double‑entry ledgers (Formance) supporting 100+ currencies.

⸻

Context

We are using Formance strictly as the core ledger, not as a wallet product.

We are building a thin but strict Wallet Wrapper layer whose sole responsibility is to safely translate wallet operations into ledger movements while guaranteeing:
	•	Balance integrity
	•	Idempotency
	•	Concurrency safety
	•	Auditability

The wrapper must support 100+ currencies and be safe under retries, parallel requests, async workers, and partial failures.

⸻

Scope (What the Wrapper MUST support)

The wrapper must support only the following wallet operations (no more, no less):
	1.	Create a currency wallet
	2.	Credit a wallet
	3.	Debit a wallet
	4.	Lien (hold) funds on a wallet
	5.	Release a lien and optionally debit

Everything else (FX, payouts, fees, settlements) will be layered later on top of these primitives.

⸻

Core Design Principles (Non‑Negotiable)
	1.	Ledger is append‑only
	•	No balance overwrites
	•	No transaction mutation
	•	Reversals are explicit ledger entries
	2.	Idempotency is enforced at the wrapper
	•	Formance is never trusted to dedupe business intent
	3.	Concurrency is hostile by default
	•	Assume parallel credits/debits on the same wallet
	•	Assume retries, timeouts, and duplicate messages
	4.	Wallet = Logical construct
	•	Wallets are not balances
	•	Wallets resolve to ledger accounts via deterministic mapping

⸻

Currency Wallet Model

Currency Registry (Required)

Each currency must be defined in a registry with:
	•	currency_code
	•	precision
	•	enabled
	•	settleable

No wallet or transaction may execute unless the currency exists and is enabled.

⸻

Ledger Account Mapping Strategy

Each currency wallet maps to deterministic Formance accounts:

users/{user_id}/wallets/{currency}/available
users/{user_id}/wallets/{currency}/lien

System‑level control accounts:

system/control/{currency}

Rules:
	•	Available and lien balances are separate accounts
	•	A wallet’s usable balance = available − lien

⸻

Wrapper Operations (Authoritative API)

1. CreateCurrencyWallet(userId, currency)

Behavior
	•	Validates currency via registry
	•	Idempotent: multiple calls MUST produce the same ledger state
	•	Creates ledger accounts lazily (on first use)

Concurrency Rule
	•	Must be safe under parallel calls
	•	Account creation must be atomic or safely retryable

⸻

2. CreditWallet(walletId, amount, reference)

Ledger Movement
	•	Debit: system/control/{currency}
	•	Credit: users/{user_id}/wallets/{currency}/available

Rules
	•	reference is globally unique
	•	Duplicate reference MUST be a no‑op

⸻

3. DebitWallet(walletId, amount, reference)

Pre‑Checks
	•	Available balance ≥ amount

Ledger Movement
	•	Debit: users/{user_id}/wallets/{currency}/available
	•	Credit: system/control/{currency}

Failure Rule
	•	Insufficient funds MUST fail atomically

⸻

4. LienWallet(walletId, amount, reference)

Purpose
Temporarily reserve funds without debiting the wallet.

Ledger Movement
	•	Debit: users/{user_id}/wallets/{currency}/available
	•	Credit: users/{user_id}/wallets/{currency}/lien

Rules
	•	Lien reference MUST be unique
	•	A lien MUST never exceed available balance

⸻

5. ReleaseLien(walletId, reference, mode)

mode ∈ {release_only, release_and_debit}

a) Release Only
Ledger Movement
	•	Debit: users/{user_id}/wallets/{currency}/lien
	•	Credit: users/{user_id}/wallets/{currency}/available

b) Release and Debit
Ledger Movement
	•	Debit: users/{user_id}/wallets/{currency}/lien
	•	Credit: system/control/{currency}

⸻

Idempotency Model
	•	Every operation requires a client‑supplied reference
	•	Wrapper stores (reference → ledger_tx_id) mapping
	•	If reference exists:
	•	Return previous result
	•	DO NOT create new ledger entries

Idempotency keys are currency‑scoped but globally unique.

⸻

Concurrency Control Strategy

The wrapper MUST implement at least one of the following:
	•	Serializable database transactions around balance‑affecting operations
	•	Distributed lock per (walletId, currency)
	•	Optimistic concurrency with retry on conflict

Rules:
	•	Balance checks and ledger writes must be one atomic unit
	•	Never check balance outside the transaction boundary

⸻

Failure & Retry Guarantees
	•	All operations are safe to retry
	•	Partial failures MUST be detectable and replayable
	•	Ledger is the source of truth; caches are derived

⸻

Wallet Statements & Transaction History (Required)

The wrapper MUST expose statement and history APIs per currency wallet. These APIs are read-only and MUST NEVER mutate ledger state.

⸻

Statement Model

A wallet statement represents a ledger-derived view over a time range.

Each statement entry MUST include:
	•	timestamp
	•	reference
	•	operation_type (credit, debit, lien, release, release_and_debit)
	•	amount
	•	currency
	•	balance_before
	•	balance_after
	•	lien_before
	•	lien_after
	•	ledger_tx_id

Balances MUST be computed from ledger ordering, not cached counters.

⸻

Statement API

GetWalletStatement(walletId, currency, from, to, cursor, limit)

Rules:
	•	Results MUST be ordered by ledger commit time
	•	Pagination MUST be cursor-based (not offset-based)
	•	Statements MUST be reproducible at any point in time

⸻

Transaction History API

GetWalletHistory(walletId, currency, cursor, limit)

Rules:
	•	History is an append-only timeline
	•	No filtering that alters ordering
	•	Every history entry MUST map to exactly one ledger transaction

⸻

Ledger Query Strategy
	•	Statements MUST be built by querying Formance transactions by:
	•	Account prefix
	•	Currency
	•	Time window
	•	Wrapper MAY cache statement results, but cache MUST be invalidated on new ledger writes

⸻

Concurrency & Consistency Rules
	•	Statements MUST tolerate concurrent writes
	•	New transactions may appear between pages
	•	Duplicate or missing entries are forbidden

⸻

Hard Guarantees
	•	A statement MUST always reconcile to the ledger
	•	A statement MUST be reproducible for audit
	•	No derived balance may contradict ledger truth

⸻

Hard Invariants (Violations = SEV‑1)
	•	A wallet balance may never go negative
	•	A lien may never exceed available funds
	•	The same reference may never create two ledger entries
	•	Cross‑currency movement without FX logic is forbidden

⸻

Identity

Think like a fintech CTO protecting real money:
	•	Every race condition is a future incident
	•	Every retry path will be exercised
	•	Every invariant will be attacked