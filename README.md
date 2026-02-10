<p align="center">
  <img src="https://formance01.b-cdn.net/Github-Attachements/banners/ledger-readme-banner.webp" alt="ledger" width="100%" />
</p>

# LedgerTrack (Forked from Formance Ledger)

**LedgerTrack** is an enhanced, simplified fork of the Formance Ledger, optimized specifically for **building scalable wallet systems**. We've taken the robust core of Formance and added a high-level API layer that abstracts away the complexities of double-entry accounting, making it easy to manage user wallets, payment channels, and multi-ledger reconciliation.

## Why LedgerTrack?

Building a wallet system is hard. You need to handle:
*   **Double-Entry Accounting:** Ensuring every debit has a credit.
*   **Concurrency:** Handling high-volume transactions without race conditions.
*   **Multi-Ledger Operations:** Tracking user balances separately from payment provider (channel) liquidity and revenue.
*   **Liens & Holds:** Locking funds for pending transactions.

LedgerTrack solves this by providing a **"Wallet Wrapper"** API on top of the programmable ledger core.

## Key Features

### 1. Wallets as First-Class Citizens
Forget about manually constructing Numscript for every user interaction. We provide dedicated endpoints for:
*   **Create Wallet**: Automatically sets up a deterministic wallet ID (e.g., `user123-USD`).
*   **Credit/Debit**: Simple API to fund or charge wallets.
*   **History & Statements**: Built-in endpoints to retrieve transaction history and monthly statements.

### 2. Multi-Ledger Architecture
We separate concerns to keep your accounting clean:
*   **User Ledger (`ledgertrack`)**: Tracks individual user balances (`users:u1:wallets:USD:available`).
*   **Channel Ledger (`channels-USD`)**: Tracks liquidity with external payment providers (e.g., Stripe, PayPal).
*   **Revenue Ledger (`revenue-USD`)**: Automatically captures fees and revenue.

### 3. Smart Transaction Routing
When you debit a user wallet, LedgerTrack can automatically:
1.  **Debit the User**: Reduce user's available balance.
2.  **Debit the Channel**: Update the liquidity tracking for the payment provider used.
3.  **Book Revenue**: Calculate and book the difference as revenue.
All linked via metadata for full traceability.

### 4. Liens & Holds
Built-in support for:
*   **Creating Liens**: Lock funds (move from `available` to `lien` sub-account).
*   **Releasing Liens**: Either finalize the payment (Pay) or return funds to the user (Cancel).

### 5. Deployment Ready
*   **Automated Deployment Script**: A `deploy.sh` script to get you up and running on Ubuntu in minutes.
*   **Postman Collection**: A complete collection to test all new endpoints.

## Quick Start

### Local Development

1.  **Start the Server**:
    ```bash
    # Create a .env file (or use the provided deploy script to generate one)
    export POSTGRES_URI="postgresql://ledger:ledger@localhost:5432/ledger?sslmode=disable"
    go run main.go serve
    ```

2.  **Use the API**:
    *   **Create a Wallet**:
        ```bash
        curl -X POST http://localhost:3068/v2/ledgertrack/wallets \
          -d '{"userID": "u1", "currency": "USD"}'
        ```
    *   **Credit Funds**:
        ```bash
        curl -X POST http://localhost:3068/v2/ledgertrack/wallets/u1-USD/credit \
          -d '{"amount": 1000, "reference": "ref-001"}'
        ```

### Deployment

We provide a helper script for Ubuntu servers:

```bash
sudo ./deploy.sh
```
This will:
*   Install dependencies (Go, Postgres client, etc.).
*   Configure your database connection.
*   Set up a Systemd service.
*   Pre-create your payment channels.

## API Documentation

The core Formance API documentation applies, but we have added the following specific V2 endpoints:

### Wallets
*   `POST /v2/ledgertrack/wallets`: Create a new wallet.
*   `POST /v2/ledgertrack/wallets/{walletID}/credit`: Add funds.
*   `POST /v2/ledgertrack/wallets/{walletID}/debit`: Remove funds (supports `channelID` for multi-ledger routing).
*   `POST /v2/ledgertrack/wallets/{walletID}/lien`: Lock funds.
*   `POST /v2/ledgertrack/wallets/{walletID}/lien/release`: Release locked funds (Pay or Cancel).
*   `GET /v2/ledgertrack/wallets/{walletID}/history`: Get transaction history.
*   `GET /v2/ledgertrack/wallets/{walletID}/statement`: Get statement.

### Channels
*   `POST /v2/ledgertrack/channels`: Register a new payment channel.
*   `POST /v2/ledgertrack/channels/{channelID}/credit`: Add liquidity to a channel.
*   `GET /v2/ledgertrack/channels/{channelID}`: Get channel balance/info.

## Original Formance Ledger
This project is a fork of [Formance Ledger](https://github.com/formancehq/ledger). We acknowledge and thank the Formance team for building the incredible core engine that makes this possible.

---
**License**: Apache 2.0 (Inherited from Formance Ledger)
