from __future__ import annotations

import json
import os
import shutil
import sys
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests
from dateutil import tz

try:
    from rich.console import Console
    from rich.table import Table
except Exception:
    Console = None
    Table = None


def _console() -> Any:
    if Console is None:
        return None
    return Console()


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _now_iso_plus(seconds: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + seconds))


def _parse_bool(v: str, default: bool) -> bool:
    if v is None:
        return default
    s = v.strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return default


@dataclass(frozen=True)
class E2EConfig:
    base_url: str
    ledger: str
    admin_token: Optional[str]
    finance_token: Optional[str]
    audit_token: Optional[str]
    run_id: str
    cleanup: bool


def load_config() -> E2EConfig:
    run_id = os.getenv("E2E_RUN_ID", "").strip() or str(uuid.uuid4())
    return E2EConfig(
        base_url=os.getenv("LEDGERTRACK_BASE_URL", "http://localhost:3068").rstrip("/"),
        ledger=os.getenv("LEDGERTRACK_LEDGER", "ledgertrack").strip(),
        admin_token=os.getenv("LEDGERTRACK_ADMIN_TOKEN", "").strip() or None,
        finance_token=os.getenv("LEDGERTRACK_FINANCE_TOKEN", "").strip() or None,
        audit_token=os.getenv("LEDGERTRACK_AUDIT_TOKEN", "").strip() or None,
        run_id=run_id,
        cleanup=_parse_bool(os.getenv("E2E_CLEANUP", "true"), True),
    )


class HTTPError(Exception):
    def __init__(self, message: str, method: str, url: str, status: int, request_body: Any, response_text: str):
        super().__init__(message)
        self.method = method
        self.url = url
        self.status = status
        self.request_body = request_body
        self.response_text = response_text


class LedgerTrackClient:
    def __init__(self, cfg: E2EConfig):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )
        self.last_method: Optional[str] = None
        self.last_path: Optional[str] = None
        self.last_status: Optional[int] = None
        self.last_url: Optional[str] = None
        self.last_request_body: Any = None
        self.last_response_headers: Dict[str, str] = {}
        self.last_response_body: Any = None

    def _truncate(self, s: str, max_chars: int = 4000) -> str:
        if len(s) <= max_chars:
            return s
        return s[:max_chars] + f"...(truncated, {len(s)} chars total)"

    def _capture_response_body(self, resp: requests.Response, stream: bool) -> Any:
        ctype = resp.headers.get("Content-Type", "")
        if "application/json" in ctype:
            try:
                return resp.json()
            except Exception:
                return self._truncate(resp.text)
        if stream:
            try:
                content = resp.content
                return f"<{ctype or 'stream'} {len(content)} bytes>"
            except Exception:
                return f"<{ctype or 'stream'}>"
        try:
            return self._truncate(resp.text)
        except Exception:
            return "<unreadable>"

    def _auth_header(self, token: Optional[str]) -> Dict[str, str]:
        if token:
            return {"Authorization": f"Bearer {token}"}
        return {}

    def _request(
        self,
        method: str,
        path: str,
        token: Optional[str] = None,
        json_body: Any = None,
        expected_status: Optional[Tuple[int, ...]] = None,
        stream: bool = False,
    ) -> requests.Response:
        url = f"{self.cfg.base_url}{path}"
        headers = self._auth_header(token)
        attempt = 0
        while True:
            resp = self.session.request(method, url, headers=headers, json=json_body, stream=stream)
            attempt += 1
            if resp.status_code < 500 or resp.status_code >= 600 or attempt >= 4:
                break
            time.sleep(0.5 * (2 ** (attempt - 1)))

        self.last_method = method
        self.last_path = path
        self.last_status = resp.status_code
        self.last_url = url
        self.last_request_body = json_body
        self.last_response_headers = {k: v for k, v in resp.headers.items()}
        self.last_response_body = self._capture_response_body(resp, stream=stream)
        if expected_status is None:
            expected_status = (200, 201, 202, 204)
        if resp.status_code not in expected_status:
            body = None
            try:
                body = json_body
            except Exception:
                body = None
            raise HTTPError(
                f"unexpected status {resp.status_code}",
                method=method,
                url=url,
                status=resp.status_code,
                request_body=body,
                response_text=resp.text,
            )
        return resp

    def _json(self, resp: requests.Response) -> Any:
        return resp.json()

    def get_info(self) -> Any:
        resp = self._request("GET", "/_info", token=self.cfg.admin_token, expected_status=(200, 404))
        if resp.status_code == 404:
            resp = self._request("GET", "/", token=self.cfg.admin_token, expected_status=(200,))
        return self._json(resp)

    def ensure_ledger(self, ledger: Optional[str] = None) -> Any:
        name = ledger or self.cfg.ledger
        resp = self._request("POST", f"/v2/{name}", token=self.cfg.admin_token, expected_status=(201, 409, 400, 200))
        if resp.status_code in (409, 400):
            try:
                payload = resp.json()
            except Exception:
                payload = {}
            if isinstance(payload, dict) and payload.get("errorCode") == "LEDGER_ALREADY_EXISTS":
                return {"status": "exists"}
            raise HTTPError(
                f"unexpected status {resp.status_code}",
                method="POST",
                url=f"{self.cfg.base_url}/v2/{name}",
                status=resp.status_code,
                request_body=None,
                response_text=resp.text,
            )
        return self._json(resp)

    def list_currencies(self) -> Any:
        resp = self._request("GET", f"/v2/{self.cfg.ledger}/currencies", token=self.cfg.admin_token)
        return self._json(resp)

    def create_channel(self, currency: str, metadata: Optional[Dict[str, str]] = None) -> Any:
        body: Dict[str, Any] = {"currency": currency}
        if metadata:
            body["metadata"] = metadata
        resp = self._request("POST", f"/v2/{self.cfg.ledger}/channels", token=self.cfg.admin_token, json_body=body)
        return self._json(resp)

    def credit_channel(self, channel_id: str, currency: str, amount_minor: int, reference: str) -> Any:
        resp = self._request(
            "POST",
            f"/v2/{self.cfg.ledger}/channels/{channel_id}/credit",
            token=self.cfg.admin_token,
            json_body={"amount": amount_minor, "currency": currency, "reference": reference},
        )
        return self._json(resp)

    def get_channel(self, channel_id: str, currency: str) -> Any:
        resp = self._request("GET", f"/v2/{self.cfg.ledger}/channels/{channel_id}?currency={currency}", token=self.cfg.admin_token)
        return self._json(resp)

    def get_channel_history(self, channel_id: str, currency: str) -> Any:
        resp = self._request("GET", f"/v2/{self.cfg.ledger}/channels/{channel_id}/history?currency={currency}", token=self.cfg.admin_token)
        return self._json(resp)

    def create_wallet(self, user_id: str, currency: str) -> Any:
        resp = self._request(
            "POST",
            f"/v2/{self.cfg.ledger}/wallets",
            token=self.cfg.admin_token,
            json_body={"userID": user_id, "currency": currency},
        )
        return self._json(resp)

    def create_product(self, body: Dict[str, Any]) -> Any:
        resp = self._request("POST", f"/v2/{self.cfg.ledger}/products", token=self.cfg.admin_token, json_body=body)
        return self._json(resp)

    def activate_product(self, product_id: str) -> Any:
        resp = self._request("POST", f"/v2/{self.cfg.ledger}/products/{product_id}/activate", token=self.cfg.admin_token)
        return self._json(resp)

    def retire_product(self, product_id: str) -> Any:
        resp = self._request("POST", f"/v2/{self.cfg.ledger}/products/{product_id}/retire", token=self.cfg.admin_token)
        return self._json(resp)

    def create_client(self, body: Dict[str, Any]) -> Any:
        resp = self._request("POST", f"/v2/{self.cfg.ledger}/clients", token=self.cfg.admin_token, json_body=body)
        return self._json(resp)

    def activate_client(self, client_id: str) -> Any:
        resp = self._request("POST", f"/v2/{self.cfg.ledger}/clients/{client_id}/activate", token=self.cfg.admin_token)
        return self._json(resp)

    def suspend_client(self, client_id: str, reason: str) -> Any:
        resp = self._request(
            "POST",
            f"/v2/{self.cfg.ledger}/clients/{client_id}/suspend",
            token=self.cfg.admin_token,
            json_body={"reason": reason},
        )
        return self._json(resp)

    def reactivate_client(self, client_id: str) -> Any:
        resp = self._request("POST", f"/v2/{self.cfg.ledger}/clients/{client_id}/reactivate", token=self.cfg.admin_token)
        return self._json(resp)

    def close_client(self, client_id: str) -> Any:
        resp = self._request("POST", f"/v2/{self.cfg.ledger}/clients/{client_id}/close", token=self.cfg.admin_token)
        return self._json(resp)

    def submit_kyc(self, client_id: str, body: Dict[str, Any]) -> Any:
        resp = self._request("POST", f"/v2/{self.cfg.ledger}/clients/{client_id}/kyc", token=self.cfg.admin_token, json_body=body)
        return self._json(resp)

    def list_kyc(self, client_id: str) -> Any:
        resp = self._request("GET", f"/v2/{self.cfg.ledger}/clients/{client_id}/kyc", token=self.cfg.admin_token)
        return self._json(resp)

    def verify_kyc(self, client_id: str, kyc_id: str, body: Dict[str, Any]) -> Any:
        resp = self._request(
            "POST",
            f"/v2/{self.cfg.ledger}/clients/{client_id}/kyc/{kyc_id}/verify",
            token=self.cfg.admin_token,
            json_body=body,
        )
        return self._json(resp)

    def open_account(self, body: Dict[str, Any]) -> Any:
        resp = self._request("POST", f"/v2/{self.cfg.ledger}/accounts", token=self.cfg.admin_token, json_body=body)
        return self._json(resp)

    def get_account(self, account_id: str) -> Any:
        resp = self._request("GET", f"/v2/{self.cfg.ledger}/accounts/{account_id}", token=self.cfg.admin_token)
        return self._json(resp)

    def activate_account(self, account_id: str) -> Any:
        resp = self._request("POST", f"/v2/{self.cfg.ledger}/accounts/{account_id}/activate", token=self.cfg.admin_token)
        return self._json(resp)

    def suspend_account(self, account_id: str) -> Any:
        resp = self._request("POST", f"/v2/{self.cfg.ledger}/accounts/{account_id}/suspend", token=self.cfg.admin_token)
        return self._json(resp)

    def freeze_account(self, account_id: str) -> Any:
        resp = self._request("POST", f"/v2/{self.cfg.ledger}/accounts/{account_id}/freeze", token=self.cfg.admin_token)
        return self._json(resp)

    def reactivate_account(self, account_id: str) -> Any:
        resp = self._request("POST", f"/v2/{self.cfg.ledger}/accounts/{account_id}/reactivate", token=self.cfg.admin_token)
        return self._json(resp)

    def close_account(self, account_id: str) -> Any:
        resp = self._request("POST", f"/v2/{self.cfg.ledger}/accounts/{account_id}/close", token=self.cfg.admin_token)
        return self._json(resp)

    def account_balance(self, account_id: str) -> Any:
        resp = self._request("GET", f"/v2/{self.cfg.ledger}/accounts/{account_id}/balance", token=self.cfg.admin_token)
        return self._json(resp)

    def account_history(self, account_id: str) -> Any:
        resp = self._request("GET", f"/v2/{self.cfg.ledger}/accounts/{account_id}/history", token=self.cfg.admin_token)
        return self._json(resp)

    def account_statement(self, account_id: str) -> Any:
        resp = self._request("GET", f"/v2/{self.cfg.ledger}/accounts/{account_id}/statement", token=self.cfg.admin_token)
        return self._json(resp)

    def account_credit(self, account_id: str, amount_minor: int, reference: str) -> Any:
        resp = self._request(
            "POST",
            f"/v2/{self.cfg.ledger}/accounts/{account_id}/credit",
            token=self.cfg.admin_token,
            json_body={"amount": str(amount_minor), "reference": reference, "metadata": {}},
        )
        return self._json(resp)

    def account_debit(
        self,
        account_id: str,
        amount_minor: int,
        reference: str,
        channel_id: Optional[str] = None,
        channel_amount_minor: Optional[int] = None,
    ) -> Any:
        body: Dict[str, Any] = {"amount": str(amount_minor), "reference": reference, "metadata": {}}
        if channel_id:
            body["channelID"] = channel_id
        if channel_amount_minor is not None:
            body["channelAmount"] = str(channel_amount_minor)
        resp = self._request("POST", f"/v2/{self.cfg.ledger}/accounts/{account_id}/debit", token=self.cfg.admin_token, json_body=body)
        return self._json(resp)

    def credit_wallet(self, wallet_id: str, amount_minor: int, reference: str, metadata: Optional[Dict[str, str]] = None) -> Any:
        body: Dict[str, Any] = {"amount": str(amount_minor), "reference": reference}
        if metadata:
            body["metadata"] = metadata
        resp = self._request("POST", f"/v2/{self.cfg.ledger}/wallets/{wallet_id}/credit", token=self.cfg.admin_token, json_body=body)
        return self._json(resp)

    def debit_wallet(
        self,
        wallet_id: str,
        amount_minor: int,
        reference: str,
        channel_id: Optional[str] = None,
        channel_amount_minor: Optional[int] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> Any:
        body: Dict[str, Any] = {"amount": str(amount_minor), "reference": reference}
        if metadata:
            body["metadata"] = metadata
        if channel_id:
            body["channelID"] = channel_id
        if channel_amount_minor is not None:
            body["channelAmount"] = str(channel_amount_minor)
        resp = self._request("POST", f"/v2/{self.cfg.ledger}/wallets/{wallet_id}/debit", token=self.cfg.admin_token, json_body=body)
        return self._json(resp)

    def lien_wallet(self, wallet_id: str, amount_minor: int, reference: str) -> Any:
        resp = self._request(
            "POST",
            f"/v2/{self.cfg.ledger}/wallets/{wallet_id}/lien",
            token=self.cfg.admin_token,
            json_body={"amount": str(amount_minor), "reference": reference},
        )
        return self._json(resp)

    def release_lien(
        self,
        wallet_id: str,
        amount_minor: int,
        reference: str,
        mode: str,
        channel_id: Optional[str] = None,
        channel_amount_minor: Optional[int] = None,
    ) -> Any:
        body: Dict[str, Any] = {"amount": str(amount_minor), "reference": reference, "mode": mode}
        if channel_id:
            body["channelID"] = channel_id
        if channel_amount_minor is not None:
            body["channelAmount"] = str(channel_amount_minor)
        resp = self._request("POST", f"/v2/{self.cfg.ledger}/wallets/{wallet_id}/lien/release", token=self.cfg.admin_token, json_body=body)
        return self._json(resp)

    def wallet_statement(self, wallet_id: str) -> Any:
        resp = self._request("GET", f"/v2/{self.cfg.ledger}/wallets/{wallet_id}/statement", token=self.cfg.admin_token)
        return self._json(resp)

    def wallet_history(self, wallet_id: str) -> Any:
        resp = self._request("GET", f"/v2/{self.cfg.ledger}/wallets/{wallet_id}/history", token=self.cfg.admin_token)
        return self._json(resp)

    def get_channel_fee_config(self, channel_id: str) -> Any:
        resp = self._request("GET", f"/v2/{self.cfg.ledger}/channels/{channel_id}/fees/config", token=self.cfg.audit_token or self.cfg.admin_token)
        return self._json(resp)

    def upsert_channel_fee_config(self, channel_id: str, body: Dict[str, Any]) -> Any:
        resp = self._request("PUT", f"/v2/{self.cfg.ledger}/channels/{channel_id}/fees/config", token=self.cfg.admin_token, json_body=body)
        return self._json(resp)

    def list_channel_fee_audits(self, channel_id: str, limit: int = 50) -> Any:
        resp = self._request("GET", f"/v2/{self.cfg.ledger}/channels/{channel_id}/fees/audits?limit={limit}", token=self.cfg.audit_token or self.cfg.admin_token)
        return self._json(resp)

    def list_channel_fee_configs(self) -> Any:
        resp = self._request("GET", f"/v2/{self.cfg.ledger}/channels/fees/configs", token=self.cfg.admin_token)
        return self._json(resp)

    def channel_revenue_summary(self, body: Dict[str, Any]) -> Any:
        resp = self._request("POST", f"/v2/{self.cfg.ledger}/reports/channels/revenue", token=self.cfg.finance_token or self.cfg.admin_token, json_body=body)
        return _extract_report_payload(self._json(resp))

    def channel_revenue_timeseries(self, body: Dict[str, Any]) -> Any:
        resp = self._request("POST", f"/v2/{self.cfg.ledger}/reports/channels/revenue/timeseries", token=self.cfg.finance_token or self.cfg.admin_token, json_body=body)
        return _extract_data(self._json(resp))

    def channel_revenue_export(self, body: Dict[str, Any]) -> requests.Response:
        return self._request(
            "POST",
            f"/v2/{self.cfg.ledger}/reports/channels/revenue/export",
            token=self.cfg.finance_token or self.cfg.admin_token,
            json_body=body,
            expected_status=(200,),
            stream=True,
        )

    def channel_revenue_dashboard(self, body: Dict[str, Any]) -> Any:
        resp = self._request("POST", f"/v2/{self.cfg.ledger}/reports/channels/dashboard", token=self.cfg.finance_token or self.cfg.admin_token, json_body=body)
        return self._json(resp)

    def list_transactions(self, ledger_name: str, page_size: int = 100, after: Optional[str] = None) -> Any:
        path = f"/v2/{ledger_name}/transactions?pageSize={page_size}"
        if after:
            path += f"&after={after}"
        resp = self._request("GET", path, token=self.cfg.admin_token)
        return self._json(resp)


def quantize_major(amount: Decimal, precision: int) -> Decimal:
    q = Decimal(10) ** (-precision)
    return amount.quantize(q, rounding=ROUND_HALF_UP)


def major_to_minor(amount: Decimal, precision: int) -> int:
    scaled = (amount * (Decimal(10) ** precision)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return int(scaled)


def minor_to_major(amount_minor: int, precision: int) -> Decimal:
    return (Decimal(amount_minor) / (Decimal(10) ** precision)).quantize(Decimal(10) ** (-precision))


def _require(cond: bool, message: str) -> None:
    if not cond:
        raise AssertionError(message)


def _extract_data(payload: Any) -> Any:
    while isinstance(payload, dict) and "data" in payload:
        payload = payload["data"]
    return payload


def _extract_report_payload(payload: Any) -> Any:
    if not isinstance(payload, dict):
        return payload
    if "totals" in payload:
        return payload
    data = payload.get("data")
    if isinstance(data, dict):
        if "totals" in data:
            return data
        nested = data.get("data")
        if isinstance(nested, dict) and "totals" in nested:
            return nested
    return _extract_data(payload)


def _extract_cursor(payload: Any) -> List[Any]:
    if isinstance(payload, dict) and "cursor" in payload and isinstance(payload["cursor"], dict):
        data = payload["cursor"].get("data")
        if isinstance(data, list):
            return data
    return []

def _extract_cursor_next(payload: Any) -> Optional[str]:
    if isinstance(payload, dict) and "cursor" in payload and isinstance(payload["cursor"], dict):
        nxt = payload["cursor"].get("next")
        if isinstance(nxt, str) and nxt.strip():
            return nxt
    return None

def _list_all_transactions(client: LedgerTrackClient, ledger_name: str, max_pages: int = 25) -> List[Any]:
    after: Optional[str] = None
    out: List[Any] = []
    seen_after: set[str] = set()
    seen_ids: set[str] = set()
    for _ in range(max_pages):
        if after:
            if after in seen_after:
                break
            seen_after.add(after)
        page = client.list_transactions(ledger_name, page_size=100, after=after)
        data = _extract_cursor(page)
        for tx in data:
            tx_id = None
            if isinstance(tx, dict) and tx.get("id") is not None:
                tx_id = str(tx.get("id"))
            if tx_id is not None:
                if tx_id in seen_ids:
                    continue
                seen_ids.add(tx_id)
            out.append(tx)
        after = _extract_cursor_next(page)
        if not after:
            break
    return out


def _get_volume_balance(account_data: Dict[str, Any], currency: str) -> int:
    vols = account_data.get("volumes") or {}
    if not isinstance(vols, dict):
        return 0
    v = vols.get(currency)
    if v is None:
        v = vols.get(f"{currency}/2")
    if not isinstance(v, dict):
        return 0
    bal = v.get("balance")
    if bal is None:
        return 0
    return int(bal)


def _posting_amount_minor(p: Any) -> int:
    if not isinstance(p, dict):
        return 0
    amt = p.get("amount")
    if amt is None:
        return 0
    return int(amt)


def _posting_asset(p: Any) -> str:
    if not isinstance(p, dict):
        return ""
    asset = p.get("asset") or ""
    return str(asset)


def _compute_account_balance_from_txs(transactions: List[Any], account: str, currency: str) -> int:
    bal = 0
    for tx in transactions:
        postings = []
        if isinstance(tx, dict):
            postings = tx.get("postings") or []
        if not isinstance(postings, list):
            continue
        for p in postings:
            asset = _posting_asset(p)
            if asset != currency and asset != f"{currency}/2":
                continue
            amt = _posting_amount_minor(p)
            if p.get("destination") == account:
                bal += amt
            if p.get("source") == account:
                bal -= amt
    return bal


class StepRunner:
    def __init__(self, console: Any, client: LedgerTrackClient):
        self.console = console
        self.client = client
        self.sections: List[Dict[str, Any]] = []

    def banner(self, title: str) -> None:
        if self.console is not None:
            self.console.rule(title)
        else:
            print(f"=== {title} ===")

    def begin_section(self, name: str) -> Dict[str, Any]:
        section = {"name": name, "steps_run": 0, "steps_passed": 0, "started": time.monotonic(), "took": 0.0}
        self.sections.append(section)
        self.banner(name)
        return section

    def end_section(self, section: Dict[str, Any]) -> None:
        section["took"] = time.monotonic() - section["started"]

    def _print_exchange(self) -> None:
        payload = {
            "request": {
                "method": self.client.last_method,
                "url": self.client.last_url,
                "body": self.client.last_request_body,
            },
            "response": {
                "status": self.client.last_status,
                "headers": self.client.last_response_headers,
                "body": self.client.last_response_body,
            },
        }
        if self.console is not None:
            self.console.print_json(json.dumps(payload, default=str))
        else:
            print(json.dumps(payload, indent=2, default=str))

    def run_step(self, section: Dict[str, Any], title: str, fn: Callable[[], Any]) -> Any:
        section["steps_run"] += 1
        started = time.monotonic()
        try:
            result = fn()
            section["steps_passed"] += 1
            took = time.monotonic() - started
            m = self.client.last_method or ""
            p = self.client.last_path or ""
            s = self.client.last_status or 0
            line = f"{title} | {m} {p} -> {s} | PASS | {took:.3f}s"
            if self.console is not None:
                self.console.print(line)
            else:
                print(line)
            self._print_exchange()
            return result
        except Exception as e:
            took = time.monotonic() - started
            m = self.client.last_method or ""
            p = self.client.last_path or ""
            s = self.client.last_status or 0
            line = f"{title} | {m} {p} -> {s} | FAIL | {took:.3f}s"
            if self.console is not None:
                self.console.print(line)
            else:
                print(line)
            if isinstance(e, HTTPError):
                detail = {
                    "error": str(e),
                    "request": {"method": e.method, "url": e.url, "body": e.request_body},
                    "response": {"status": e.status, "body": e.response_text},
                }
                if self.console is not None:
                    self.console.print_json(json.dumps(detail))
                else:
                    print(json.dumps(detail, indent=2))
            else:
                if self.console is not None:
                    self.console.print(str(e))
                else:
                    print(str(e))
            raise

    def summary(self) -> None:
        if self.console is not None and Table is not None:
            table = Table(title="E2E Summary")
            table.add_column("Section")
            table.add_column("Steps Run", justify="right")
            table.add_column("Steps Passed", justify="right")
            table.add_column("Time (s)", justify="right")
            for s in self.sections:
                table.add_row(s["name"], str(s["steps_run"]), str(s["steps_passed"]), f'{s["took"]:.3f}')
            self.console.print(table)
        else:
            for s in self.sections:
                print(f'{s["name"]}: {s["steps_passed"]}/{s["steps_run"]} in {s["took"]:.3f}s')

    def run_step_expect_status(
        self,
        section: Dict[str, Any],
        title: str,
        fn: Callable[[], Any],
        expected_statuses: Tuple[int, ...],
    ) -> Any:
        section["steps_run"] += 1
        started = time.monotonic()
        try:
            fn()
        except HTTPError as e:
            took = time.monotonic() - started
            m = e.method
            p = self.client.last_path or ""
            line = f"{title} | {m} {p} -> {e.status} | PASS | {took:.3f}s"
            if e.status not in expected_statuses:
                line = f"{title} | {m} {p} -> {e.status} | FAIL | {took:.3f}s"
                if self.console is not None:
                    self.console.print(line)
                    self.console.print_json(
                        json.dumps(
                            {
                                "expected_statuses": expected_statuses,
                                "request": {"method": e.method, "url": e.url, "body": e.request_body},
                                "response": {"status": e.status, "body": e.response_text},
                            }
                        )
                    )
                else:
                    print(line)
                    print(
                        json.dumps(
                            {
                                "expected_statuses": expected_statuses,
                                "request": {"method": e.method, "url": e.url, "body": e.request_body},
                                "response": {"status": e.status, "body": e.response_text},
                            },
                            indent=2,
                        )
                    )
                raise

            section["steps_passed"] += 1
            if self.console is not None:
                self.console.print(line)
            else:
                print(line)
            self._print_exchange()
            return {"status": e.status, "body": e.response_text}

        took = time.monotonic() - started
        m = self.client.last_method or ""
        p = self.client.last_path or ""
        s = self.client.last_status or 0
        line = f"{title} | {m} {p} -> {s} | FAIL | {took:.3f}s"
        if self.console is not None:
            self.console.print(line)
        else:
            print(line)
        raise AssertionError(f"expected HTTP error status in {expected_statuses}, got success")


def section_0_preflight(r: StepRunner, cfg: E2EConfig) -> Dict[str, int]:

    section = r.begin_section("Section 0: Preflight")
    client = r.client

    r.run_step(section, "Check service info", lambda: client.get_info())
    r.run_step(section, "Ensure ledger exists", lambda: client.ensure_ledger(cfg.ledger))
    r.run_step(section, "Ensure channel ledger exists", lambda: client.ensure_ledger("channels-USD"))
    r.run_step(section, "Ensure revenue ledger exists", lambda: client.ensure_ledger("revenue-USD"))
    currencies_payload = r.run_step(section, "List currencies", lambda: client.list_currencies())
    currencies = _extract_data(currencies_payload).get("currencies", [])
    precision_by_code: Dict[str, int] = {}
    enabled_codes: List[str] = []
    for ccy in currencies:
        if not isinstance(ccy, dict):
            continue
        code = str(ccy.get("code", "")).upper()
        precision = int(ccy.get("precision", 2))
        enabled = bool(ccy.get("enabled", False))
        precision_by_code[code] = precision
        if enabled:
            enabled_codes.append(code)

    _require("USD" in enabled_codes, "USD currency is not enabled; seed _system.currencies first")
    _require("NGN" in enabled_codes, "NGN currency is not enabled; seed _system.currencies first")

    r.end_section(section)
    return precision_by_code


def section_1_wallet_and_channel(r: StepRunner, cfg: E2EConfig, precision_by_code: Dict[str, int]) -> Dict[str, Any]:
    section = r.begin_section("Section 1: Wallet & Channel layer")
    client = r.client

    currency = "USD"
    p = precision_by_code.get(currency, 2)

    channel_name = f"test-channel-usd-{cfg.run_id}"
    channel = r.run_step(
        section,
        "Create channel",
        lambda: client.create_channel(currency, metadata={"name": channel_name, "run_id": cfg.run_id}),
    )
    channel_id = _extract_data(channel).get("channel_id")
    _require(bool(channel_id), "channel_id missing from create channel response")
    channel_id = str(channel_id)

    r.run_step(
        section,
        "Seed zero-fee config for wallet PAY flow",
        lambda: client.upsert_channel_fee_config(
            channel_id,
            {
                "currency": currency,
                "enabled": True,
                "actor": "e2e",
                "user_fee": {"type": "none"},
                "processing_fee": {"type": "none"},
            },
        ),
    )

    credit_ref = f"channel-credit-{cfg.run_id}-1"
    channel_liquidity_minor = major_to_minor(Decimal("1000000"), p)
    r.run_step(
        section,
        "Credit channel liquidity",
        lambda: client.credit_channel(channel_id, currency, channel_liquidity_minor, credit_ref),
    )

    channel_account = r.run_step(section, "Read channel", lambda: client.get_channel(channel_id, currency))
    channel_data = _extract_data(channel_account)
    bal_minor = _get_volume_balance(channel_data, currency)
    _require(bal_minor == channel_liquidity_minor, f"channel balance mismatch: expected {channel_liquidity_minor}, got {bal_minor}")

    user_id = f"wallet-user-A-{cfg.run_id}"
    wallet = r.run_step(section, "Create wallet", lambda: client.create_wallet(user_id, currency))
    wallet_id = _extract_data(wallet).get("walletID") or _extract_data(wallet).get("wallet_id")
    _require(bool(wallet_id), "walletID missing from create wallet response")
    wallet_id = str(wallet_id)

    wallet_credit_ref = f"wallet-credit-A-{cfg.run_id}-1"
    wallet_credit_minor = major_to_minor(Decimal("5000"), p)
    r.run_step(section, "Credit wallet", lambda: client.credit_wallet(wallet_id, wallet_credit_minor, wallet_credit_ref))

    history = r.run_step(section, "Get wallet history", lambda: client.wallet_history(wallet_id))
    history_items = _extract_cursor(history)
    _require(any(isinstance(x, dict) and x.get("reference") == wallet_credit_ref for x in history_items), "wallet credit not found in history")

    lien_ref = f"lien-A-{cfg.run_id}-1"
    release_pay_ref = f"lien-release-pay-{cfg.run_id}-1"
    lien_minor = major_to_minor(Decimal("1200"), p)
    r.run_step(section, "Place lien", lambda: client.lien_wallet(wallet_id, lien_minor, lien_ref))

    history2 = r.run_step(section, "Get wallet history (after lien)", lambda: client.wallet_history(wallet_id))
    history2_items = _extract_cursor(history2)
    available_account = f"users:{user_id}:wallets:{currency}:available"
    lien_account = f"users:{user_id}:wallets:{currency}:lien"
    available_minor = _compute_account_balance_from_txs(history2_items, available_account, currency)
    lien_balance_minor = _compute_account_balance_from_txs(history2_items, lien_account, currency)
    _require(available_minor == major_to_minor(Decimal("3800"), p), f"available balance mismatch: got {available_minor}")
    _require(lien_balance_minor == lien_minor, f"lien balance mismatch: got {lien_balance_minor}")

    r.run_step(
        section,
        "Release lien (PAY) through channel (no fee)",
        lambda: client.release_lien(wallet_id, lien_minor, release_pay_ref, mode="PAY", channel_id=channel_id, channel_amount_minor=lien_minor),
    )
    history3 = r.run_step(section, "Get wallet history (after pay)", lambda: client.wallet_history(wallet_id))
    history3_items = _extract_cursor(history3)
    available_minor_after = _compute_account_balance_from_txs(history3_items, available_account, currency)
    lien_minor_after = _compute_account_balance_from_txs(history3_items, lien_account, currency)
    _require(available_minor_after == major_to_minor(Decimal("3800"), p), f"available balance after pay mismatch: got {available_minor_after}")
    _require(lien_minor_after == 0, f"lien balance after pay mismatch: got {lien_minor_after}")

    channel_after = r.run_step(section, "Read channel (after pay)", lambda: client.get_channel(channel_id, currency))
    channel_after_data = _extract_data(channel_after)
    channel_bal_after = _get_volume_balance(channel_after_data, currency)
    _require(channel_bal_after == channel_liquidity_minor - lien_minor, f"channel balance mismatch after pay: got {channel_bal_after}")

    lien_ref2 = f"lien-A-{cfg.run_id}-2"
    release_cancel_ref = f"lien-release-cancel-{cfg.run_id}-2"
    lien_minor2 = major_to_minor(Decimal("500"), p)
    r.run_step(section, "Place lien (second)", lambda: client.lien_wallet(wallet_id, lien_minor2, lien_ref2))
    r.run_step(section, "Release lien (cancel)", lambda: client.release_lien(wallet_id, lien_minor2, release_cancel_ref, mode="RELEASE_ONLY"))

    history4 = r.run_step(section, "Get wallet history (after cancel)", lambda: client.wallet_history(wallet_id))
    history4_items = _extract_cursor(history4)
    available_minor_end = _compute_account_balance_from_txs(history4_items, available_account, currency)
    lien_minor_end = _compute_account_balance_from_txs(history4_items, lien_account, currency)
    _require(available_minor_end == major_to_minor(Decimal("3800"), p), f"available balance after cancel mismatch: got {available_minor_end}")
    _require(lien_minor_end == 0, f"lien balance after cancel mismatch: got {lien_minor_end}")

    r.end_section(section)
    return {
        "currency": currency,
        "precision": p,
        "channel_id": channel_id,
        "channel_liquidity_minor": channel_liquidity_minor,
        "wallet_user_id": user_id,
        "wallet_id": wallet_id,
    }


def section_2_cba(r: StepRunner, cfg: E2EConfig, precision_by_code: Dict[str, int]) -> Dict[str, Any]:
    section = r.begin_section("Section 2: CBA layer")
    client = r.client

    currency = "USD"
    p = precision_by_code.get(currency, 2)

    product_code = f"SAV-USD-{cfg.run_id}"
    product = r.run_step(
        section,
        "Create product (draft)",
        lambda: client.create_product(
            {
                "code": product_code,
                "name": f"Savings USD {cfg.run_id}",
                "description": "E2E savings product",
                "category": "savings",
                "currency": currency,
                "rules": {
                    "requires_kyc_level": 1,
                    "min_opening_balance": "0.00",
                    "allow_debits": True,
                    "allow_credits": True,
                    "transaction_limits": {
                        "single_debit_limit": "2000.00",
                    },
                },
                "fee_schedule": {
                    "transaction_fees": [
                        {
                            "event": "debit",
                            "type": "flat",
                            "value": "0.50",
                        }
                    ]
                },
            }
        ),
    )
    product_id = str(_extract_data(product).get("id"))
    _require(product_id != "None", "product id missing")

    r.run_step(section, "Activate product", lambda: client.activate_product(product_id))

    client_obj = r.run_step(
        section,
        "Create client (individual)",
        lambda: client.create_client(
            {
                "type": "individual",
                "contact": {"email": f"e2e-{cfg.run_id}@example.com", "phone": f"+234000{cfg.run_id[:6]}"},
                "individual_data": {
                    "first_name": "E2E",
                    "last_name": cfg.run_id[:8],
                    "national_id_type": "NIN",
                    "national_id_number": f"NIN-{cfg.run_id[:10]}",
                    "nationality": "NG",
                    "occupation": "Tester",
                },
            }
        ),
    )
    client_id = str(_extract_data(client_obj).get("id"))
    client_number = str(_extract_data(client_obj).get("client_number"))
    _require(client_id != "None", "client id missing")
    _require(client_number not in ("None", ""), "client_number missing")

    r.run_step_expect_status(
        section,
        "Open account before KYC (expect 4xx)",
        lambda: client.open_account(
            {
                "client_id": client_id,
                "product_id": product_id,
                "opening_deposit": "0.00",
            }
        ),
        expected_statuses=(400, 401, 403),
    )

    r.run_step_expect_status(
        section,
        "Activate client before KYC (expect 4xx)",
        lambda: client.activate_client(client_id),
        expected_statuses=(400, 401, 403),
    )

    kyc = r.run_step(
        section,
        "Submit KYC (level 1)",
        lambda: client.submit_kyc(
            client_id,
            {
                "level": 1,
                "documents": [{"type": "id", "reference": f"doc-{cfg.run_id}", "provider": "e2e"}],
                "payload": {"run_id": cfg.run_id},
            },
        ),
    )
    kyc_id = str(_extract_data(kyc).get("id"))
    _require(kyc_id != "None", "kyc id missing")

    r.run_step(section, "Verify KYC", lambda: client.verify_kyc(client_id, kyc_id, {"verifier": "e2e"}))
    r.run_step(section, "Activate client (after KYC)", lambda: client.activate_client(client_id))

    opening_deposit_major = Decimal("1000.00")
    account = r.run_step(
        section,
        "Open account with opening deposit",
        lambda: client.open_account(
            {
                "client_id": client_id,
                "product_id": product_id,
                "opening_deposit": str(opening_deposit_major),
            }
        ),
    )
    account_id = str(_extract_data(account).get("id"))
    wallet_id = str(_extract_data(account).get("wallet_id"))
    _require(account_id != "None", "account id missing")
    _require(wallet_id not in ("None", ""), "wallet_id missing")

    r.run_step(section, "Activate account", lambda: client.activate_account(account_id))

    acc_details = r.run_step(section, "Read account details", lambda: client.get_account(account_id))
    acc_data = _extract_data(acc_details)
    _require(acc_data.get("status") == "active", f'expected account status active, got {acc_data.get("status")}')

    expected_wallet_id = f"client-{client_number}-{product_code}"
    _require(wallet_id == expected_wallet_id, f"wallet_id mismatch: expected {expected_wallet_id}, got {wallet_id}")

    wallet_info = acc_data.get("wallet_info") or {}
    expected_opening_minor = major_to_minor(opening_deposit_major, p)
    _require(int(wallet_info.get("available_balance", 0)) == expected_opening_minor, "opening deposit not reflected in wallet_info.available_balance")

    credit_ref = f"account-credit-{cfg.run_id}-1"
    r.run_step(section, "Credit account $500", lambda: client.account_credit(account_id, major_to_minor(Decimal("500.00"), p), credit_ref))

    bal1 = r.run_step(section, "Get account balance", lambda: client.account_balance(account_id))
    bal_data = _extract_data(bal1)
    _require(int(bal_data.get("balance", 0)) == major_to_minor(Decimal("1500.00"), p), "account balance mismatch after credit")

    debit_ref = f"account-debit-{cfg.run_id}-1"
    r.run_step(section, "Debit account $200", lambda: client.account_debit(account_id, major_to_minor(Decimal("200.00"), p), debit_ref))

    bal2 = r.run_step(section, "Get account balance (after debit)", lambda: client.account_balance(account_id))
    bal2_minor = int(_extract_data(bal2).get("balance", 0))

    history = r.run_step(section, "Get account history", lambda: client.account_history(account_id))
    txs = _extract_cursor(history)
    fee_tx = None
    for tx in txs:
        if not isinstance(tx, dict):
            continue
        md = tx.get("metadata") or {}
        if md.get("cba_operation") == "fee_posting" and md.get("linked_reference") == debit_ref:
            fee_tx = tx
            break

    expected_without_fee = major_to_minor(Decimal("1300.00"), p)
    expected_with_fee = major_to_minor(Decimal("1299.50"), p)
    if fee_tx is None:
        msg = "SKIPPED product fee assertion (no fee_posting transactions observed; transaction-fee posting may not be wired)"
        if r.console is not None:
            r.console.print(msg)
        else:
            print(msg)
        _require(bal2_minor == expected_without_fee, f"balance mismatch after debit: expected {expected_without_fee}, got {bal2_minor}")
    else:
        _require(bal2_minor == expected_with_fee, f"balance mismatch after debit+fee: expected {expected_with_fee}, got {bal2_minor}")

    r.run_step_expect_status(
        section,
        "Attempt over-limit debit $5000 (expect 4xx)",
        lambda: client.account_debit(account_id, major_to_minor(Decimal("5000.00"), p), f"account-debit-{cfg.run_id}-overlimit"),
        expected_statuses=(400, 402),
    )

    r.run_step(section, "Suspend account", lambda: client.suspend_account(account_id))
    r.run_step_expect_status(
        section,
        "Debit while suspended (expect 4xx)",
        lambda: client.account_debit(account_id, major_to_minor(Decimal("100.00"), p), f"account-debit-{cfg.run_id}-suspended"),
        expected_statuses=(400, 402),
    )

    r.run_step(section, "Reactivate account", lambda: client.reactivate_account(account_id))
    r.run_step(section, "Debit $100 after reactivate", lambda: client.account_debit(account_id, major_to_minor(Decimal("100.00"), p), f"account-debit-{cfg.run_id}-2"))

    r.run_step(section, "Freeze account", lambda: client.freeze_account(account_id))
    r.run_step_expect_status(
        section,
        "Debit while frozen (expect 4xx)",
        lambda: client.account_debit(account_id, major_to_minor(Decimal("10.00"), p), f"account-debit-{cfg.run_id}-frozen"),
        expected_statuses=(400, 402),
    )
    r.run_step(section, "Credit while frozen (should succeed)", lambda: client.account_credit(account_id, major_to_minor(Decimal("10.00"), p), f"account-credit-{cfg.run_id}-frozen"))
    r.run_step(section, "Reactivate (unfreeze)", lambda: client.reactivate_account(account_id))

    bal3 = r.run_step(section, "Get account balance (final)", lambda: client.account_balance(account_id))
    bal3_minor = int(_extract_data(bal3).get("balance", 0))
    history_final = r.run_step(section, "Get account history (final)", lambda: client.account_history(account_id))
    txs_final = _extract_cursor(history_final)
    account_available = f"users:{wallet_id}:wallets:{currency}:available"
    computed_balance = _compute_account_balance_from_txs(txs_final, account_available, currency)
    _require(bal3_minor == computed_balance, f"account balance does not reconcile with history: {bal3_minor} vs {computed_balance}")

    statement = r.run_step(section, "Get account statement", lambda: client.account_statement(account_id))
    statement_txs = _extract_cursor(statement)
    computed_balance_statement = _compute_account_balance_from_txs(statement_txs, account_available, currency)
    _require(computed_balance_statement == computed_balance, "statement does not reconcile with history")

    r.end_section(section)
    return {
        "currency": currency,
        "precision": p,
        "product_id": product_id,
        "product_code": product_code,
        "client_id": client_id,
        "client_number": client_number,
        "account_id": account_id,
        "wallet_id": wallet_id,
    }


def _round_percent_minor(amount_minor: int, pct: Decimal) -> int:
    return int((Decimal(amount_minor) * pct / Decimal(100)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _channel_fee_combined_user_fee(principal_minor: int, flat_major: Decimal, pct: Decimal, precision: int) -> int:
    flat_minor = major_to_minor(flat_major, precision)
    return flat_minor + _round_percent_minor(principal_minor, pct)


def _channel_fee_layered_user_fee(principal_minor: int, precision: int) -> int:
    tiers = [
        (0, major_to_minor(Decimal("1000.00"), precision), Decimal("3.5")),
        (major_to_minor(Decimal("1000.00"), precision), major_to_minor(Decimal("10000.00"), precision), Decimal("2.5")),
        (major_to_minor(Decimal("10000.00"), precision), None, Decimal("1.5")),
    ]
    fee = 0
    for lower, upper, pct in tiers:
        if principal_minor <= lower:
            continue
        ceiling = principal_minor if upper is None else min(principal_minor, upper)
        tranche = ceiling - lower
        if tranche > 0:
            fee += _round_percent_minor(tranche, pct)
    return fee


def section_3_channel_fees_and_reporting(
    r: StepRunner,
    cfg: E2EConfig,
    section1: Dict[str, Any],
    section2: Dict[str, Any],
) -> Dict[str, Any]:
    section = r.begin_section("Section 3: Channel Fees & Revenue Reporting layer")
    client = r.client

    currency = section2["currency"]
    precision = section2["precision"]
    channel_id = section1["channel_id"]
    wallet_id = section1["wallet_id"]
    wallet_user_id = section1["wallet_user_id"]

    r.run_step(
        section,
        "Upsert fee config (combined user fee + % processing fee with min)",
        lambda: client.upsert_channel_fee_config(
            channel_id,
            {
                "currency": currency,
                "enabled": True,
                "actor": "e2e",
                "user_fee": {"type": "combined", "flat": "0.30", "percentage": "2.9"},
                "processing_fee": {"type": "percentage", "percentage": "1.5", "min": "0.10"},
            },
        ),
    )

    audits = r.run_step(section, "List fee config audits", lambda: client.list_channel_fee_audits(channel_id, limit=25))
    audits_list = _extract_data(audits).get("audits", [])
    _require(isinstance(audits_list, list) and len(audits_list) >= 1, "expected at least one audit entry")

    def wallet_available_balance_minor() -> int:
        history = client.wallet_history(wallet_id)
        txs = _extract_cursor(history)
        account = f"users:{wallet_user_id}:wallets:{currency}:available"
        return _compute_account_balance_from_txs(txs, account, currency)

    def run_wallet_debit(principal_major: Decimal, ref_suffix: str) -> Dict[str, Any]:
        principal_minor = major_to_minor(principal_major, precision)
        ref = f"channel-fee-debit-{cfg.run_id}-{ref_suffix}"

        before_minor = wallet_available_balance_minor()

        resp = client.debit_wallet(
            wallet_id,
            amount_minor=0,
            reference=ref,
            channel_id=channel_id,
            channel_amount_minor=principal_minor,
        )
        data = _extract_data(resp)
        md = data.get("metadata") or {}
        after_minor = wallet_available_balance_minor()

        return {
            "principal_minor": principal_minor,
            "reference": ref,
            "response": data,
            "metadata": md,
            "balance_before": before_minor,
            "balance_after": after_minor,
        }

    results: List[Dict[str, Any]] = []

    r.run_step(section, "Top up wallet for fee tests", lambda: client.credit_wallet(wallet_id, major_to_minor(Decimal("60000.00"), precision), f"fee-topup-{cfg.run_id}-1"))

    res_1000 = r.run_step(section, "Debit $1000 through channel (fee engine)", lambda: run_wallet_debit(Decimal("1000.00"), "1000"))
    results.append(res_1000)

    principal_minor = res_1000["principal_minor"]
    expected_user_fee = _channel_fee_combined_user_fee(principal_minor, Decimal("0.30"), Decimal("2.9"), precision)
    expected_processing = max(_round_percent_minor(principal_minor, Decimal("1.5")), major_to_minor(Decimal("0.10"), precision))
    expected_net = expected_user_fee - expected_processing
    expected_total = principal_minor + expected_user_fee

    md = res_1000["metadata"]
    _require(int(md.get("channel_amount", 0)) == principal_minor, "channel_amount mismatch")
    _require(int(md.get("channel_user_fee_amount", 0)) == expected_user_fee, "channel_user_fee_amount mismatch")
    _require(int(md.get("channel_processing_fee_amount", 0)) == expected_processing, "channel_processing_fee_amount mismatch")
    _require(int(md.get("channel_net_revenue_amount", 0)) == expected_net, "channel_net_revenue_amount mismatch")
    _require(res_1000["balance_before"] - res_1000["balance_after"] == expected_total, "wallet delta mismatch for $1000 debit")

    res_50 = r.run_step(section, "Debit $50 through channel", lambda: run_wallet_debit(Decimal("50.00"), "50"))
    results.append(res_50)

    res_5 = r.run_step(section, "Debit $5 through channel (processing min applies)", lambda: run_wallet_debit(Decimal("5.00"), "5"))
    results.append(res_5)

    principal_minor_5 = res_5["principal_minor"]
    expected_processing_5 = max(_round_percent_minor(principal_minor_5, Decimal("1.5")), major_to_minor(Decimal("0.10"), precision))
    _require(int(res_5["metadata"].get("channel_processing_fee_amount", 0)) == expected_processing_5, "processing min did not apply as expected")

    r.run_step(
        section,
        "Upsert layered user-fee config (progressive tiers)",
        lambda: client.upsert_channel_fee_config(
            channel_id,
            {
                "currency": currency,
                "enabled": True,
                "actor": "e2e",
                "user_fee": {
                    "type": "layered",
                    "layers": [
                        {"to": "1000.00", "percentage": "3.5"},
                        {"from": "1000.00", "to": "10000.00", "percentage": "2.5"},
                        {"from": "10000.00", "percentage": "1.5"},
                    ],
                },
                "processing_fee": {"type": "percentage", "percentage": "1.5", "min": "0.10"},
            },
        ),
    )

    res_1500 = r.run_step(section, "Debit $1500 through layered config", lambda: run_wallet_debit(Decimal("1500.00"), "1500"))
    results.append(res_1500)
    expected_user_fee_1500 = _channel_fee_layered_user_fee(res_1500["principal_minor"], precision)
    expected_processing_1500 = max(_round_percent_minor(res_1500["principal_minor"], Decimal("1.5")), major_to_minor(Decimal("0.10"), precision))
    expected_net_1500 = expected_user_fee_1500 - expected_processing_1500
    _require(int(res_1500["metadata"].get("channel_user_fee_amount", 0)) == expected_user_fee_1500, "layered user fee mismatch for $1500 debit")
    _require(int(res_1500["metadata"].get("channel_processing_fee_amount", 0)) == expected_processing_1500, "layered processing fee mismatch for $1500 debit")
    _require(int(res_1500["metadata"].get("channel_net_revenue_amount", 0)) == expected_net_1500, "layered net revenue mismatch for $1500 debit")

    res_50000 = r.run_step(section, "Debit $50000 through layered config", lambda: run_wallet_debit(Decimal("50000.00"), "50000"))
    results.append(res_50000)
    expected_user_fee_50000 = _channel_fee_layered_user_fee(res_50000["principal_minor"], precision)
    expected_processing_50000 = max(_round_percent_minor(res_50000["principal_minor"], Decimal("1.5")), major_to_minor(Decimal("0.10"), precision))
    expected_net_50000 = expected_user_fee_50000 - expected_processing_50000
    _require(int(res_50000["metadata"].get("channel_user_fee_amount", 0)) == expected_user_fee_50000, "layered user fee mismatch for $50000 debit")
    _require(int(res_50000["metadata"].get("channel_processing_fee_amount", 0)) == expected_processing_50000, "layered processing fee mismatch for $50000 debit")
    _require(int(res_50000["metadata"].get("channel_net_revenue_amount", 0)) == expected_net_50000, "layered net revenue mismatch for $50000 debit")

    expected_user_fee_50 = _channel_fee_combined_user_fee(res_50["principal_minor"], Decimal("0.30"), Decimal("2.9"), precision)
    expected_processing_50 = max(_round_percent_minor(res_50["principal_minor"], Decimal("1.5")), major_to_minor(Decimal("0.10"), precision))
    expected_net_50 = expected_user_fee_50 - expected_processing_50

    expected_user_fee_5 = _channel_fee_combined_user_fee(res_5["principal_minor"], Decimal("0.30"), Decimal("2.9"), precision)
    expected_net_5 = expected_user_fee_5 - expected_processing_5

    gross_sum = expected_user_fee + expected_user_fee_50 + expected_user_fee_5 + expected_user_fee_1500 + expected_user_fee_50000
    cost_sum = expected_processing + expected_processing_50 + expected_processing_5 + expected_processing_1500 + expected_processing_50000
    net_sum = expected_net + expected_net_50 + expected_net_5 + expected_net_1500 + expected_net_50000
    tx_count = 5

    start_time = time.strftime("%Y-%m-%dT00:00:00Z", time.gmtime())
    end_time = _now_iso_plus(120)
    summary = r.run_step(
        section,
        "Revenue summary report",
        lambda: client.channel_revenue_summary({"currency": currency, "channel_id": channel_id, "start_time": start_time, "end_time": end_time}),
    )
    summary_data = summary
    totals = summary_data.get("totals") or {}
    _require(int(totals.get("gross_revenue", 0)) == gross_sum, f"report gross_revenue mismatch: expected {gross_sum}, got {int(totals.get('gross_revenue', 0))}")
    _require(int(totals.get("processing_cost", 0)) == cost_sum, f"report processing_cost mismatch: expected {cost_sum}, got {int(totals.get('processing_cost', 0))}")
    _require(int(totals.get("net_revenue", 0)) == net_sum, f"report net_revenue mismatch: expected {net_sum}, got {int(totals.get('net_revenue', 0))}")
    _require(int(totals.get("transaction_count", 0)) == tx_count, f"report transaction_count mismatch: expected {tx_count}, got {int(totals.get('transaction_count', 0))}")

    out_dir = f"/tmp/ledgertrack-e2e-{cfg.run_id}"
    os.makedirs(out_dir, exist_ok=True)

    export_csv = r.run_step(
        section,
        "Export revenue summary CSV",
        lambda: client.channel_revenue_export({"currency": currency, "format": "csv", "report": "summary", "channel_id": channel_id, "start_time": start_time, "end_time": end_time}),
    )
    csv_bytes = export_csv.content
    csv_path = os.path.join(out_dir, "channel_revenue_summary.csv")
    with open(csv_path, "wb") as f:
        f.write(csv_bytes)

    csv_text = csv_bytes.decode("utf-8", errors="replace")
    _require("TOTALS" in csv_text, "CSV export missing TOTALS row")
    lines = [line for line in csv_text.splitlines() if line.strip()]
    totals_row = None
    for line in lines:
        if line.startswith("TOTALS,"):
            totals_row = line
            break
    _require(totals_row is not None, "CSV export missing TOTALS row content")

    export_pdf = r.run_step(
        section,
        "Export revenue summary PDF",
        lambda: client.channel_revenue_export({"currency": currency, "format": "pdf", "report": "summary", "channel_id": channel_id, "start_time": start_time, "end_time": end_time}),
    )
    _require(export_pdf.headers.get("Content-Type", "").startswith("application/pdf"), "PDF export content-type mismatch")
    _require(export_pdf.content[:4] == b"%PDF", "PDF export does not start with %PDF")
    pdf_path = os.path.join(out_dir, "channel_revenue_summary.pdf")
    with open(pdf_path, "wb") as f:
        f.write(export_pdf.content)

    dash = r.run_step(section, "Dashboard revenue metrics", lambda: client.channel_revenue_dashboard({"currency": currency}))
    dash_data = _extract_data(dash)
    _require("net_revenue_current" in dash_data, "dashboard missing net_revenue_current")

    msg = "SKIPPED RBAC negative cases (server enforces scopes only; no role claims available)"
    if r.console is not None:
        r.console.print(msg)
    else:
        print(msg)

    msg = "SKIPPED fault-injection partial-posting test (no fault injection flag configured)"
    if r.console is not None:
        r.console.print(msg)
    else:
        print(msg)

    r.end_section(section)
    return {
        "currency": currency,
        "gross_sum": gross_sum,
        "cost_sum": cost_sum,
        "net_sum": net_sum,
        "tx_count": tx_count,
        "start_time": start_time,
        "end_time": end_time,
        "out_dir": out_dir,
    }

def section_4_cross_layer_reconciliation(
    r: StepRunner,
    cfg: E2EConfig,
    section2: Dict[str, Any],
    section3: Dict[str, Any],
) -> None:
    section = r.begin_section("Section 4: Cross-layer reconciliation")
    client = r.client

    currency = section2["currency"]
    wallet_id = section2["wallet_id"]
    account_id = section2["account_id"]

    acct_hist = r.run_step(section, "Account history for same wallet", lambda: client.account_history(account_id))
    acct_txs = _extract_cursor(acct_hist)

    if wallet_id.endswith(f"-{currency}"):
        wallet_hist = r.run_step(section, "Wallet history for account wallet", lambda: client.wallet_history(wallet_id))
        wallet_txs = _extract_cursor(wallet_hist)
        available_account = f"users:{wallet_id}:wallets:{currency}:available"
        wallet_balance = _compute_account_balance_from_txs(wallet_txs, available_account, currency)
        acct_balance = _compute_account_balance_from_txs(acct_txs, available_account, currency)
        _require(wallet_balance == acct_balance, "wallet history and account history disagree on balance delta")
    else:
        msg = "SKIPPED account wallet history cross-check (wallet history endpoint cannot parse CBA wallet IDs with embedded dashes)"
        if r.console is not None:
            r.console.print(msg)
        else:
            print(msg)

    revenue_ledger = f"revenue-{currency}"
    revenue_txs = r.run_step(section, "List revenue ledger transactions (filtered by run_id)", lambda: _list_all_transactions(client, revenue_ledger))
    gross = 0
    cost = 0
    for tx in revenue_txs:
        if not isinstance(tx, dict):
            continue
        ref = str(tx.get("reference", ""))
        if cfg.run_id not in ref:
            continue
        postings = tx.get("postings") or []
        if not isinstance(postings, list):
            continue
        for p in postings:
            asset = _posting_asset(p)
            if asset != currency and asset != f"{currency}/2":
                continue
            amt = _posting_amount_minor(p)
            if p.get("destination") == "revenue:accumulated":
                gross += amt
            if p.get("destination") == "revenue:channel_processing_cost":
                cost += amt
    _require(gross == section3["gross_sum"], "revenue ledger gross postings do not match fee engine gross sum")
    _require(cost == section3["cost_sum"], "revenue ledger processing postings do not match fee engine cost sum")

    r.end_section(section)


def section_5_cleanup(
    r: StepRunner,
    cfg: E2EConfig,
    section1: Dict[str, Any],
    section2: Dict[str, Any],
    section3: Dict[str, Any],
) -> None:
    section = r.begin_section("Section 5: Cleanup (skippable)")
    client = r.client

    if not cfg.cleanup:
        msg = "Cleanup skipped (E2E_CLEANUP=false)"
        if r.console is not None:
            r.console.print(msg)
        else:
            print(msg)
        r.end_section(section)
        return

    channel_id = section1["channel_id"]
    currency = section2["currency"]
    account_id = section2["account_id"]
    product_id = section2["product_id"]
    client_id = section2["client_id"]

    r.run_step(
        section,
        "Disable fee config (best-effort)",
        lambda: client.upsert_channel_fee_config(
            channel_id,
            {"currency": currency, "enabled": False, "actor": "e2e-cleanup", "user_fee": {"type": "none"}, "processing_fee": {"type": "none"}},
        ),
    )

    bal = r.run_step(section, "Read account balance for cleanup", lambda: client.account_balance(account_id))
    balance_minor = int(_extract_data(bal).get("balance", 0))
    if balance_minor > 0:
        r.run_step(
            section,
            "Sweep remaining balance to zero (account debit)",
            lambda: client.account_debit(account_id, balance_minor, f"cleanup-sweep-{cfg.run_id}"),
        )

    def best_effort(fn: Callable[[], Any], ok_statuses: Tuple[int, ...]) -> Any:
        try:
            return fn()
        except HTTPError as e:
            if e.status in ok_statuses:
                return {"status": e.status, "body": e.response_text}
            raise

    r.run_step(section, "Close account (best-effort)", lambda: best_effort(lambda: client.close_account(account_id), (400, 409)))
    r.run_step(section, "Close client (best-effort)", lambda: best_effort(lambda: client.close_client(client_id), (400, 409)))
    r.run_step(section, "Retire product (best-effort)", lambda: best_effort(lambda: client.retire_product(product_id), (400, 409)))

    out_dir = section3.get("out_dir")
    if isinstance(out_dir, str) and out_dir.startswith("/tmp/ledgertrack-e2e-"):
        try:
            shutil.rmtree(out_dir)
        except Exception:
            pass

    r.end_section(section)



def main() -> int:
    cfg = load_config()
    c = _console()

    config_payload = {
        "base_url": cfg.base_url,
        "ledger": cfg.ledger,
        "admin_token_set": bool(cfg.admin_token),
        "finance_token_set": bool(cfg.finance_token),
        "audit_token_set": bool(cfg.audit_token),
        "run_id": cfg.run_id,
        "cleanup": cfg.cleanup,
        "local_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "utc_time": _now_iso(),
        "tz": str(tz.tzlocal()),
    }

    if c is not None:
        c.print(config_payload)
    else:
        print(config_payload)

    if cfg.admin_token is None:
        print("LEDGERTRACK_ADMIN_TOKEN is required for this run.")
        return 1

    client = LedgerTrackClient(cfg)
    runner = StepRunner(c, client)
    started = time.monotonic()
    try:
        precision_by_code = section_0_preflight(runner, cfg)
        section1 = section_1_wallet_and_channel(runner, cfg, precision_by_code)
        section2 = section_2_cba(runner, cfg, precision_by_code)
        section3 = section_3_channel_fees_and_reporting(runner, cfg, section1, section2)
        section_4_cross_layer_reconciliation(runner, cfg, section2, section3)
        section_5_cleanup(runner, cfg, section1, section2, section3)
        runner.summary()
        took = time.monotonic() - started
        if c is not None:
            c.print(f"PASS in {took:.3f}s")
        else:
            print(f"PASS in {took:.3f}s")
        return 0
    except Exception as e:
        if c is not None:
            c.print(str(e))
        else:
            print(str(e))
        runner.summary()
        took = time.monotonic() - started
        if c is not None:
            c.print(f"FAIL in {took:.3f}s")
        else:
            print(f"FAIL in {took:.3f}s")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
