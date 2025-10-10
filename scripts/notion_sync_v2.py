#!/usr/bin/env python3
"""
Notion -> Firestore (clients_v2) daily sync
 - Save ONLY contract_ok == True clients
 - Deterministic docId = Notion page id
 - Delete doc when contract_ok becomes False
 - Append result log to Firestore collection 'sync_logs'

ENV VARS (GitHub Actions Secrets recommended):
  - FIREBASE_SERVICE_ACCOUNT_JSON
  - NOTION_TOKEN
  - NOTION_DATABASE_ID
"""
import os
import json
import time
import requests
from datetime import datetime, timezone
from typing import Dict, Any, Iterable, Tuple


def get_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise RuntimeError(f"Missing env: {name}")
    return v


def get_firebase_token(sa_info: Dict[str, Any]) -> Tuple[str, str]:
    from google.oauth2 import service_account
    from google.auth.transport.requests import Request as GARequest
    creds = service_account.Credentials.from_service_account_info(
        sa_info, scopes=["https://www.googleapis.com/auth/datastore"]
    )
    creds.refresh(GARequest())
    return str(creds.token), sa_info.get("project_id", "")


def notion_iter_pages(database_id: str, token: str) -> Iterable[Dict[str, Any]]:
    base = "https://api.notion.com/v1"
    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": "2025-09-03",
        "Content-Type": "application/json",
    }

    # DB meta（data_sources を優先）
    r = requests.get(f"{base}/databases/{database_id}", headers=headers, timeout=30)
    r.raise_for_status()
    meta = r.json()
    data_sources = meta.get("data_sources", []) if isinstance(meta, dict) else []

    def _post(path: str, body: Dict[str, Any]):
        rr = requests.post(f"{base}/{path}", headers=headers, json=body, timeout=45)
        rr.raise_for_status()
        return rr.json()

    fetched = 0
    if data_sources:
        ds_id = data_sources[0].get("id")
        next_cursor = None
        while True:
            body = {"page_size": 100}
            if next_cursor:
                body["start_cursor"] = next_cursor
            data = _post(f"data_sources/{ds_id}/query", body)
            results = data.get("results", [])
            for r in results:
                yield r
            fetched += len(results)
            if data.get("has_more") and data.get("next_cursor"):
                next_cursor = data["next_cursor"]
                continue
            break

    if fetched == 0:
        next_cursor = None
        while True:
            body = {"page_size": 100}
            if next_cursor:
                body["start_cursor"] = next_cursor
            data = _post(f"databases/{database_id}/query", body)
            results = data.get("results", [])
            for r in results:
                yield r
            fetched += len(results)
            if data.get("has_more") and data.get("next_cursor"):
                next_cursor = data["next_cursor"]
                continue
            break


def _title(props: Dict[str, Any]) -> str:
    # 題名: '顧客名' または 'Name' を優先
    if "顧客名" in props and props["顧客名"].get("type") == "title":
        return "".join(t.get("plain_text", "") for t in props["顧客名"].get("title", [])).strip()
    if "Name" in props and props["Name"].get("type") == "title":
        return "".join(t.get("plain_text", "") for t in props["Name"].get("title", [])).strip()
    for v in props.values():
        if v.get("type") == "title":
            return "".join(t.get("plain_text", "") for t in v.get("title", [])).strip()
    return ""


def _acc_app(props: Dict[str, Any]) -> str:
    candidates = ["AccountingApp", "会計ソフト", "会計システム", "Accounting", "App", "Software"]
    for key in candidates:
        if key in props and props[key].get("type") in ("select", "multi_select"):
            sel = props[key].get("select") or (props[key].get("multi_select") or [])
            if isinstance(sel, dict):
                return (sel.get("name") or "").strip()
            if isinstance(sel, list) and sel:
                return (sel[0].get("name") or "").strip()
    for v in props.values():
        if v.get("type") in ("select", "multi_select"):
            if v.get("select"):
                return (v["select"].get("name") or "").strip()
            elif v.get("multi_select"):
                arr = v["multi_select"]
                if arr:
                    return (arr[0].get("name") or "").strip()
    return ""


def _contract_ok(props: Dict[str, Any]) -> bool:
    key = "契約区分"
    values = []
    if key in props:
        p = props[key]
        t = p.get("type")
        if t == "select" and p.get("select"):
            values = [(p["select"].get("name") or "").strip()]
        elif t == "multi_select" and p.get("multi_select"):
            values = [(x.get("name") or "").strip() for x in p["multi_select"]]
        elif t in ("rich_text", "title"):
            arr = p.get("rich_text") or p.get("title") or []
            values = ["".join(x.get("plain_text", "") for x in arr).strip()]
    text = " ".join(values)
    if not text:
        return False
    if "会計" not in text:
        return False
    if ("解約" in text) or ("停止" in text):
        return False
    return True


def _customer_code(props: Dict[str, Any]) -> str:
    candidates = ["顧客コード", "customer_code", "CustomerCode", "顧客CD", "ClientCode"]
    for key in candidates:
        if key in props:
            comp = props[key]
            if comp.get("type") == "number" and comp.get("number") is not None:
                return str(comp["number"])
            if comp.get("type") in ("rich_text", "title"):
                arr = comp.get("rich_text") or comp.get("title") or []
                if arr:
                    return "".join(t.get("plain_text", "") for t in arr).strip()
            if comp.get("type") == "select" and comp.get("select"):
                return (comp["select"].get("name") or "").strip()
    return ""


def _sv(v: str) -> Dict[str, Any]:
    return {"stringValue": v if v is not None else ""}


def _bv(b: bool) -> Dict[str, Any]:
    return {"booleanValue": bool(b)}


def _ts(dt: datetime = None) -> Dict[str, Any]:
    d = dt or datetime.now(timezone.utc)
    return {"timestampValue": d.isoformat(timespec="seconds").replace("+00:00", "Z")}


def upsert_client(token: str, project_id: str, page_id: str, data: Dict[str, Any]) -> None:
    url = f"https://firestore.googleapis.com/v1/projects/{project_id}/databases/(default)/documents/clients_v2/{page_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {
        "fields": {
            "name": _sv(data.get("name", "")),
            "customer_code": _sv(data.get("customer_code", "")),
            "accounting_app": _sv(data.get("accounting_app", "")),
            "contract_ok": _bv(True),
            "notion_page_id": _sv(page_id),
            "updated_at": _ts(),
        }
    }
    # PATCHでupsert（存在しなくても作成される）
    r = requests.patch(url, headers=headers, json=body, timeout=30)
    if r.status_code not in (200, 201):
        # 初回は作成エンドポイント経由
        url_create = f"https://firestore.googleapis.com/v1/projects/{project_id}/databases/(default)/documents/clients_v2?documentId={page_id}"
        r2 = requests.post(url_create, headers=headers, json=body, timeout=30)
        r2.raise_for_status()


def delete_client(token: str, project_id: str, page_id: str) -> None:
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://firestore.googleapis.com/v1/projects/{project_id}/databases/(default)/documents/clients_v2/{page_id}"
    requests.delete(url, headers=headers, timeout=20)


def append_log(token: str, project_id: str, log: Dict[str, Any]) -> None:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url = f"https://firestore.googleapis.com/v1/projects/{project_id}/databases/(default)/documents/sync_logs"
    body = {"fields": {}}
    for k, v in log.items():
        if isinstance(v, bool):
            body["fields"][k] = _bv(v)
        elif isinstance(v, int):
            body["fields"][k] = {"integerValue": str(v)}
        elif isinstance(v, datetime):
            body["fields"][k] = _ts(v)
        else:
            body["fields"][k] = _sv(str(v))
    requests.post(url, headers=headers, json=body, timeout=20)


def main():
    start = time.time()
    notion_token = get_env("NOTION_TOKEN")
    database_id = get_env("NOTION_DATABASE_ID")
    sa_json = get_env("FIREBASE_SERVICE_ACCOUNT_JSON")
    sa = json.loads(sa_json)
    fb_token, project_id = get_firebase_token(sa)

    counters = {"updated": 0, "created": 0, "deleted": 0, "skipped": 0, "processed": 0, "fetched": 0}
    try:
        for page in notion_iter_pages(database_id, notion_token):
            counters["fetched"] += 1
            props = page.get("properties", {})
            page_id = page.get("id", "").replace("-", "")
            if not page_id:
                counters["skipped"] += 1
                continue
            name = _title(props)
            if not name:
                counters["skipped"] += 1
                continue
            if not _contract_ok(props):
                # NGは削除（存在すれば）
                delete_client(fb_token, project_id, page_id)
                counters["deleted"] += 1
                counters["processed"] += 1
                continue
            data = {
                "name": name,
                "customer_code": _customer_code(props),
                "accounting_app": _acc_app(props),
            }
            # upsert
            upsert_client(fb_token, project_id, page_id, data)
            counters["updated"] += 1
            counters["processed"] += 1

        ok = True
        status = "ok"
    except Exception as e:  # noqa: BLE001
        ok = False
        status = f"error: {e}"
    # log
    append_log(
        fb_token,
        project_id,
        {
            "kind": "notion_sync",
            "trigger": "workflow",
            "project_id": project_id,
            "updated": counters["updated"],
            "created": counters["created"],
            "deleted": counters["deleted"],
            "skipped": counters["skipped"],
            "fetched": counters["fetched"],
            "processed": counters["processed"],
            "started_at": datetime.fromtimestamp(start, tz=timezone.utc),
            "finished_at": datetime.now(timezone.utc),
            "duration_sec": int(time.time() - start),
            "ok": ok,
            "status": status,
        },
    )


if __name__ == "__main__":
    main()


