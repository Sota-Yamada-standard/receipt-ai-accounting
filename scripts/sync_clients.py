#!/usr/bin/env python3
import os
import time
import json
import math
import argparse
import requests
from datetime import datetime
from typing import Dict, Any, Iterable


def get_env(name: str, required: bool = True) -> str:
    val = os.getenv(name, '')
    if required and not val:
        raise RuntimeError(f"Environment variable {name} is required")
    return val


def notion_iter_pages(database_id: str, token: str) -> Iterable[Dict[str, Any]]:
    base = 'https://api.notion.com/v1'
    headers = {
        'Authorization': f'Bearer {token}',
        'Notion-Version': '2025-09-03',
        'Content-Type': 'application/json',
    }
    # Prefer data_sources querying
    resp = requests.get(f"{base}/databases/{database_id}", headers=headers, timeout=20)
    resp.raise_for_status()
    meta = resp.json()
    data_sources = meta.get('data_sources', []) if isinstance(meta, dict) else []

    def _post(path: str, body: Dict[str, Any]):
        r = requests.post(f"{base}/{path}", headers=headers, json=body, timeout=30)
        r.raise_for_status()
        return r.json()

    fetched = 0
    if data_sources:
        ds_id = data_sources[0].get('id')
        next_cursor = None
        while True:
            body = {'page_size': 100}
            if next_cursor:
                body['start_cursor'] = next_cursor
            data = _post(f"data_sources/{ds_id}/query", body)
            results = data.get('results', [])
            for r in results:
                yield r
            fetched += len(results)
            if data.get('has_more') and data.get('next_cursor'):
                next_cursor = data['next_cursor']
                continue
            break

    if fetched == 0:
        next_cursor = None
        while True:
            body = {'page_size': 100}
            if next_cursor:
                body['start_cursor'] = next_cursor
            data = _post(f"databases/{database_id}/query", body)
            results = data.get('results', [])
            for r in results:
                yield r
            fetched += len(results)
            if data.get('has_more') and data.get('next_cursor'):
                next_cursor = data['next_cursor']
                continue
            break


def _get_title(props: Dict[str, Any]) -> str:
    if '顧客名' in props and props['顧客名'].get('type') == 'title':
        return ''.join([t.get('plain_text', '') for t in props['顧客名'].get('title', [])]).strip()
    if 'Name' in props and props['Name'].get('type') == 'title':
        return ''.join([t.get('plain_text', '') for t in props['Name'].get('title', [])]).strip()
    for v in props.values():
        if v.get('type') == 'title':
            return ''.join([t.get('plain_text', '') for t in v.get('title', [])]).strip()
    return ''


def _acc_app(props: Dict[str, Any]) -> str:
    candidates = ['AccountingApp', '会計ソフト', '会計システム', 'Accounting', 'App', 'Software']
    for key in candidates:
        if key in props and props[key].get('type') in ('select', 'multi_select'):
            sel = props[key].get('select') or (props[key].get('multi_select') or [])
            if isinstance(sel, dict):
                return (sel.get('name') or '').strip()
            if isinstance(sel, list) and sel:
                return (sel[0].get('name') or '').strip()
    for v in props.values():
        if v.get('type') in ('select', 'multi_select'):
            if v.get('select'):
                return (v['select'].get('name') or '').strip()
            elif v.get('multi_select'):
                arr = v['multi_select']
                if arr:
                    return (arr[0].get('name') or '').strip()
    return ''


def _contract_ok(props: Dict[str, Any]) -> bool:
    key = '契約区分'
    values = []
    if key in props:
        p = props[key]
        t = p.get('type')
        if t == 'select' and p.get('select'):
            values = [(p['select'].get('name') or '').strip()]
        elif t == 'multi_select' and p.get('multi_select'):
            values = [(x.get('name') or '').strip() for x in p['multi_select']]
        elif t in ('rich_text', 'title'):
            arr = p.get('rich_text') or p.get('title') or []
            values = [''.join([x.get('plain_text', '') for x in arr]).strip()]
    text = ' '.join(values)
    if not text:
        return False
    if '会計' not in text:
        return False
    if ('解約' in text) or ('停止' in text):
        return False
    return True


def _company_id(props: Dict[str, Any]) -> str:
    candidates = ['CompanyId', 'company_id', 'freee_company_id', 'FreeeCompanyId', '会社ID', '顧客ID', 'freee会社ID']
    for key in candidates:
        if key in props:
            comp = props[key]
            if comp.get('type') == 'number' and comp.get('number') is not None:
                return str(comp['number'])
            if comp.get('type') in ('rich_text', 'title'):
                arr = comp.get('rich_text') or comp.get('title') or []
                if arr:
                    return ''.join([t.get('plain_text', '') for t in arr]).strip()
    for v in props.values():
        if v.get('type') == 'number' and v.get('number') is not None:
            return str(v['number'])
    for v in props.values():
        if v.get('type') in ('rich_text', 'title'):
            arr = v.get('rich_text') or v.get('title') or []
            if arr:
                return ''.join([t.get('plain_text', '') for t in arr]).strip()
    return ''


def _customer_code(props: Dict[str, Any]) -> str:
    candidates = ['顧客コード', 'customer_code', 'CustomerCode', '顧客CD', 'ClientCode']
    for key in candidates:
        if key in props:
            comp = props[key]
            if comp.get('type') == 'number' and comp.get('number') is not None:
                return str(comp['number'])
            if comp.get('type') in ('rich_text', 'title'):
                arr = comp.get('rich_text') or comp.get('title') or []
                if arr:
                    return ''.join([t.get('plain_text', '') for t in arr]).strip()
            if comp.get('type') == 'select' and comp.get('select'):
                return (comp['select'].get('name') or '').strip()
    return ''


def get_rest_token(sa_info: Dict[str, Any]) -> str:
    from google.oauth2 import service_account
    from google.auth.transport.requests import Request as GARequest
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=['https://www.googleapis.com/auth/datastore'])
    creds.refresh(GARequest())
    return str(creds.token)


def run_sync() -> Dict[str, int]:
    notion_token = get_env('NOTION_TOKEN')
    database_id = get_env('NOTION_DATABASE_ID')
    sa_json = get_env('FIREBASE_SERVICE_ACCOUNT_JSON')
    sa = json.loads(sa_json)
    project_id = sa.get('project_id')
    token = get_rest_token(sa)

    # 既存の name -> docId を先に取得（REST）
    url_query = f"https://firestore.googleapis.com/v1/projects/{project_id}/databases/(default)/documents:runQuery"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {"structuredQuery": {"from": [{"collectionId": "clients"}], "select": {"fields": [{"fieldPath": "name"}]}}}
    existing: Dict[str, str] = {}
    resp = requests.post(url_query, headers=headers, json=body, timeout=30)
    if resp.status_code == 200:
        for line in resp.json():
            doc = (line.get('document') or {})
            name = ((doc.get('fields') or {}).get('name') or {}).get('stringValue')
            if name:
                # name->pathの末尾がdocId
                path = doc.get('name', '')
                if path:
                    existing[name] = path.split('/')[-1]

    results = {'updated': 0, 'created': 0, 'skipped': 0}

    def _doc_body(upd: Dict[str, Any]) -> Dict[str, Any]:
        def sv(v: str):
            return {"stringValue": v}
        def bv(b: bool):
            return {"booleanValue": b}
        def ts():
            return {"timestampValue": datetime.utcnow().isoformat(timespec='seconds') + 'Z'}
        fields = {
            'accounting_app': sv(upd.get('accounting_app', '')),
            'external_company_id': sv(upd.get('external_company_id', '')),
            'customer_code': sv(upd.get('customer_code', '')),
            'contract_ok': bv(upd.get('contract_ok', False)),
            'updated_at': ts(),
        }
        if 'name' in upd:
            fields['name'] = sv(upd['name'])
        if 'created_at' in upd:
            fields['created_at'] = ts()
        return {"fields": fields}

    # バッチ（RESTは最大500/バッチだが、レート対策で控えめに100件）
    batch_size = 100
    to_commit = []

    def commit_batch():
        nonlocal to_commit, token
        if not to_commit:
            return
        # RESTのバッチはv1での複数リクエスト合成がないため、1件ずつ呼び出しつつ指数バックオフ
        for req in to_commit:
            for attempt in range(6):
                try:
                    r = requests.request(**req, timeout=30)
                    if r.status_code in (200, 201):
                        break
                    if r.status_code in (429, 500, 503):
                        time.sleep(2 ** attempt)
                        continue
                    r.raise_for_status()
                    break
                except Exception:
                    time.sleep(2 ** attempt)
                    if attempt == 5:
                        raise
        to_commit = []
        time.sleep(0.2)

    for p in notion_iter_pages(database_id, notion_token):
        props = p.get('properties', {})
        name = _get_title(props)
        if not name:
            results['skipped'] += 1
            continue
        upd = {
            'accounting_app': _acc_app(props),
            'external_company_id': _company_id(props),
            'customer_code': _customer_code(props),
            'contract_ok': _contract_ok(props),
        }
        if name in existing:
            doc_id = existing[name]
            url = f"https://firestore.googleapis.com/v1/projects/{project_id}/databases/(default)/documents/clients/{doc_id}?updateMask.fieldPaths=accounting_app&updateMask.fieldPaths=external_company_id&updateMask.fieldPaths=customer_code&updateMask.fieldPaths=contract_ok&updateMask.fieldPaths=updated_at"
            to_commit.append({
                'method': 'PATCH',
                'url': url,
                'headers': headers,
                'json': _doc_body(upd),
            })
            results['updated'] += 1
        else:
            url = f"https://firestore.googleapis.com/v1/projects/{project_id}/databases/(default)/documents/clients"
            new_doc = {**upd, 'name': name, 'created_at': True}
            to_commit.append({
                'method': 'POST',
                'url': url,
                'headers': headers,
                'json': _doc_body(new_doc),
            })
            results['created'] += 1
        if len(to_commit) >= batch_size:
            commit_batch()
    commit_batch()
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    start = time.time()
    res = run_sync()
    dur = int(time.time() - start)
    print(json.dumps({'result': res, 'seconds': dur}, ensure_ascii=False))


if __name__ == '__main__':
    main()



