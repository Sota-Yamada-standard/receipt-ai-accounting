# redeploy: sync to latest code (force update)
import streamlit as st
import os
import re
import pandas as pd
from datetime import datetime
import json
from google.cloud import vision
import requests
from pdf2image import convert_from_bytes
import tempfile
import platform
import io
from PyPDF2 import PdfReader
from PIL import Image
import unicodedata
import firebase_admin
from firebase_admin import credentials, firestore
import time
from pandas import Index
import threading

# Notion
try:
    from notion_client import Client as NotionClient  # type: ignore
    NOTION_AVAILABLE = True
except Exception:
    NOTION_AVAILABLE = False

# freee API機能をインポート
from freee_api_helper import (
    initialize_freee_api, get_freee_companies, get_freee_accounts, get_freee_partners,
    create_freee_journal_entry, upload_freee_receipt,
    find_freee_account_by_name, find_freee_partner_by_name,
    render_customer_selection_ui, render_freee_api_ui
)

print("【DEBUG: app.py 実行開始】")
# ベクトル検索用ライブラリ（遅延ロードに変更）
VECTOR_SEARCH_AVAILABLE = True  # デフォルトTrue。失敗時に関数側でFalse扱い

# HEIC対応（将来的に対応予定）
# try:
#     import pillow_heif
#     HEIC_SUPPORT = True
# except ImportError:
#     HEIC_SUPPORT = False

# --- レビュー機能フラグ（将来復活を見据えてスイッチ可能に） ---
# False: レビューUI・保存処理をオミット（無効化）
# True : レビュー機能を有効化
REVIEW_FEATURE_ENABLED = False

# OpenAI APIキーをSecretsから取得
OPENAI_API_KEY = st.secrets.get("OPENAI_API_KEY", "")

# Streamlit CloudのSecretsからサービスアカウントJSONを一時ファイルに保存
if "GOOGLE_APPLICATION_CREDENTIALS_JSON" in st.secrets:
    key_path = "/tmp/gcp_key.json"
    key_dict = json.loads(st.secrets["GOOGLE_APPLICATION_CREDENTIALS_JSON"].strip())
    with open(key_path, "w") as f:
        json.dump(key_dict, f)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

# Cloudmersive APIキーをSecretsから取得
CLOUDMERSIVE_API_KEY = st.secrets.get("CLOUDMERSIVE_API_KEY", "")

# PDF.co APIキーをSecretsから取得
PDFCO_API_KEY = st.secrets.get("PDFCO_API_KEY", "")

# Firebase初期化
def initialize_firebase():
    """Firebase Admin SDKを初期化"""
    try:
        # 既に初期化されているかチェック
        firebase_admin.get_app()
        return firestore.client()
    except ValueError:
        # 初期化されていない場合は初期化
        try:
            if "FIREBASE_SERVICE_ACCOUNT_JSON" in st.secrets:
                # Streamlit Secretsからサービスアカウント情報を取得
                service_account_info = json.loads(st.secrets["FIREBASE_SERVICE_ACCOUNT_JSON"])
                cred = credentials.Certificate(service_account_info)
                firebase_admin.initialize_app(cred)
                return firestore.client()
            else:
                st.error("Firebaseサービスアカウントの設定が見つかりません。")
                return None
        except Exception as e:
            st.error(f"Firebase初期化エラー: {e}")
            return None
    except Exception as e:
        st.error(f"Firebase接続エラー: {e}")
        return None

# Firestoreクライアント（遅延初期化）
db = None

# --- clientsコレクション名（v2優先） ---
def clients_collection_name() -> str:
    # 今後はv2を正とする。必要なら環境で切替可能にする余地を残す。
    return 'clients_v2'

def _clients_rest_base(project_id: str) -> str:
    return f"https://firestore.googleapis.com/v1/projects/{project_id}/databases/(default)/documents/{clients_collection_name()}"

def get_db():
    global db
    if db is not None:
        return db
    try:
        db = initialize_firebase()
        return db
    except Exception as e:
        st.error(f"Firebase初期化で予期しないエラーが発生しました: {e}")
        db = None
        return None

# Firebase接続のデバッグ表示（デバッグモード時のみ表示）

# 接続先のproject_idをSecretsから取得（UI診断用）
def _get_project_id_from_secrets() -> str:
    try:
        import json as _json
        sa_raw = st.secrets.get('FIREBASE_SERVICE_ACCOUNT_JSON', '')
        if not sa_raw:
            return ''
        return (_json.loads(sa_raw) or {}).get('project_id', '') or ''
    except Exception:
        return ''


# ===== 顧問先（クライアント）管理と学習データ =====
def _load_clients_from_db():
    # Firestore RESTで安全に一覧取得（gRPCハング対策）
    def _rest_fetch() -> list:
        import json as _json
        import requests as _rq
        from google.oauth2 import service_account as _sa
        from google.auth.transport.requests import Request as _GARequest
        sa = _json.loads(st.secrets.get('FIREBASE_SERVICE_ACCOUNT_JSON', '{}'))
        if not sa:
            return []
        creds = _sa.Credentials.from_service_account_info(sa, scopes=['https://www.googleapis.com/auth/datastore'])
        creds.refresh(_GARequest())
        token = creds.token
        # listDocuments でページング取得（優先）
        items = []
        page_token = None
        while True:
            params = {"pageSize": 1000}
            if page_token:
                params["pageToken"] = page_token
            r = _rq.get(_clients_rest_base(sa.get('project_id')), headers={"Authorization": f"Bearer {token}"}, params=params, timeout=20)
            r.raise_for_status()
            data = r.json()
            for doc in data.get('documents', []) or []:
                fields = doc.get('fields', {})
                def _sv2(key):
                    v = fields.get(key)
                    if not v:
                        return ''
                    return v.get('stringValue') or v.get('integerValue') or v.get('booleanValue') or ''
                def _bool_any2(key):
                    v = fields.get(key)
                    if not v:
                        return None
                    if 'booleanValue' in v:
                        return bool(v.get('booleanValue'))
                    if 'integerValue' in v:
                        try:
                            return int(v.get('integerValue')) == 1
                        except Exception:
                            return None
                    if 'stringValue' in v:
                        return str(v.get('stringValue','')).strip().lower() in ('true','1','yes','ok')
                    return None
                items.append({
                    'id': doc.get('name', '').split('/')[-1],
                    'name': _sv2('name'),
                    'customer_code': _sv2('customer_code'),
                    'accounting_app': _sv2('accounting_app'),
                    'external_company_id': _sv2('external_company_id'),
                    'contract_ok': _bool_any2('contract_ok'),
                    'notion_page_id': _sv2('notion_page_id'),
                })
            page_token = data.get('nextPageToken')
            if not page_token:
                break
        if items:
            return items
        # フォールバック: runQuery（nameでソート）
        try:
            url = f"https://firestore.googleapis.com/v1/projects/{sa.get('project_id')}/databases/(default)/documents:runQuery"
            body = {
                "structuredQuery": {
                    "select": {"fields": [
                        {"fieldPath": "name"},
                        {"fieldPath": "customer_code"},
                        {"fieldPath": "accounting_app"},
                        {"fieldPath": "external_company_id"},
                        {"fieldPath": "contract_ok"},
                        {"fieldPath": "notion_page_id"}
                    ]},
                    "from": [{"collectionId": clients_collection_name()}],
                    "orderBy": [{"field": {"fieldPath": "name"}}],
                    "limit": 2000
                }
            }
            resp = _rq.post(url, headers={"Authorization": f"Bearer {token}"}, json=body, timeout=20)
            resp.raise_for_status()
            items_q = []
            payload = resp.json()
            for line in payload:
                doc = (line.get('document') or {})
                if not doc:
                    continue
                fields = doc.get('fields', {})
                def _sv(key):
                    v = fields.get(key)
                    if not v:
                        return ''
                    return v.get('stringValue') or v.get('integerValue') or v.get('booleanValue') or ''
                def _bool_any(key):
                    v = fields.get(key)
                    if not v:
                        return None
                    if 'booleanValue' in v:
                        return bool(v.get('booleanValue'))
                    if 'integerValue' in v:
                        try:
                            return int(v.get('integerValue')) == 1
                        except Exception:
                            return None
                    if 'stringValue' in v:
                        return str(v.get('stringValue','')).strip().lower() in ('true','1','yes','ok')
                    return None
                items_q.append({
                    'id': doc.get('name', '').split('/')[-1],
                    'name': _sv('name'),
                    'customer_code': _sv('customer_code'),
                    'accounting_app': _sv('accounting_app'),
                    'external_company_id': _sv('external_company_id'),
                    'contract_ok': _bool_any('contract_ok'),
                    'notion_page_id': _sv('notion_page_id'),
                })
            return items_q
        except Exception:
            return []

    try:
        raw = _rest_fetch()
        if not raw and get_db() is not None:
            # フォールバック: gRPC
            try:
                clients_ref = (
                    get_db()
                    .collection(clients_collection_name())
                    .select(['name', 'customer_code', 'accounting_app', 'external_company_id', 'contract_ok', 'notion_page_id'])
                    .order_by('name')
                    .stream()
                )
            except Exception:
                clients_ref = get_db().collection(clients_collection_name()).order_by('name').stream()
            for doc in clients_ref:
                data = doc.to_dict()
                data['id'] = doc.id
                raw.append(data)
    except Exception:
        raw = []
    def _norm_name(s: str) -> str:
        return (s or '').strip().lower()
    def _ts(d: dict):
        v = d.get('updated_at') or d.get('created_at') or 0
        try:
            # Firestore Timestamp 互換
            if hasattr(v, 'timestamp'):
                return float(v.timestamp())
            return float(v)
        except Exception:
            return 0.0
    uniq = {}
    for c in raw:
        key = c.get('notion_page_id') or c.get('customer_code') or _norm_name(c.get('name',''))
        if not key:
            key = c.get('id')
        if key in uniq:
            # 新しい方を採用
            if _ts(c) >= _ts(uniq[key]):
                uniq[key] = c
        else:
            uniq[key] = c
    result = list(uniq.values())
    # ロード完了フラグをON（UIの"読み込み中..."を消す）
    st.session_state['clients_loading'] = False
    return result

def refresh_clients_cache(background: bool = True):
    """顧問先キャッシュを更新。既定はバックグラウンドで非ブロッキング。"""
    def _do_load():
        try:
            data = _load_clients_from_db()
        except Exception:
            data = []
        st.session_state['clients_cache'] = data
        st.session_state['clients_cache_time'] = time.time()
        st.session_state['clients_loading'] = False
        st.session_state['clients_loading_started_at'] = 0.0

    if background:
        if not st.session_state.get('clients_loading', False):
            st.session_state['clients_loading'] = True
            st.session_state['clients_loading_started_at'] = time.time()
            threading.Thread(target=_do_load, daemon=True).start()
    else:
        _do_load()

def _load_with_timeout(timeout_sec: float = 10.0):
    # 互換のため残すが、基本使わない
    result_holder = {'data': None}
    def _worker():
        try:
            result_holder['data'] = _load_clients_from_db()
        except Exception:
            result_holder['data'] = None
    th = threading.Thread(target=_worker, daemon=True)
    th.start()
    th.join(timeout=timeout_sec)
    if th.is_alive():
        return None
    return result_holder['data']

def get_all_clients_raw():
    """Firestoreから顧問先一覧（フィルタなし）を取得（5分キャッシュ）"""
    cache = st.session_state.get('clients_cache')
    ts = st.session_state.get('clients_cache_time', 0)
    if cache is not None and (time.time() - ts) < 300:
        return cache
    # まず短いタイムアウトで同期取得を試みる（2秒）
    data = _load_with_timeout(2.0)
    if data is not None:
        st.session_state['clients_cache'] = data
        st.session_state['clients_cache_time'] = time.time()
        return data
    # だめならバックグラウンド更新を起動して即返す
    refresh_clients_cache(background=True)
    return cache or []

def get_clients():
    """有効な顧問先のみを取得（契約区分フィルタ適用）"""
    def _is_ok(val):
        # True/true/1/yes をOKとして扱う（既存データの型ゆらぎ吸収）
        if val is True:
            return True
        if isinstance(val, bool):
            return val
        if isinstance(val, (int, float)):
            return int(val) == 1
        if isinstance(val, str):
            return val.strip().lower() in ('true', '1', 'yes', 'ok')
        return False
    all_clients = get_all_clients_raw()
    ok_clients = [c for c in all_clients if _is_ok(c.get('contract_ok'))]
    # 0件の場合は、データが入っているかの確認のため一時的に全件を返す（ユーザーに案内表示）
    if len(all_clients) > 0 and len(ok_clients) == 0:
        st.caption('契約区分OKが0件のため一時的に全件表示中（設定で固定可能）。')
        return all_clients
    return ok_clients

# --- 診断用: Firestore RESTでclientsを直接取得（listDocumentsページング） ---
def fetch_clients_via_rest() -> list:
    try:
        import json as _json
        import requests as _rq
        from google.oauth2 import service_account as _sa
        from google.auth.transport.requests import Request as _GARequest
        sa = _json.loads(st.secrets.get('FIREBASE_SERVICE_ACCOUNT_JSON', '{}'))
        if not sa:
            return []
        creds = _sa.Credentials.from_service_account_info(sa, scopes=['https://www.googleapis.com/auth/datastore'])
        creds.refresh(_GARequest())
        token = creds.token
        items = []
        page_token = None
        while True:
            params = {"pageSize": 1000}
            if page_token:
                params["pageToken"] = page_token
            r = _rq.get(_clients_rest_base(sa.get('project_id')), headers={"Authorization": f"Bearer {token}"}, params=params, timeout=20)
            r.raise_for_status()
            data = r.json()
            for doc in data.get('documents', []) or []:
                fields = doc.get('fields', {})
                def _sv(key):
                    v = fields.get(key)
                    if not v:
                        return ''
                    return v.get('stringValue') or v.get('integerValue') or v.get('booleanValue') or ''
                def _bool_any(key):
                    v = fields.get(key)
                    if not v:
                        return None
                    if 'booleanValue' in v:
                        return bool(v.get('booleanValue'))
                    if 'integerValue' in v:
                        try:
                            return int(v.get('integerValue')) == 1
                        except Exception:
                            return None
                    if 'stringValue' in v:
                        return str(v.get('stringValue','')).strip().lower() in ('true','1','yes','ok')
                    return None
                items.append({
                    'id': doc.get('name', '').split('/')[-1],
                    'name': _sv('name'),
                    'customer_code': _sv('customer_code'),
                    'accounting_app': _sv('accounting_app'),
                    'external_company_id': _sv('external_company_id'),
                    'contract_ok': _bool_any('contract_ok'),
                    'notion_page_id': _sv('notion_page_id'),
                })
            page_token = data.get('nextPageToken')
            if not page_token:
                break
        return items
    except Exception as e:
        try:
            status = getattr(getattr(e, 'response', None), 'status_code', None)
            if status == 429:
                st.warning('Firestore REST読取が429（クォータ超過）で失敗しました。時間を置いて再試行してください。')
            else:
                st.error(f'Firestore REST読取エラー: {e}')
        except Exception:
            pass
        return []

def sync_clients_from_notion(database_id: str) -> dict:
    """Notionの顧客マスタDBからclientsを同期。既存はname一致で更新/なければ作成。
    期待プロパティ例: Name(タイトル), AccountingApp(選択: 'freee'|'mf'|'csv' 等), CompanyId(数値/テキスト)
    """
    result = {'updated': 0, 'created': 0, 'skipped': 0}
    if not NOTION_AVAILABLE:
        st.error('notion-client が利用できません。requirementsを確認してください。')
        return result
    if get_db() is None:
        st.error('Firestore接続がありません。')
        return result
    token = st.secrets.get('NOTION_TOKEN', '')
    if not token:
        st.error('Streamlit Secrets に NOTION_TOKEN を設定してください。')
        return result
    try:
        # Notion API 2025-09-03: databaseは複数data sourceを持つ可能性あり
        notion = NotionClient(auth=token, notion_version='2025-09-03')  # type: ignore
        # 1) databaseメタからdata_sourcesを取得（Python SDKのrequestは位置引数）
        db_meta = notion.request(f'databases/{database_id}', 'GET')
        data_sources = db_meta.get('data_sources', []) if isinstance(db_meta, dict) else []
        pages = []
        # data_sources API（ページング対応）
        if data_sources:
            ds_id = data_sources[0].get('id')
            if ds_id:
                next_cursor = None
                while True:
                    body = {'page_size': 100}
                    if next_cursor:
                        body['start_cursor'] = next_cursor
                    resp = notion.request(f'data_sources/{ds_id}/query', 'POST', None, body)
                    if isinstance(resp, dict):
                        pages.extend(resp.get('results', []))
                        if resp.get('has_more') and resp.get('next_cursor'):
                            next_cursor = resp['next_cursor']
                            continue
                    break
        # フォールバック: databases API（ページング対応）
        if not pages:
            try:
                next_cursor = None
                while True:
                    body = {'page_size': 100}
                    if next_cursor:
                        body['start_cursor'] = next_cursor
                    legacy = notion.request(f'databases/{database_id}/query', 'POST', None, body)
                    if isinstance(legacy, dict):
                        pages.extend(legacy.get('results', []))
                        if legacy.get('has_more') and legacy.get('next_cursor'):
                            next_cursor = legacy['next_cursor']
                            continue
                    break
            except Exception:
                pages = []
        def _title(props: dict) -> str:
            # 優先: 顧客名, 次点: Name, それ以外のtitle型
            if '顧客名' in props and props['顧客名'].get('type') == 'title':
                return ''.join([t.get('plain_text', '') for t in props['顧客名'].get('title', [])]).strip()
            if 'Name' in props and props['Name'].get('type') == 'title':
                return ''.join([t.get('plain_text', '') for t in props['Name'].get('title', [])]).strip()
            for k, v in props.items():
                if v.get('type') == 'title':
                    return ''.join([t.get('plain_text', '') for t in v.get('title', [])]).strip()
            return ''

        def _acc_app(props: dict) -> str:
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
                        val = (v['select'].get('name') or '').lower()
                        if any(k in val for k in ['freee', 'mf', 'マネーフォワード', 'csv']):
                            return val
                    elif v.get('multi_select'):
                        arr = v['multi_select']
                        if arr:
                            val = (arr[0].get('name') or '').lower()
                            if any(k in val for k in ['freee', 'mf', 'マネーフォワード', 'csv']):
                                return val
            return ''

        def _contract_ok(props: dict) -> bool:
            # 契約区分に「会計」を含み、かつ「解約」「停止」を含まないもののみ採用
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

        def _company_id(props: dict) -> str:
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

        def _customer_code(props: dict) -> str:
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

        for p in pages:
            props = p.get('properties', {})
            name = _title(props)
            if not name:
                result['skipped'] += 1
                continue
            # 契約区分フラグ（保存はするが、表示では除外）
            contract_ok = _contract_ok(props)
            app_str = _acc_app(props)
            company_id = _company_id(props)
            customer_code = _customer_code(props)
            # 契約区分NGの新規は作成せずスキップ。既存のみ更新。
            existing_doc = None
            try:
                existing_list = list(get_db().collection(clients_collection_name()).where('name', '==', name.strip()).limit(1).stream())
                if existing_list:
                    existing_doc = existing_list[0]
            except Exception:
                existing_doc = None
            if not contract_ok and existing_doc is None:
                result['skipped'] += 1
                continue
            if existing_doc is not None:
                client = existing_doc.to_dict() or {}
                client['id'] = existing_doc.id
                created = False
            else:
                client, created = get_or_create_client_by_name(name)
                if not client:
                    result['skipped'] += 1
                    continue
            updates = {
                'accounting_app': app_str,
                'external_company_id': company_id,
                'customer_code': customer_code,
                'contract_ok': contract_ok,
                'notion_page_id': p.get('id', ''),
                'updated_at': datetime.now()
            }
            get_db().collection(clients_collection_name()).document(client['id']).set({**client, **updates}, merge=True)
            if created:
                result['created'] += 1
            else:
                result['updated'] += 1
        return result
    except Exception as e:
        st.error(f'Notion同期に失敗しました: {e}')
        return result

def start_notion_sync_bg(database_id: str):
    """Notion同期をバックグラウンドで開始する。進捗は session_state['notion_sync'] に格納。"""
    if not database_id:
        st.warning('Notion Database IDを入力してください')
        return
    state = st.session_state.setdefault('notion_sync', {})
    if state.get('running'):
        return
    state.clear()
    state.update({
        'running': True,
        'phase': 'starting',
        'fetched': 0,
        'processed': 0,
        'created': 0,
        'updated': 0,
        'skipped': 0,
        'error': '',
        'cancel': False,
        'result': None,
        'started_at': time.time(),
    })

    def _runner():
        try:
            token = st.secrets.get('NOTION_TOKEN', '')
            if not token:
                raise RuntimeError('Streamlit Secrets に NOTION_TOKEN を設定してください。')
            if get_db() is None:
                raise RuntimeError('Firestore接続がありません。')

            # Notion HTTPクライアント（requestsでタイムアウト制御）
            import requests as _rq
            sess = _rq.Session()
            base = 'https://api.notion.com/v1'
            headers = {
                'Authorization': f'Bearer {token}',
                'Notion-Version': '2025-09-03',
                'Content-Type': 'application/json',
            }

            def api_get(path: str):
                # タイムアウト延長＋簡易リトライ（指数バックオフ）
                for attempt in range(5):
                    try:
                        resp = sess.get(f"{base}/{path}", headers=headers, timeout=30)
                        resp.raise_for_status()
                        return resp.json()
                    except Exception:
                        time.sleep(min(2 ** attempt, 8))
                        if attempt == 4:
                            raise

            def api_post(path: str, body: dict):
                # タイムアウト延長＋簡易リトライ（指数バックオフ）
                for attempt in range(5):
                    try:
                        resp = sess.post(f"{base}/{path}", headers=headers, json=body, timeout=45)
                        resp.raise_for_status()
                        return resp.json()
                    except Exception:
                        time.sleep(min(2 ** attempt, 8))
                        if attempt == 4:
                            raise

            # DBメタから data_source を取得
            state['phase'] = 'fetching'
            db_meta = api_get(f'databases/{database_id}')
            data_sources = db_meta.get('data_sources', []) if isinstance(db_meta, dict) else []

            def _iter_pages():
                pages_local = []
                if data_sources:
                    ds_id = data_sources[0].get('id')
                    if ds_id:
                        next_cursor = None
                        while True:
                            if state.get('cancel'):
                                break
                            body = {'page_size': 100}
                            if next_cursor:
                                body['start_cursor'] = next_cursor
                            resp = api_post(f'data_sources/{ds_id}/query', body)
                            if isinstance(resp, dict):
                                results = resp.get('results', [])
                                for r in results:
                                    yield r
                                state['fetched'] += len(results)
                                if resp.get('has_more') and resp.get('next_cursor'):
                                    next_cursor = resp['next_cursor']
                                    continue
                            break
                # フォールバック
                if state['fetched'] == 0:
                    next_cursor = None
                    while True:
                        if state.get('cancel'):
                            break
                        body = {'page_size': 100}
                        if next_cursor:
                            body['start_cursor'] = next_cursor
                        legacy = api_post(f'databases/{database_id}/query', body)
                        if isinstance(legacy, dict):
                            results = legacy.get('results', [])
                            for r in results:
                                yield r
                            state['fetched'] += len(results)
                            if legacy.get('has_more') and legacy.get('next_cursor'):
                                next_cursor = legacy['next_cursor']
                                continue
                        break

            # 既存顧問先の name/notion_page_id -> id マップ（REST優先で重複防止）
            existing_by_name = {}
            existing_by_notion = {}
            try:
                # REST: listDocumentsで全件取得
                import json as _json
                from google.oauth2 import service_account as _sa
                from google.auth.transport.requests import Request as _GARequest
                import requests as _rq
                sa_raw = st.secrets.get('FIREBASE_SERVICE_ACCOUNT_JSON', '{}')
                sa = _json.loads(sa_raw)
                creds = _sa.Credentials.from_service_account_info(sa, scopes=['https://www.googleapis.com/auth/datastore'])
                creds.refresh(_GARequest())
                token_fs = creds.token
                page_token = None
                while True:
                    params = {"pageSize": 1000}
                    if page_token:
                        params["pageToken"] = page_token
                    rr = _rq.get(_clients_rest_base(sa.get('project_id')), headers={"Authorization": f"Bearer {token_fs}"}, params=params, timeout=20)
                    rr.raise_for_status()
                    dj = rr.json() or {}
                    for doc in dj.get('documents', []) or []:
                        fields = doc.get('fields') or {}
                        doc_id = doc.get('name', '').split('/')[-1]
                        name_val = ((fields.get('name') or {}).get('stringValue') or '').strip()
                        notion_val = ((fields.get('notion_page_id') or {}).get('stringValue') or '').strip()
                        if name_val:
                            existing_by_name[name_val] = doc_id
                        if notion_val:
                            existing_by_notion[notion_val] = doc_id
                    page_token = dj.get('nextPageToken')
                    if not page_token:
                        break
            except Exception:
                # フォールバック: Admin SDK（最小限）
                try:
                    cur = (
                        get_db()
                        .collection(clients_collection_name())
                        .select(['name', 'notion_page_id'])
                        .stream()
                    )
                    for d in cur:
                        data = d.to_dict() or {}
                        name = (data.get('name') or '').strip()
                        npid = (data.get('notion_page_id') or '').strip()
                        if name:
                            existing_by_name[name] = d.id
                        if npid:
                            existing_by_notion[npid] = d.id
                except Exception:
                    pass

            # Notionプロパティ抽出ヘルパ
            def _title(props: dict) -> str:
                if '顧客名' in props and props['顧客名'].get('type') == 'title':
                    return ''.join([t.get('plain_text', '') for t in props['顧客名'].get('title', [])]).strip()
                if 'Name' in props and props['Name'].get('type') == 'title':
                    return ''.join([t.get('plain_text', '') for t in props['Name'].get('title', [])]).strip()
                for k, v in props.items():
                    if v.get('type') == 'title':
                        return ''.join([t.get('plain_text', '') for t in v.get('title', [])]).strip()
                return ''

            def _acc_app(props: dict) -> str:
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

            def _contract_ok(props: dict) -> bool:
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

            def _company_id(props: dict) -> str:
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

            def _customer_code(props: dict) -> str:
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

            # Firestoreバッチ（429対策のため軽いスロットリング＋指数バックオフ）
            state['phase'] = 'writing'
            batch = get_db().batch()
            batch_count = 0
            BATCH_LIMIT = 100

            def _commit_batch():
                nonlocal batch, batch_count
                if batch_count == 0:
                    return
                try:
                    batch.commit()
                except Exception as _ce:
                    # レート制限などに対して指数バックオフ
                    msg = str(_ce)
                    wait = 1.0
                    for _ in range(5):
                        if state.get('cancel'):
                            break
                        time.sleep(wait)
                        try:
                            batch.commit()
                            break
                        except Exception:
                            wait *= 2
                            continue
                # 小休止でスロットリング
                time.sleep(0.2)
                batch = get_db().batch()
                batch_count = 0

            for p in _iter_pages():
                if state.get('cancel'):
                    break
                props = p.get('properties', {})
                name = _title(props)
                if not name:
                    state['skipped'] += 1
                    continue
                updates = {
                    'accounting_app': _acc_app(props),
                    # v2では外部IDは保存しない（運用未決のため）
                    'customer_code': _customer_code(props),
                    'contract_ok': _contract_ok(props),
                    'updated_at': datetime.now(),
                }
                # 決定的ID: notion_page_id があればそれをdoc_idに採用
                target_doc_ref = None
                npid = updates.get('notion_page_id', '')
                if npid and npid in existing_by_notion:
                    target_doc_ref = get_db().collection(clients_collection_name()).document(existing_by_notion[npid])
                    state['updated'] += 1
                elif npid:
                    # まだ存在しない -> 決定的IDで新規作成
                    target_doc_ref = get_db().collection(clients_collection_name()).document(npid)
                    existing_by_notion[npid] = npid
                    updates['name'] = name
                    updates['created_at'] = datetime.now()
                    state['created'] += 1
                elif name in existing_by_name:
                    target_doc_ref = get_db().collection(clients_collection_name()).document(existing_by_name[name])
                    state['updated'] += 1
                else:
                    # 最後の手段: ランダムID（同名重複の増殖を避けるため、nameマップへ登録）
                    target_doc_ref = get_db().collection(clients_collection_name()).document()
                    existing_by_name[name] = target_doc_ref.id
                    updates['name'] = name
                    updates['created_at'] = datetime.now()
                    state['created'] += 1
                batch.set(target_doc_ref, updates, merge=True)
                batch_count += 1
                state['processed'] += 1
                if batch_count >= BATCH_LIMIT:
                    _commit_batch()

            _commit_batch()
            state['result'] = {
                'updated': state['updated'],
                'created': state['created'],
                'skipped': state['skipped'],
            }
        except Exception as e:  # noqa: BLE001
            state['error'] = str(e)
        finally:
            state['running'] = False

    threading.Thread(target=_runner, daemon=True).start()

def get_or_create_client_by_name(name: str):
    """名称で顧問先を検索し、なければ作成して返す。戻り値:(client_dict, created_bool)"""
    if get_db() is None or not name:
        return None, False
    try:
        existing = list(get_db().collection(clients_collection_name()).where('name', '==', name.strip()).limit(1).stream())
        if existing:
            doc = existing[0]
            data = doc.to_dict()
            data['id'] = doc.id
            return data, False
        # なければ作成
        now = datetime.now()
        doc_ref = get_db().collection(clients_collection_name()).add({
            'name': name.strip(),
            'special_prompt': '',
            'created_at': now,
            'updated_at': now
        })
        return {
            'id': doc_ref[1].id,
            'name': name.strip(),
            'special_prompt': '',
            'created_at': now,
            'updated_at': now
        }, True
    except Exception:
        # Admin SDK失敗時のRESTフォールバック
        try:
            import requests as _rq
            sa, token, project_id = _get_sa_and_token_for_firestore()
            if not (token and project_id):
                return None, False
            # まず名称一致で検索（runQuery）
            body = {
                "structuredQuery": {
                    "from": [{"collectionId": clients_collection_name()}],
                    "where": {
                        "fieldFilter": {
                            "field": {"fieldPath": "name"},
                            "op": "EQUAL",
                            "value": {"stringValue": name.strip()}
                        }
                    },
                    "limit": 1
                }
            }
            url = f"https://firestore.googleapis.com/v1/projects/{project_id}/databases/(default)/documents:runQuery"
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            r = _rq.post(url, headers=headers, json=body, timeout=15)
            if r.status_code == 200:
                for line in r.json():
                    doc = (line.get('document') or {})
                    if doc:
                        fields = doc.get('fields') or {}
                        path = doc.get('name', '')
                        doc_id = path.split('/')[-1] if path else ''
                        return {
                            'id': doc_id,
                            'name': (fields.get('name') or {}).get('stringValue', name.strip()),
                            'special_prompt': (fields.get('special_prompt') or {}).get('stringValue', ''),
                        }, False
            # 見つからなければ作成
            def _sv(v: str):
                return {"stringValue": v}
            def _ts():
                return {"timestampValue": datetime.utcnow().isoformat(timespec='seconds') + 'Z'}
            create_body = {
                "fields": {
                    "name": _sv(name.strip()),
                    "special_prompt": _sv(''),
                    "created_at": _ts(),
                    "updated_at": _ts(),
                }
            }
            url_create = _clients_rest_base(project_id)
            rc = _rq.post(url_create, headers=headers, json=create_body, timeout=15)
            if rc.status_code in (200, 201):
                doc = rc.json() or {}
                path = doc.get('name', '')
                doc_id = path.split('/')[-1] if path else ''
                return {
                    'id': doc_id,
                    'name': name.strip(),
                    'special_prompt': '',
                }, True
            return None, False
        except Exception:
            return None, False

def get_client_special_prompt(client_id: str) -> str:
    if get_db() is None or not client_id:
        return ''
    try:
        # まず Notion優先で取得（別DBのページ本文）。10分キャッシュ。
        cache_key = f"client_sp_prompt_{client_id}"
        cache_ts = f"client_sp_prompt_ts_{client_id}"
        ts = st.session_state.get(cache_ts, 0)
        if cache_key in st.session_state and (time.time() - ts) < 600:
            return st.session_state.get(cache_key, '')
        sp = ''
        notion_page_id = ''
        # Admin SDKでの取得を試行
        try:
            doc = get_db().collection(clients_collection_name()).document(client_id).get()
            if doc.exists:
                data = doc.to_dict() or {}
                notion_page_id = data.get('notion_page_id', '')
                sp = data.get('special_prompt', '') or ''
        except Exception:
            pass
        # キャッシュからのフォールバック
        if not notion_page_id:
            clients_cache = st.session_state.get('clients_cache') or []
            for c in clients_cache:
                if str(c.get('id')) == str(client_id):
                    notion_page_id = c.get('notion_page_id', '') or notion_page_id
                    break
        # RESTフォールバック（ドキュメント取得）
        if not notion_page_id:
            sa, token, project_id = _get_sa_and_token_for_firestore()
            if token and project_id:
                import requests as _rq
                url = f"{_clients_rest_base(project_id)}/{client_id}"
                headers = {"Authorization": f"Bearer {token}"}
                rr = _rq.get(url, headers=headers, timeout=10)
                if rr.status_code == 200:
                    dj = rr.json() or {}
                    fields = dj.get('fields') or {}
                    if 'notion_page_id' in fields and fields['notion_page_id'].get('stringValue'):
                        notion_page_id = fields['notion_page_id']['stringValue']
                    if not sp and 'special_prompt' in fields and fields['special_prompt'].get('stringValue'):
                        sp = fields['special_prompt']['stringValue']
        # Notionから取得
        token = st.secrets.get('NOTION_TOKEN', '')
        prompt_db_id = st.secrets.get('NOTION_PROMPT_DATABASE_ID', '27d4c173d9f780efbef4e8cc0cde0965')
        if token and notion_page_id:
            # REST経由で確実に取得
            sp_rest = _fetch_notion_page_text_by_relation(notion_page_id, prompt_db_id, token)
            if sp_rest:
                sp = sp_rest
        st.session_state[cache_key] = sp
        st.session_state[cache_ts] = time.time()
        return sp
    except Exception:
        return ''

def set_client_special_prompt(client_id: str, text: str) -> bool:
    if get_db() is None or not client_id:
        return False
    try:
        # Notion運用に切り替えたため、アプリ側からの編集は保存しない
        return False
    except Exception:
        return False

def _get_sa_and_token_for_firestore():
    """Firestore REST用のサービスアカウントとアクセストークン、プロジェクトIDを取得"""
    try:
        import json as _json
        from google.oauth2 import service_account as _sa
        from google.auth.transport.requests import Request as _GARequest
        sa_str = st.secrets.get('FIREBASE_SERVICE_ACCOUNT_JSON', '')
        if not sa_str:
            return None, None, None
        sa = _json.loads(sa_str)
        creds = _sa.Credentials.from_service_account_info(sa, scopes=['https://www.googleapis.com/auth/datastore'])
        creds.refresh(_GARequest())
        token = str(creds.token)
        project_id = sa.get('project_id')
        return sa, token, project_id
    except Exception:
        return None, None, None

def _firestore_rest_add_learning_entry(project_id: str, token: str, client_id: str, entry: dict) -> bool:
    """Firestore RESTで clients/{client_id}/learning_entries に1件追加する"""
    if not (project_id and token and client_id and entry):
        return False
    try:
        import requests as _rq
        def _sv(v: str):
            return {"stringValue": str(v) if v is not None else ""}
        def _ts(dt):
            try:
                return {"timestampValue": dt.utcnow().isoformat(timespec='seconds') + 'Z'}
            except Exception:
                from datetime import datetime as _dt
                return {"timestampValue": _dt.utcnow().isoformat(timespec='seconds') + 'Z'}
        def _map_str(d: dict):
            return {
                "mapValue": {
                    "fields": {k: _sv(v) for k, v in (d or {}).items()}
                }
            }
        body = {
            "fields": {
                "original_text": _sv(entry.get('original_text', '')),
                "ai_journal": _sv(entry.get('ai_journal', '')),
                "corrected_journal": _sv(entry.get('corrected_journal', '')),
                "comments": _sv(entry.get('comments', '')),
                "fields": _map_str(entry.get('fields', {})),
                "timestamp": _ts(datetime)
            }
        }
        url = f"https://firestore.googleapis.com/v1/projects/{project_id}/databases/(default)/documents/clients/{client_id}/learning_entries"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        r = _rq.post(url, headers=headers, json=body, timeout=20)
        if r.status_code in (200, 201):
            return True
        # 429/5xxはリトライ対象とするためFalse
        return False
    except Exception:
        return False

def add_learning_entries_from_csv(client_id: str, csv_bytes: bytes) -> dict:
    """顧問先別の学習エントリをCSVから取り込み保存。簡易正規化を行う。
    期待カラム例: original_text, ai_journal, corrected_journal, comments, company, date, amount, tax, description, account
    """
    result = {'saved': 0, 'skipped': 0}
    if get_db() is None or not client_id or not csv_bytes:
        return result
    try:
        import pandas as pd
        import io as _io
        df = pd.read_csv(_io.BytesIO(csv_bytes))
        # カラム名を小文字化
        df.columns = [str(c).strip().lower() for c in df.columns]
        # 正規化: 必須近似フィールド
        # RESTフォールバック用の遅延初期化コンテキスト
        rest_ctx = {"token": None, "project_id": None}
        for _, row in df.iterrows():
            original_text = str(row.get('original_text', '')).strip()
            ai_journal = str(row.get('ai_journal', '')).strip()
            corrected_journal = str(row.get('corrected_journal', '')).strip()
            comments = str(row.get('comments', '')).strip()
            # 補助: なければdescription等から原文を組み立て
            if not original_text:
                parts = []
                for k in ['company', 'date', 'amount', 'tax', 'description', 'account']:
                    v = row.get(k)
                    if pd.notna(v) and str(v).strip() != '':
                        parts.append(f"{k}:{str(v).strip()}")
                original_text = ' '.join(parts)
            # 必須：少なくともどちらかは欲しい
            if not original_text and not corrected_journal and not ai_journal:
                result['skipped'] += 1
                continue
            entry = {
                'original_text': original_text,
                'ai_journal': ai_journal,
                'corrected_journal': corrected_journal or ai_journal,
                'comments': comments,
                'fields': {
                    'company': str(row.get('company', '')).strip(),
                    'date': str(row.get('date', '')).strip(),
                    'amount': str(row.get('amount', '')).strip(),
                    'tax': str(row.get('tax', '')).strip(),
                    'description': str(row.get('description', '')).strip(),
                    'account': str(row.get('account', '')).strip(),
                },
                'timestamp': datetime.now()
            }
            # Admin SDKでの保存（指数バックオフ）
            save_ok = False
            wait = 0.2
            for attempt in range(5):
                try:
                    get_db().collection('clients').document(client_id).collection('learning_entries').add(entry)
                    save_ok = True
                    break
                except Exception as _e:
                    # 429や一時的な失敗を想定して指数バックオフ
                    time.sleep(wait)
                    wait = min(wait * 2, 2.0)
            # RESTフォールバック
            if not save_ok:
                if not (rest_ctx["token"] and rest_ctx["project_id"]):
                    _, token, project_id = _get_sa_and_token_for_firestore()
                    rest_ctx["token"], rest_ctx["project_id"] = token, project_id
                if rest_ctx["token"] and rest_ctx["project_id"]:
                    wait_rest = 0.5
                    for attempt in range(5):
                        ok = _firestore_rest_add_learning_entry(rest_ctx["project_id"], rest_ctx["token"], client_id, entry)
                        if ok:
                            save_ok = True
                            break
                        time.sleep(wait_rest)
                        wait_rest = min(wait_rest * 2, 4.0)
            if save_ok:
                result['saved'] += 1
                # 連続書き込みのスロットリング
                time.sleep(0.12)
            else:
                result['skipped'] += 1
        # 取り込み後はクライアント別ベクトルキャッシュをクリア
        cache_key = f"learning_data_cache_{client_id}"
        cache_ts_key = f"learning_data_timestamp_{client_id}"
        if cache_key in st.session_state:
            del st.session_state[cache_key]
        if cache_ts_key in st.session_state:
            del st.session_state[cache_ts_key]
        return result
    except Exception as e:
        st.error(f"CSV取り込みに失敗しました: {e}")
        return result

def _extract_text_from_block(block: dict) -> str:
    """Notionのブロックからテキストを抽出（主要テキスト系＋画像キャプション）。"""
    try:
        t = block.get('type')
        if not t:
            return ''
        # リッチテキストをもつ代表的ブロック
        rich = []
        if t in ('paragraph', 'heading_1', 'heading_2', 'heading_3', 'quote', 'callout', 'bulleted_list_item', 'numbered_list_item', 'to_do', 'toggle', 'code'):
            rt = block.get(t, {}).get('rich_text', [])
            for r in rt:
                rich.append(r.get('plain_text', ''))
            return ''.join(rich).strip()
        if t == 'image':
            cap = block.get('image', {}).get('caption', [])
            for r in cap:
                rich.append(r.get('plain_text', ''))
            return ''.join(rich).strip()
        return ''
    except Exception:
        return ''

def _fetch_notion_page_text_by_relation(notion_page_id: str, prompt_db_id: str, token: str) -> str:
    """Relation(顧客マスタ)で紐づくプロンプトDBのページ本文をRESTで取得しテキスト化。"""
    if not (notion_page_id and prompt_db_id and token):
        return ''
    import requests as _rq
    base = 'https://api.notion.com/v1'
    headers = {
        'Authorization': f'Bearer {token}',
        'Notion-Version': '2025-09-03',
        'Content-Type': 'application/json',
    }
    # 1) プロンプトDBをRelation(顧客マスタ) contains で検索
    body = {
        'filter': {
            'property': '顧客マスタ',
            'relation': {'contains': notion_page_id}
        },
        'page_size': 1
    }
    try:
        r = _rq.post(f'{base}/databases/{prompt_db_id}/query', headers=headers, json=body, timeout=20)
        r.raise_for_status()
        results = r.json().get('results', [])
        if not results:
            return ''
        page_id = results[0]['id']
        # 2) ページ本文のブロックを取得（ページング）
        texts = []
        next_cursor = None
        while True:
            params = {'page_size': 100}
            if next_cursor:
                params['start_cursor'] = next_cursor
            r2 = _rq.get(f'{base}/blocks/{page_id}/children', headers=headers, params=params, timeout=20)
            r2.raise_for_status()
            data = r2.json()
            for blk in data.get('results', []):
                t = _extract_text_from_block(blk)
                if t:
                    texts.append(t)
            if data.get('has_more') and data.get('next_cursor'):
                next_cursor = data['next_cursor']
                continue
            break
        return '\n'.join(texts).strip()
    except Exception:
        return ''

def get_all_client_learning_entries(client_id: str):
    """顧問先別の学習データを全件取得"""
    if get_db() is None or not client_id:
        return []
    try:
        ref = get_db().collection(clients_collection_name()).document(client_id).collection('learning_entries').stream()
        entries = []
        for doc in ref:
            data = doc.to_dict()
            data['doc_id'] = doc.id
            # 既存のレビュー型と互換のキーへ写像
            mapped = {
                'original_text': data.get('original_text', ''),
                'ai_journal': data.get('ai_journal', ''),
                'corrected_journal': data.get('corrected_journal', ''),
                'comments': data.get('comments', ''),
                'doc_id': data['doc_id']
            }
            entries.append(mapped)
        return entries
    except Exception:
        return []

# ベクトル検索機能の実装
def initialize_vector_model():
    """ベクトル検索用のモデルを初期化（遅延import）"""
    try:
        from sentence_transformers import SentenceTransformer  # 遅延ロード
    except Exception:
        return None
    try:
        model = SentenceTransformer('sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2')
        return model
    except Exception:
        return None

def create_text_embeddings(texts, model):
    """テキストの埋め込みベクトルを生成"""
    if model is None:
        return None
    try:
        embeddings = model.encode(texts, show_progress_bar=False)
        return embeddings
    except Exception:
        return None

def build_vector_index(reviews, model):
    """レビューデータからベクトルインデックスを構築"""
    if model is None:
        return None
    try:
        import faiss  # 遅延ロード
        # レビューテキストを準備
        texts = []
        for review in reviews:
            text_parts = []
            if review.get('original_text'):
                text_parts.append(review['original_text'])
            if review.get('ai_journal'):
                text_parts.append(review['ai_journal'])
            if review.get('corrected_journal'):
                text_parts.append(review['corrected_journal'])
            if review.get('comments'):
                text_parts.append(review['comments'])
            combined_text = ' '.join(text_parts)
            texts.append(combined_text)
        if not texts:
            return None
        embeddings = create_text_embeddings(texts, model)
        if embeddings is None:
            return None
        dimension = embeddings.shape[1]
        index = faiss.IndexFlatIP(dimension)
        # 正規化
        faiss.normalize_L2(embeddings)
        index.add(embeddings)
        return {
            'index': index,
            'embeddings': embeddings,
            'reviews': reviews
        }
    except Exception:
        return None

def search_similar_reviews_vector(query_text, vector_index, model, top_k=5, similarity_threshold=0.3):
    """ベクトル検索による類似レビューの検索"""
    if model is None or vector_index is None:
        return []
    try:
        import numpy as np
        import faiss  # 遅延
        query_embedding = create_text_embeddings([query_text], model)
        if query_embedding is None:
            return []
        faiss.normalize_L2(query_embedding)
        D, I = vector_index['index'].search(query_embedding, min(top_k, len(vector_index['reviews'])))
        results = []
        for rank, idx in enumerate(I[0]):
            if idx < 0:
                continue
            review = vector_index['reviews'][idx]
            score = float(D[0][rank])
            if score >= similarity_threshold:
                results.append({'review': review, 'similarity': score, 'search_method': 'vector'})
        return results
    except Exception:
        return []

def hybrid_search_similar_reviews(text, reviews, vector_model=None, top_k=5):
    if not reviews:
        return []
    results = []
    try:
        # ベクトル
        if vector_model is None:
            vector_model = initialize_vector_model()
        vector_results = []
        if vector_model:
            vector_index = build_vector_index(reviews, vector_model)
            if vector_index:
                vector_results = search_similar_reviews_vector(text, vector_index, vector_model, top_k=top_k)
        # テキスト（既存ロジック）
        text_results = find_similar_reviews_advanced(text, reviews)
        results = vector_results + [{'review': r, 'similarity': 0.0, 'search_method': 'text'} for r in text_results]
        # 重複排除
        seen = set()
        unique = []
        for r in results:
            rid = r['review'].get('doc_id', '')
            if rid in seen:
                continue
            seen.add(rid)
            unique.append(r)
        return unique[:top_k]
    except Exception:
        return results

def generate_hybrid_learning_prompt(text, similar_reviews):
    """ハイブリッド検索結果から学習プロンプトを生成"""
    if not similar_reviews:
        return ""
    
    prompt_parts = []
    prompt_parts.append("\n【過去の修正例（参考）】")
    
    for i, result in enumerate(similar_reviews[:3]):  # 上位3件まで
        review = result['review']
        similarity = result['similarity']
        search_method = result.get('search_method', 'unknown')
        
        prompt_parts.append(f"\n{i+1}. 類似度: {similarity:.3f} ({search_method})")
        prompt_parts.append(f"元テキスト: {review.get('original_text', '')[:200]}...")
        prompt_parts.append(f"AI推測: {review.get('ai_journal', '')}")
        prompt_parts.append(f"修正後: {review.get('corrected_journal', '')}")
        if review.get('comments'):
            prompt_parts.append(f"修正理由: {review.get('comments', '')}")
    
    prompt_parts.append("\n上記の修正例を参考に、より適切な勘定科目を選択してください。")
    
    return '\n'.join(prompt_parts)

def get_vector_search_status():
    """ベクトル検索の利用可能性を確認"""
    if not VECTOR_SEARCH_AVAILABLE:
        return {
            'available': False,
            'message': 'ベクトル検索ライブラリがインストールされていません',
            'recommendation': 'sentence-transformers、scikit-learn、faiss-cpuをインストールしてください'
        }
    
    model = initialize_vector_model()
    if model is None:
        return {
            'available': False,
            'message': 'ベクトル検索モデルの初期化に失敗しました',
            'recommendation': 'モデルのダウンロードを確認してください'
        }
    
    return {
        'available': True,
        'message': 'ベクトル検索が利用可能です',
        'model': model
    }

# フォルダ準備
def ensure_dirs():
    os.makedirs('input', exist_ok=True)
    os.makedirs('output', exist_ok=True)

ensure_dirs()

# HEICファイルをJPEGに変換
# def convert_heic_to_jpeg(heic_path):
#     if not HEIC_SUPPORT:
#         st.error("HEICファイルを処理するにはpillow_heifライブラリが必要です。")
#         return None
#     try:
#         heif_file = pillow_heif.read_heif(heic_path)
#         image = Image.frombytes(
#             heif_file.mode, 
#             heif_file.size, 
#             heif_file.data,
#             "raw",
#             heif_file.mode,
#             heif_file.stride,
#         )
#         jpeg_path = heic_path.replace('.heic', '.jpg').replace('.HEIC', '.jpg')
#         image.save(jpeg_path, 'JPEG', quality=95)
#         return jpeg_path
#     except Exception as e:
#         st.error(f"HEICファイルの変換に失敗しました: {e}")
#         return None

# Google Cloud Vision APIでOCR
def ocr_image_gcv(image_path):
    client = vision.ImageAnnotatorClient()
    with open(image_path, "rb") as image_file:
        content = image_file.read()
    image = vision.Image(content=content)
    # type: ignore でlinterエラーを抑制
    response = client.text_detection(image=image)  # type: ignore
    texts = response.text_annotations
    if texts:
        return texts[0].description
    return ""

def ocr_image(image_path, mode='gcv'):
    """OCR処理の統一インターフェース"""
    if mode == 'gcv':
        return ocr_image_gcv(image_path)
    else:
        return ocr_image_gcv(image_path)  # デフォルトはGoogle Cloud Vision

# ChatGPT APIで勘定科目を推測
def guess_account_ai(text, stance='received', extra_prompt=''):
    """従来のAI推測（後方互換性のため残す）"""
    # 学習機能のON/OFFをチェック
    learning_enabled = st.session_state.get('learning_enabled', True)
    if learning_enabled:
        return guess_account_ai_with_learning(text, stance, extra_prompt)
    else:
        # 学習機能が無効の場合は従来の方法
        return guess_account_ai_basic(text, stance, extra_prompt)

def guess_account_ai_basic(text, stance='received', extra_prompt=''):
    """学習機能なしの基本的なAI推測"""
    if not OPENAI_API_KEY:
        st.warning("OpenAI APIキーが設定されていません。AI推測はスキップされます。")
        return None
    if stance == 'issued':
        stance_prompt = "あなたは請求書を発行した側（売上計上側）の経理担当者です。売上・収入に該当する勘定科目のみを選んでください。"
        account_list = "売上高、雑収入、受取手形、売掛金"
    else:
        stance_prompt = "あなたは請求書を受領した側（費用計上側）の経理担当者です。費用・仕入・販管費に該当する勘定科目のみを選んでください。"
        account_list = "研修費、教育研修費、旅費交通費、通信費、消耗品費、会議費、交際費、広告宣伝費、外注費、支払手数料、仮払金、修繕費、仕入高、減価償却費"
    prompt = (
        f"{stance_prompt}\n"
        "以下のテキストは領収書や請求書から抽出されたものです。\n"
        f"必ず下記の勘定科目リストから最も適切なものを1つだけ日本語で出力してください。\n"
        "\n【勘定科目リスト】\n{account_list}\n"
        "\n摘要や商品名・サービス名・講義名をそのまま勘定科目にしないでください。\n"
        "たとえば『SNS講義費』や『○○セミナー費』などは『研修費』や『教育研修費』に分類してください。\n"
        "分からない場合は必ず『仮払金』と出力してください。\n"
        "\n※『レターパック』『切手』『郵便』『ゆうパック』『ゆうメール』『ゆうパケット』『スマートレター』『ミニレター』など郵便・配送サービスに該当する場合は必ず『通信費』としてください。\n"
        "※『飲料』『食品』『お菓子』『ペットボトル』『弁当』『パン』『コーヒー』『お茶』『水』『ジュース』など飲食物や軽食・会議用の食べ物・飲み物が含まれる場合は、会議費または消耗品費を優先してください。\n"
        "\n【良い例】\n"
        "テキスト: SNS講義費 10,000円\n→ 勘定科目：研修費\n"
        "テキスト: レターパックプラス 1,200円\n→ 勘定科目：通信費\n"
        "テキスト: ペットボトル飲料・お菓子 2,000円\n→ 勘定科目：会議費\n"
        "テキスト: 食品・飲料・パン 1,500円\n→ 勘定科目：消耗品費\n"
        "\n【悪い例】\n"
        "テキスト: SNS講義費 10,000円\n→ 勘定科目：SNS講義費（×）\n"
        "テキスト: レターパックプラス 1,200円\n→ 勘定科目：広告宣伝費（×）\n"
        "テキスト: ペットボトル飲料・お菓子 2,000円\n→ 勘定科目：通信費（×）\n"
        "テキスト: 食品・飲料・パン 1,500円\n→ 勘定科目：通信費（×）\n"
        f"\n【テキスト】\n{text}\n\n勘定科目："
    ) + (f"\n【追加指示】\n{extra_prompt}" if extra_prompt else "")
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }
    data = {
        "model": "gpt-4.1-nano",
        "messages": [
            {"role": "system", "content": stance_prompt},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 20,
        "temperature": 0
    }
    try:
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json=data,
            timeout=10
        )
        response.raise_for_status()
        result = response.json()
        content = result["choices"][0]["message"]["content"].strip()
        account = content.split("\n")[0].replace("勘定科目：", "").strip()
        return account
    except Exception as e:
        st.warning(f"AIによる勘定科目推測でエラー: {e}")
        return None

# 摘要をAIで生成

def guess_description_ai(text, period_hint=None, extra_prompt=''):
    if not OPENAI_API_KEY:
        return ""
    period_instruction = ""
    if period_hint:
        period_instruction = f"\nこの請求書には『{period_hint}』という期間情報が記載されています。摘要には必ずこの情報を含めてください。"
    prompt = (
        "あなたは日本の会計実務に詳しい経理担当者です。\n"
        "以下のテキストは領収書や請求書から抽出されたものです。\n"
        "摘要欄には、何に使ったか・サービス名・講義名など、領収書から読み取れる具体的な用途や内容を20文字以内で簡潔に日本語で記載してください。\n"
        "金額や『消費税』などの単語だけを摘要にしないでください。\n"
        "また、『x月分』『上期分』『下期分』などの期間情報があれば必ず摘要に含めてください。"
        f"{period_instruction}"
        "\n【良い例】\n"
        "テキスト: 4月分PR報酬 交通費 1,000円 タクシー利用\n→ 摘要：4月分PR報酬 タクシー移動\n"
        "\n【悪い例】\n"
        "テキスト: 4月分PR報酬 交通費 1,000円 タクシー利用\n→ 摘要：1,000円（×）\n"
        f"\n【テキスト】\n{text}\n\n摘要："
    ) + (f"\n【追加指示】\n{extra_prompt}" if extra_prompt else "")
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }
    data = {
        "model": "gpt-4.1-nano",
        "messages": [
            {"role": "system", "content": "あなたは日本の会計仕訳に詳しい経理担当者です。摘要欄には用途や内容が分かる日本語を簡潔に記載してください。"},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 40,
        "temperature": 0
    }
    try:
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json=data,
            timeout=10
        )
        response.raise_for_status()
        result = response.json()
        content = result["choices"][0]["message"]["content"].strip()
        description = content.split("\n")[0].replace("摘要：", "").strip()
        return description
    except Exception:
        return ""

# 金額をAIで抽出

def guess_amount_ai(text):
    if not OPENAI_API_KEY:
        return None
    prompt = (
        "以下は日本の請求書や領収書から抽出したテキストです。"
        "この請求書の合計金額（支払金額、税込）を数字のみで出力してください。"
        "絶対に口座番号・登録番号・電話番号・振込先・連絡先・登録番号・TEL・No.などの数字や、10桁以上の数字、カンマ区切りでない長い数字は金額として出力しないでください。"
        "合計金額は『合計』『小計』『ご請求金額』『請求金額』『総額』『現金支払額』などのラベルの直後に記載されていることが多いです。"
        "金額のカンマやスペース、改行が混じっていても正しい合計金額（例：1,140円）を抽出してください。"
        "『お預り』『お預かり』『お釣り』『現金』などのラベルが付いた金額は絶対に選ばないでください。"
        "複数の金額がある場合は、合計・総額などのラベル付きで最も下にあるものを選んでください。"
        "分からない場合は空欄で出力してください。"
        "【良い例】\nテキスト: 合計 ¥1, 140\n→ 1140\nテキスト: 合計 18,000円 振込先: 2688210\n→ 18000\n【悪い例】\nテキスト: 合計 ¥1, 140\n→ 1（×）や140（×）\nテキスト: 合計 18,000円 振込先: 2688210\n→ 2688210（×）"
        "\n\nテキスト:\n{text}\n\n合計金額："
    )
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }
    data = {
        "model": "gpt-4.1-nano",
        "messages": [
            {"role": "system", "content": "あなたは日本の会計実務に詳しい経理担当者です。請求書や領収書から合計金額を正確に抽出してください。"},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 20,
        "temperature": 0
    }
    try:
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json=data,
            timeout=10
        )
        response.raise_for_status()
        result = response.json()
        content = result["choices"][0]["message"]["content"].strip()
        amount_str = content.split("\n")[0].replace("合計金額：", "").replace(",", "").replace(" ", "").strip()
        if amount_str.isdigit():
            return int(amount_str)
        return None
    except Exception as e:
        st.warning(f"AIによる金額抽出でエラー: {e}")
        return None

# 年度表記を除外する関数
def is_year_number(val, text):
    """年度表記（2025など）を除外する"""
    if val >= 2020 and val <= 2030:  # 年度の範囲
        # 年度表記のパターンをチェック
        year_patterns = [
            r'\d{4}年',
            r'\d{4}/',
            r'\d{4}-',
            r'年度',
            r'FY\d{4}',
            r'fiscal.*\d{4}'
        ]
        for pattern in year_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return True
    return False

def preprocess_receipt_text(text):
    # 全角→半角変換、余計な改行・スペース除去、金額区切り記号「.」→「,」
    import re
    text = unicodedata.normalize('NFKC', text)
    text = text.replace('\r', '')
    text = text.replace('.', ',')  # 金額区切り記号をカンマに統一
    # 金額部分のカンマ・スペース混在（例：¥1, 140）を¥1,140に正規化
    text = re.sub(r'¥([0-9]+),\s*([0-9]{3})', r'¥\1,\2', text)
    text = re.sub(r'¥([0-9]+)\s*,\s*([0-9]{3})', r'¥\1,\2', text)
    text = re.sub(r'¥([0-9]+)\s+([0-9]{3})', r'¥\1,\2', text)
    # 括弧内の外8%/外10%パターンを複数行にまたがっても1行に連結
    def merge_parentheses_lines(txt):
        lines = txt.split('\n')
        merged = []
        buf = []
        inside = False
        for line in lines:
            if re.match(r'^\(外\s*[810]{1,2}[%％]', line):
                inside = True
                buf.append(line)
            elif inside:
                buf.append(line)
                if ')' in line:
                    merged.append(' '.join(buf))
                    buf = []
                    inside = False
            else:
                merged.append(line)
        if buf:
            merged.append(' '.join(buf))
        return '\n'.join(merged)
    text = merge_parentheses_lines(text)
    text = '\n'.join([line.strip() for line in text.split('\n') if line.strip()])
    return text

# 金額・税率ごとの複数仕訳生成関数
def extract_multiple_entries(text, stance='received', tax_mode='自動判定', debug_mode=False, extra_prompt=''):
    """10%・8%混在レシートに対応した複数仕訳生成（堅牢な正規表現・税率ごとの内税/外税判定・バリデーション強化）"""
    text = preprocess_receipt_text(text)
    entries = []
    
    # --- デバッグ強化: 全行の内容とヒット状況を必ず表示（最初に実行） ---
    tax_blocks = []
    debug_lines = []
    lines = text.split('\n')
    for i, line in enumerate(lines):
        hit = []
        # 課税10%
        m10 = re.search(r'課税計\s*[\(（]10[%％][\)）]', line)
        if m10:
            hit.append('課税10%ラベル')
            # 次行に金額があれば抽出
            if i+1 < len(lines):
                mval = re.search(r'¥?([0-9,]+)', lines[i+1])
                if mval:
                    val = int(mval.group(1).replace(',', ''))
                    # 金額行に「¥」や「円」が含まれているかつ2円以上（0円・1円は除外）
                    if (('¥' in lines[i+1] or '円' in lines[i+1]) and val > 1):
                        tax_blocks.append(('外税10%', val, '課税仕入 10%', line + ' / ' + lines[i+1]))
                        hit.append(f'金額:{val}')
        # 課税8%
        m8 = re.search(r'課税計\s*[\(（]8[%％][\)）]', line)
        if m8:
            hit.append('課税8%ラベル')
            if i+1 < len(lines):
                mval = re.search(r'¥?([0-9,]+)', lines[i+1])
                if mval:
                    val = int(mval.group(1).replace(',', ''))
                    if (('¥' in lines[i+1] or '円' in lines[i+1]) and val > 1):
                        tax_blocks.append(('外税8%', val, '課税仕入 8%', line + ' / ' + lines[i+1]))
                        hit.append(f'金額:{val}')
        # 非課税
        mex = re.search(r'非課[税稅]計', line)
        if mex:
            hit.append('非課税ラベル')
            if i+1 < len(lines):
                mval = re.search(r'¥?([0-9,]+)', lines[i+1])
                if mval:
                    val = int(mval.group(1).replace(',', ''))
                    if (('¥' in lines[i+1] or '円' in lines[i+1]) and val > 1):
                        tax_blocks.append(('非課税', val, '非課税', line + ' / ' + lines[i+1]))
                        hit.append(f'金額:{val}')
        debug_lines.append(f'[{i}] {line} => {hit if hit else "ヒットなし"}')
    # デバッグ用: Streamlitで全行のヒット状況を必ず表示
    if debug_mode and 'st' in globals():
        st.info("[デバッグ] 各行の正規表現ヒット状況:\n" + '\n'.join(debug_lines))
        if tax_blocks:
            st.info(f"[デバッグ] 税区分・金額ペア抽出結果: {[(mode, val, label, l) for mode, val, label, l in tax_blocks]}")
        else:
            st.info("[デバッグ] 税区分・金額ペア抽出結果: なし")
    # --- ここまでデバッグ強化（最初に実行） ---
    
    # デバッグで抽出された税区分・金額ペアがあれば使用
    if tax_blocks:
        for mode, amount, tax_label, _ in tax_blocks:
            entry = extract_info_from_text(text, stance, mode, extra_prompt=extra_prompt)
            entry['amount'] = str(amount)
            if mode == '非課税':
                entry['tax'] = '0'
            entry['description'] = f"{entry['description']}（{tax_label}）"
            entries.append(entry)
        return entries
    
    # (外8% 対象 ¥962)や(外10% 対象 ¥420)のパターン抽出（複数行対応・findallで全て抽出）
    pattern_8 = re.compile(r'外\s*8[%％][^\d\n]*?対象[^\d\n]*?¥?([0-9,]+)', re.IGNORECASE | re.DOTALL)
    pattern_10 = re.compile(r'外\s*10[%％][^\d\n]*?対象[^\d\n]*?¥?([0-9,]+)', re.IGNORECASE | re.DOTALL)
    amounts_8 = [int(m.replace(',', '')) for m in pattern_8.findall(text) if m and int(m.replace(',', '')) > 10]
    amounts_10 = [int(m.replace(',', '')) for m in pattern_10.findall(text) if m and int(m.replace(',', '')) > 10]
    # 8%仕訳
    for amount_8 in amounts_8:
        entry_8 = extract_info_from_text(text, stance, '外税8%', extra_prompt=extra_prompt)
        entry_8['amount'] = str(amount_8)
        entry_8['tax'] = str(int(amount_8 * 0.08))
        entry_8['description'] = f"{entry_8['description']}（8%対象）"
        entries.append(entry_8)
    # 10%仕訳
    for amount_10 in amounts_10:
        entry_10 = extract_info_from_text(text, stance, '外税10%', extra_prompt=extra_prompt)
        entry_10['amount'] = str(amount_10)
        entry_10['tax'] = str(int(amount_10 * 0.1))
        entry_10['description'] = f"{entry_10['description']}（10%対象）"
        entries.append(entry_10)
    if entries:
        return entries
    # 複数行にまたがる「内8%」「内10%」の小計・税額抽出
    # 例：(内 8% タイショウ\n¥1,755)  (内 8%\n¥130)
    pattern_8 = re.compile(r'内\s*8[%％][^\d\n]*[\(（\[｢]?(?:タイショウ)?[\s　]*\n?¥?([0-9,]+)[\)）\]｣]?', re.IGNORECASE)
    pattern_8_tax = re.compile(r'内\s*8[%％][^\d\n]*\n?¥?([0-9,]+)[\)）\]｣]?', re.IGNORECASE)
    pattern_10 = re.compile(r'内\s*10[%％][^\d\n]*[\(（\[｢]?(?:タイショウ)?[\s　]*\n?¥?([0-9,]+)[\)）\]｣]?', re.IGNORECASE)
    pattern_10_tax = re.compile(r'内\s*10[%％][^\d\n]*\n?¥?([0-9,]+)[\)）\]｣]?', re.IGNORECASE)
    # 小計
    match_8 = pattern_8.search(text)
    match_10 = pattern_10.search(text)
    # 税額
    matches_8_tax = pattern_8_tax.findall(text)
    matches_10_tax = pattern_10_tax.findall(text)
    amount_8 = int(match_8.group(1).replace(',', '')) if match_8 and match_8.group(1) else None
    amount_10 = int(match_10.group(1).replace(',', '')) if match_10 and match_10.group(1) else None
    # 税額は2回目の出現を優先（1回目は小計、2回目は税額であることが多い）
    tax_8 = int(matches_8_tax[1].replace(',', '')) if len(matches_8_tax) > 1 else None
    tax_10 = int(matches_10_tax[1].replace(',', '')) if len(matches_10_tax) > 1 else None
    # 「内8%」「内10%」が出現した場合は内税として扱う
    mode_8 = '内税' if '内8%' in text or '内 8%' in text else '外税'
    mode_10 = '内税' if '内10%' in text or '内 10%' in text else '外税'
    # 8%仕訳
    if amount_8 and amount_8 > 10:
        entry_8 = extract_info_from_text(text, stance, f'{mode_8}8%', extra_prompt=extra_prompt)
        entry_8['amount'] = str(amount_8)
        entry_8['tax'] = str(tax_8 if tax_8 is not None else (amount_8 - int(round(amount_8 / 1.08)) if mode_8 == '内税' else int(amount_8 * 0.08)))
        entry_8['description'] = f"{entry_8['description']}（8%対象）"
        entries.append(entry_8)
    # 10%仕訳
    if amount_10 and amount_10 > 10:
        entry_10 = extract_info_from_text(text, stance, f'{mode_10}10%', extra_prompt=extra_prompt)
        entry_10['amount'] = str(amount_10)
        entry_10['tax'] = str(tax_10 if tax_10 is not None else (amount_10 - int(round(amount_10 / 1.1)) if mode_10 == '内税' else int(amount_10 * 0.1)))
        entry_10['description'] = f"{entry_10['description']}（10%対象）"
        entries.append(entry_10)
    if entries:
        return entries
    # 明細行ベースの混在判定（従来ロジック）
    # レシート下部の内8%・内10%金額・税額抽出
    # 例: 内8%（\708）(税額\52)  内10%（\130）(税額\12)
    bottom_8 = re.search(r'内[\s　]*8[%％][^\d]*(?:\\?([0-9,]+))[^\d]*(?:税額[\s　]*\\?([0-9,]+))?', text)
    bottom_10 = re.search(r'内[\s　]*10[%％][^\d]*(?:\\?([0-9,]+))[^\d]*(?:税額[\s　]*\\?([0-9,]+))?', text)
    amount_8 = int(bottom_8.group(1).replace(',', '')) if bottom_8 and bottom_8.group(1) else None
    tax_8 = int(bottom_8.group(2).replace(',', '')) if bottom_8 and bottom_8.group(2) else None
    amount_10 = int(bottom_10.group(1).replace(',', '')) if bottom_10 and bottom_10.group(1) else None
    tax_10 = int(bottom_10.group(2).replace(',', '')) if bottom_10 and bottom_10.group(2) else None
    # 内税/外税判定
    is_inclusive = bool(re.search(r'内税|税込|消費税込|tax in|tax-in|taxin', text.lower()))
    is_exclusive = bool(re.search(r'外税|別途消費税|tax out|tax-out|taxout', text.lower()))
    # 10%・8%混在の判定（明細行も含む）
    has_10_percent = re.search(r'10%|１０％|消費税.*10|税率.*10', text)
    has_8_percent = re.search(r'8%|８％|消費税.*8|税率.*8', text)
    # 明細行から金額・税率を抽出（従来ロジックも残す）
    lines = text.split('\n')
    item_amounts = []
    for line in lines:
        if re.search(r'([0-9,]+)円.*[0-9]+%|([0-9,]+)円.*８％|([0-9,]+)円.*10%', line):
            amount_match = re.search(r'([0-9,]+)円', line)
            if amount_match:
                amount = int(amount_match.group(1).replace(',', ''))
                if re.search(r'8%|８％', line):
                    tax_rate = 8
                elif re.search(r'10%|１０％', line):
                    tax_rate = 10
                else:
                    tax_rate = 10
                item_amounts.append({'amount': amount, 'tax_rate': tax_rate, 'line': line})
    # レシート下部の金額があれば優先
    if amount_8 or amount_10:
        if amount_10:
            entry_10 = extract_info_from_text(text, stance, '内税10%' if is_inclusive else '外税10%', extra_prompt=extra_prompt)
            entry_10['amount'] = str(amount_10)
            entry_10['tax'] = str(tax_10 if tax_10 is not None else (amount_10 - int(round(amount_10 / 1.1)) if is_inclusive else int(amount_10 * 0.1)))
            entry_10['description'] = f"{entry_10['description']}（10%対象）"
            entries.append(entry_10)
        if amount_8:
            entry_8 = extract_info_from_text(text, stance, '内税8%' if is_inclusive else '外税8%', extra_prompt=extra_prompt)
            entry_8['amount'] = str(amount_8)
            entry_8['tax'] = str(tax_8 if tax_8 is not None else (amount_8 - int(round(amount_8 / 1.08)) if is_inclusive else int(amount_8 * 0.08)))
            entry_8['description'] = f"{entry_8['description']}（8%対象）"
            entries.append(entry_8)
        return entries
    # 明細行ベースの混在判定
    if has_10_percent and has_8_percent and len(item_amounts) > 1:
        amounts_10 = [item for item in item_amounts if item['tax_rate'] == 10]
        amounts_8 = [item for item in item_amounts if item['tax_rate'] == 8]
        if amounts_10:
            total_10 = sum(item['amount'] for item in amounts_10)
            entry_10 = extract_info_from_text(text, stance, '内税10%' if is_inclusive else '外税10%', extra_prompt=extra_prompt)
            entry_10['amount'] = str(total_10)
            entry_10['tax'] = str(int(total_10 * 0.1))
            entry_10['description'] = f"{entry_10['description']}（10%対象）"
            entries.append(entry_10)
        if amounts_8:
            total_8 = sum(item['amount'] for item in amounts_8)
            entry_8 = extract_info_from_text(text, stance, '内税8%' if is_inclusive else '外税8%', extra_prompt=extra_prompt)
            entry_8['amount'] = str(total_8)
            entry_8['tax'] = str(int(total_8 * 0.08))
            entry_8['description'] = f"{entry_8['description']}（8%対象）"
            entries.append(entry_8)
        return entries
    # 単一税率または混在でない場合
    entry = extract_info_from_text(text, stance, tax_mode, extra_prompt=extra_prompt)
    # 主要な値が空の場合は追加しない
    if entry.get('amount') and entry.get('account'):
        entries.append(entry)
    return entries

# テキストから情報を抽出（金額抽出精度強化版）
def extract_info_from_text(text, stance='received', tax_mode='自動判定', extra_prompt=''):
    info = {
        'company': '',
        'date': '',
        'amount': '',
        'tax': '',
        'description': '',
        'account': '',
        'account_source': '',
        'ocr_text': text
    }
    lines = text.split('\n')
    for line in lines:
        if any(keyword in line for keyword in ['株式会社', '有限会社', '合同会社', 'Studio', 'Inc', 'Corp']):
            company_line = line.strip()
            # 余計な期間情報などを除去
            company_line = re.sub(r'(集計期間|期間|\d{1,2}月分|[0-9]{4}/[0-9]{2}/[0-9]{2}～[0-9]{4}/[0-9]{2}/[0-9]{2}|[0-9]{4}年[0-9]{1,2}月分).*?(株式会社|有限会社|合同会社|Studio|Inc|Corp)', r'\2', company_line)
            # 会社名部分だけ抽出
            match = re.search(r'(株式会社|有限会社|合同会社|Studio|Inc|Corp)[^\s]*.*', company_line)
            if match:
                company_name = match.group(0)
            else:
                company_name = company_line
            # 敬称を除去
            for suffix in ['御中', '様', '殿', 'さん', '君', 'ちゃん']:
                if company_name.endswith(suffix):
                    company_name = company_name[:-len(suffix)]
                    break
            # 法人種別のみの場合は空欄にする
            if company_name.strip() in ['株式会社', '有限会社', '合同会社', 'Studio', 'Inc', 'Corp']:
                company_name = ''
            info['company'] = company_name.strip()
            break
    # 日付抽出ロジック強化
    date_patterns = [
        r'(20[0-9]{2})[年/\-\.](1[0-2]|0?[1-9])[月/\-\.](3[01]|[12][0-9]|0?[1-9])[日]?',  # 2019年10月11日
        r'(20[0-9]{2})[/-](1[0-2]|0?[1-9])[/-](3[01]|[12][0-9]|0?[1-9])',  # 2019/10/11
        r'(1[0-2]|0?[1-9])[月/\-\.](3[01]|[12][0-9]|0?[1-9])[日]?',  # 10月11日
    ]
    for pattern in date_patterns:
        for line in lines:
            # 電話番号やNo.などを除外
            if re.search(r'(電話|TEL|No\.|NO\.|レジ|会計|店|\d{4,}-\d{2,}-\d{2,}|\d{2,}-\d{4,}-\d{4,})', line, re.IGNORECASE):
                continue
            match = re.search(pattern, line)
            if match:
                if len(match.groups()) == 3:
                    year, month, day = match.groups()
                    if len(year) == 4:
                        info['date'] = f"{year}/{month.zfill(2)}/{day.zfill(2)}"
                    else:
                        current_year = datetime.now().year
                        info['date'] = f"{current_year}/{year.zfill(2)}/{month.zfill(2)}"
                break
        if info['date']:
            break
    # 期間情報（x月分、上期分、下期分など）を抽出
    period_hint = None
    period_match = re.search(r'([0-9]{1,2}月分|上期分|下期分|\d{1,2}月分)', text)
    if period_match:
        period_hint = period_match.group(1)
    
    # 金額抽出：ラベル優先・除外ワード・最下部優先・範囲・AIクロスチェック
    amount_ai = guess_amount_ai(text)
    label_keywords = r'(合計|小計|総額|ご請求金額|請求金額|合計金額)'
    exclude_keywords = r'(お預り|お預かり|お釣り|現金|釣銭|つり銭)'
    # 税ラベルを含む行も除外
    tax_label_keywords = r'(内消費税|消費税等|消費税|税率|内税|外税|税額)'
    label_amounts = []
    for i, line in enumerate(lines):
        if re.search(label_keywords, line) and not re.search(exclude_keywords, line) and not re.search(tax_label_keywords, line):
            amount_patterns = [r'([0-9,]+)円', r'¥([0-9,]+)', r'([0-9,]+)']
            for pattern in amount_patterns:
                matches = re.findall(pattern, line)
                for match in matches:
                    if isinstance(match, tuple):
                        match = [x for x in match if x][0] if any(match) else None
                    if match and match.replace(',', '').isdigit():
                        val = int(match.replace(',', ''))
                        if len(str(val)) >= 10:
                            continue
                        if is_year_number(val, line):
                            continue
                        if 1 <= val <= 10000000:
                            label_amounts.append((i, val))
    label_amount = label_amounts[-1][1] if label_amounts else None
    amount_candidates = []
    for i, line in enumerate(lines):
        if re.search(exclude_keywords, line) or re.search(tax_label_keywords, line):
            continue
        for pattern in [r'([0-9,]+)円', r'¥([0-9,]+)']:
            matches = re.findall(pattern, line)
            for m in matches:
                if isinstance(m, tuple):
                    m = [x for x in m if x][0] if any(m) else None
                if m and m.replace(',', '').isdigit():
                    val = int(m.replace(',', ''))
                    if len(str(val)) >= 10:
                        continue
                    if is_year_number(val, line):
                        continue
                    if 1 <= val <= 10000000:
                        amount_candidates.append(val)
    # レシート下部の税額記載を優先
    bottom_tax_8 = re.search(r'内[\s　]*8[%％][^\d]*(?:\\?[0-9,]+)[^\d]*(?:税額[\s　]*\\?([0-9,]+))', text)
    bottom_tax_10 = re.search(r'内[\s　]*10[%％][^\d]*(?:\\?[0-9,]+)[^\d]*(?:税額[\s　]*\\?([0-9,]+))', text)
    tax_8 = int(bottom_tax_8.group(1).replace(',', '')) if bottom_tax_8 and bottom_tax_8.group(1) else None
    tax_10 = int(bottom_tax_10.group(1).replace(',', '')) if bottom_tax_10 and bottom_tax_10.group(1) else None
    # AI値の妥当性チェック
    def is_in_exclude_line(val):
        for line in lines:
            if str(val) in line and re.search(exclude_keywords, line):
                return True
        return False
    if amount_ai:
        if is_year_number(amount_ai, text):
            amount_ai = None
        elif is_in_exclude_line(amount_ai):
            amount_ai = None
        elif not (1 <= amount_ai <= 10000000):
            amount_ai = None
    # --- 税区分・税額判定ロジックを商習慣に合わせて強化 ---
    text_lower = text.lower()
    # 明記があれば優先
    if re.search(r'外税|別途消費税|tax out|tax-out|taxout|税抜|本体価格', text_lower):
        default_tax_mode = '外税'
    elif re.search(r'内税|税込|消費税込|tax in|tax-in|taxin', text_lower):
        default_tax_mode = '内税'
    # 「消費税」や「税額」欄があり、かつ0円や空欄なら内税
    elif re.search(r'消費税|税額', text) and re.search(r'0円|¥0|0$', text):
        default_tax_mode = '内税'
    else:
        # 明記がなければデフォルトで内税
        default_tax_mode = '内税'

    # 金額決定後の税額計算に反映
    # 最終的な金額決定
    amount = None
    if amount_ai and not is_in_exclude_line(amount_ai):
        if label_amount and amount_ai == label_amount:
            amount = amount_ai
        elif not label_amount:
            amount = amount_ai
    if not amount and label_amount:
        amount = label_amount
    if not amount and amount_candidates:
        amount = max(amount_candidates)
    if amount:
        info['amount'] = str(amount)
        # 税区分判定
        if tax_mode == '内税10%':
            info['tax'] = str(tax_10 if tax_10 is not None else (amount - int(round(amount / 1.1))))
        elif tax_mode == '外税10%':
            info['tax'] = str(tax_10 if tax_10 is not None else int(amount * 0.1))
        elif tax_mode == '内税8%':
            info['tax'] = str(tax_8 if tax_8 is not None else (amount - int(round(amount / 1.08))))
        elif tax_mode == '外税8%':
            info['tax'] = str(tax_8 if tax_8 is not None else int(amount * 0.08))
        elif tax_mode == '非課税':
            info['tax'] = '0'
        else:
            # 明記がなければデフォルトで内税
            if default_tax_mode == '内税':
                if '8%' in text or '８％' in text:
                    info['tax'] = str(tax_8 if tax_8 is not None else (amount - int(round(amount / 1.08))))
                else:
                    info['tax'] = str(tax_10 if tax_10 is not None else (amount - int(round(amount / 1.1))))
            else:
                if '8%' in text or '８％' in text:
                    info['tax'] = str(tax_8 if tax_8 is not None else int(amount * 0.08))
                else:
                    info['tax'] = str(tax_10 if tax_10 is not None else int(amount * 0.1))
    
    # 摘要をAIで生成（期間情報と追加プロンプトを渡す）
    info['description'] = guess_description_ai(text, period_hint, extra_prompt=extra_prompt)
    
    # まずAIで推測（顧問先special_promptと顧問先スコープRAGを反映）
    account_ai = guess_account_ai_with_learning(text, stance, extra_prompt=extra_prompt, client_id=st.session_state.get('current_client_id', ''))
    # ルールベースで推測
    if account_ai:
        info['account'] = account_ai
        info['account_source'] = 'AI'
    else:
        # 飲料・食品系ワードが含まれる場合は会議費または消耗品費
        if re.search(r'飲料|食品|お菓子|ペットボトル|弁当|パン|コーヒー|お茶|水|ジュース', text):
            info['account'] = '会議費'
        elif stance == 'issued':
            if '売上' in text or '請求' in text or '納品' in text:
                info['account'] = '売上高'
            else:
                info['account'] = '雑収入'
        else:
            if '講義' in text or '研修' in text:
                info['account'] = '研修費'
            elif '交通' in text or 'タクシー' in text:
                info['account'] = '旅費交通費'
            elif '通信' in text or '電話' in text:
                info['account'] = '通信費'
            elif '事務用品' in text or '文具' in text:
                info['account'] = '消耗品費'
            else:
                info['account'] = '仮払金'
        info['account_source'] = 'ルール'
    return info

# マネーフォワード用カラム
MF_COLUMNS = [
    '取引No', '取引日', '借方勘定科目', '借方補助科目', '借方部門', '借方取引先', '借方税区分', '借方インボイス', '借方金額(円)', '借方税額',
    '貸方勘定科目', '貸方補助科目', '貸方部門', '貸方取引先', '貸方税区分', '貸方インボイス', '貸方金額(円)', '貸方税額',
    '摘要', '仕訳メモ', 'タグ', 'MF仕訳タイプ', '決算整理仕訳', '作成日時', '作成者', '最終更新日時', '最終更新者'
]

# 税区分自動判定関数を追加
def guess_tax_category(text, info, is_debit=True):
    # 10%や消費税のワードで判定
    if '売上' in info.get('account', ''):
        if '10%' in text or '消費税' in text:
            return '課税売上 10%'
        elif '8%' in text:
            return '課税売上 8%'
        elif '非課税' in text:
            return '非課税'
        elif '免税' in text:
            return '免税'
        else:
            return '対象外'
    else:
        if '10%' in text or '消費税' in text:
            return '課税仕入 10%'
        elif '8%' in text:
            return '課税仕入 8%'
        elif '非課税' in text:
            return '非課税'
        elif '免税' in text:
            return '免税'
        else:
            return '対象外'

# 収入/支出判定とMF用仕訳データ生成

def create_mf_journal_row(info):
    try:
        amount = int(info['amount']) if info['amount'] else 0
    except Exception:
        amount = 0
    if info['account'] in ['研修費', '教育研修費', '旅費交通費', '通信費', '消耗品費', '会議費', '交際費', '広告宣伝費', '外注費', '支払手数料', '仮払金', '修繕費', '仕入高', '減価償却費']:
        debit_account = info['account']
        credit_account = '現金'
        debit_amount = amount
        credit_amount = amount
    elif info['account'] in ['売上高', '雑収入', '受取手形', '売掛金']:
        debit_account = '現金'
        credit_account = info['account']
        debit_amount = amount
        credit_amount = amount
    else:
        debit_account = info['account']
        credit_account = '現金'
        debit_amount = amount
        credit_amount = amount
    tag = 'AI推測' if info.get('account_source') == 'AI' else 'ルール推測'
    # 税区分自動判定（OCR全文を使う）
    ocr_text = info.get('ocr_text', '')
    debit_tax = guess_tax_category(ocr_text, info, is_debit=True)
    credit_tax = guess_tax_category(ocr_text, info, is_debit=False)
    row = [
        '',
        info['date'],
        debit_account, '', '', '', debit_tax, '', debit_amount, info['tax'],
        credit_account, '', '', '', credit_tax, '', credit_amount, '0',
        info['description'], '', tag, '', '', '', '', '', '', ''
    ]
    if len(row) < len(MF_COLUMNS):
        row += [''] * (len(MF_COLUMNS) - len(row))
    elif len(row) > len(MF_COLUMNS):
        row = row[:len(MF_COLUMNS)]
    return row

# 既存のgenerate_csvを拡張
def generate_csv(info_list, output_filename, mode='default', as_txt=False):
    if mode == 'mf':
        rows = [MF_COLUMNS]
        for info in info_list:
            rows.append(create_mf_journal_row(info))
        df = pd.DataFrame(data=rows[1:], columns=pd.Index(rows[0]))
        file_extension = '.txt' if as_txt else '.csv'
        output_path = os.path.join('output', output_filename + file_extension)
        if as_txt:
            df.to_csv(output_path, index=False, header=True, encoding='utf-8-sig')
        else:
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        # 辞書形式で情報を返す
        return {
            'path': output_path,
            'filename': output_filename + file_extension,
            'mime_type': 'text/plain' if as_txt else 'text/csv'
        }
    else:
        df = pd.DataFrame(info_list)
        df = df[['date', 'account', 'account_source', 'amount', 'tax', 'company', 'description']]
        df.columns = ['取引日', '勘定科目', '推測方法', '金額', '消費税', '取引先', '摘要']
        file_extension = '.txt' if as_txt else '.csv'
        output_path = os.path.join('output', output_filename + file_extension)
        if as_txt:
            df.to_csv(output_path, index=False, header=True, encoding='utf-8-sig')
        else:
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        # 辞書形式で情報を返す
        return {
            'path': output_path,
            'filename': output_filename + file_extension,
            'mime_type': 'text/plain' if as_txt else 'text/csv'
        }

# レビュー機能の関数
def save_review_to_firestore(original_text, ai_journal, corrected_journal, reviewer_name, comments="", original_data=None, corrected_data=None):
    """レビュー内容をFirestoreに保存（詳細な修正データを含む）"""
    # レビュー機能が無効な場合は保存をスキップ（将来再有効化可能）
    if not 'REVIEW_FEATURE_ENABLED' in globals() or not REVIEW_FEATURE_ENABLED:
        st.info("レビュー機能は現在無効です（保存は行われません）。")
        return False
    if db is None:
        st.error("Firebase接続が確立されていません。")
        return False
    
    try:
        # 入力データの検証
        if not original_text or not ai_journal or not corrected_journal:
            st.error("必須データが不足しています。")
            return False
        
        if not reviewer_name or reviewer_name.strip() == "":
            reviewer_name = "匿名"
        
        # 詳細な修正データを準備
        detailed_corrections = {}
        if original_data and corrected_data:
            fields = ['company', 'date', 'amount', 'tax', 'description', 'account']
            for field in fields:
                original_value = original_data.get(field, '')
                corrected_value = corrected_data.get(field, '')
                if original_value != corrected_value:
                    detailed_corrections[field] = {
                        'original': original_value,
                        'corrected': corrected_value,
                        'was_corrected': True
                    }
                else:
                    detailed_corrections[field] = {
                        'original': original_value,
                        'corrected': corrected_value,
                        'was_corrected': False
                    }
        
        review_data = {
            'original_text': original_text,
            'ai_journal': ai_journal,
            'corrected_journal': corrected_journal,
            'reviewer_name': reviewer_name.strip(),
            'comments': comments.strip() if comments else "",
            'timestamp': datetime.now(),
            'is_corrected': ai_journal != corrected_journal,
            'detailed_corrections': detailed_corrections,
            'original_data': original_data,
            'corrected_data': corrected_data
        }
        
        # reviewsコレクションに保存
        doc_ref = db.collection('reviews').add(review_data)
        st.success(f"✅ レビューを保存しました。ID: {doc_ref[1].id}")
        
        # キャッシュを無効化（新しいレビューが追加されたため）
        cache_key = 'learning_data_cache'
        cache_timestamp_key = 'learning_data_timestamp'
        if cache_key in st.session_state:
            del st.session_state[cache_key]
        if cache_timestamp_key in st.session_state:
            del st.session_state[cache_timestamp_key]
        
        return True
    except Exception as e:
        st.error(f"❌ レビューの保存に失敗しました: {e}")
        st.error("詳細: Firebase接続またはデータ形式に問題がある可能性があります。")
        return False

def get_similar_reviews(text, limit=5):
    """類似するレビューを取得（RAG用）"""
    if db is None:
        return []
    
    try:
        # 全レビューを取得（将来的にはベクトル検索に変更）
        reviews_ref = db.collection('reviews').limit(limit).stream()
        reviews = []
        for doc in reviews_ref:
            review_data = doc.to_dict()
            # 簡単なテキスト類似度チェック（将来的にはベクトル検索に変更）
            if any(keyword in text.lower() for keyword in review_data.get('original_text', '').lower().split()):
                reviews.append(review_data)
        return reviews
    except Exception as e:
        st.warning(f"類似レビューの取得に失敗しました: {e}")
        return []

def get_all_reviews_for_learning():
    """学習用に全レビューデータを取得"""
    if db is None:
        return []
    
    try:
        reviews_ref = db.collection('reviews').stream()
        reviews = []
        for doc in reviews_ref:
            review_data = doc.to_dict()
            review_data['doc_id'] = doc.id
            reviews.append(review_data)
        return reviews
    except Exception as e:
        st.warning(f"全レビューデータの取得に失敗しました: {e}")
        return []

def extract_correction_patterns(reviews):
    """修正パターンを統計的に抽出（全項目対応）"""
    if not reviews:
        return {}
    
    patterns = {
        'account_patterns': {},
        'field_correction_stats': {
            'company': {'total': 0, 'corrected': 0},
            'date': {'total': 0, 'corrected': 0},
            'amount': {'total': 0, 'corrected': 0},
            'tax': {'total': 0, 'corrected': 0},
            'description': {'total': 0, 'corrected': 0},
            'account': {'total': 0, 'corrected': 0}
        },
        'common_corrections': {
            'company': {},
            'date': {},
            'amount': {},
            'tax': {},
            'description': {},
            'account': {}
        }
    }
    
    for review in reviews:
        if not review.get('is_corrected', False):
            continue
        
        # 詳細な修正データがある場合
        detailed_corrections = review.get('detailed_corrections', {})
        if detailed_corrections:
            for field, correction_data in detailed_corrections.items():
                if field in patterns['field_correction_stats']:
                    patterns['field_correction_stats'][field]['total'] += 1
                    if correction_data.get('was_corrected', False):
                        patterns['field_correction_stats'][field]['corrected'] += 1
                        
                        # よくある修正パターンを記録
                        original = correction_data.get('original', '')
                        corrected = correction_data.get('corrected', '')
                        if original and corrected:
                            correction_key = f"{original} → {corrected}"
                            if correction_key not in patterns['common_corrections'][field]:
                                patterns['common_corrections'][field][correction_key] = 0
                            patterns['common_corrections'][field][correction_key] += 1
        
        # 従来の勘定科目パターン抽出（後方互換性のため）
        ai_journal = review.get('ai_journal', '')
        corrected_journal = review.get('corrected_journal', '')
        
        ai_account = extract_account_from_journal(ai_journal)
        corrected_account = extract_account_from_journal(corrected_journal)
        
        if ai_account and corrected_account and ai_account != corrected_account:
            pattern_key = f"{ai_account} → {corrected_account}"
            if pattern_key not in patterns['account_patterns']:
                patterns['account_patterns'][pattern_key] = 0
            patterns['account_patterns'][pattern_key] += 1
    


def extract_account_from_journal(journal_text):
    """仕訳テキストから勘定科目を抽出"""
    if '勘定科目:' in journal_text:
        account_part = journal_text.split('勘定科目:')[1].split(',')[0].strip()
        return account_part
    return None

def extract_keywords_from_text(text):
    """テキストからキーワードを抽出"""
    # 簡単なキーワード抽出（将来的にはより高度なNLPを使用）
    keywords = set()
    
    # 金額パターン
    import re
    amounts = re.findall(r'\d{1,3}(?:,\d{3})*円', text)
    keywords.update(amounts)
    
    # 会社名・サービス名の候補
    words = text.split()
    for word in words:
        if len(word) > 2 and any(char in word for char in ['株式会社', '有限会社', '合同会社', 'サービス', '費', '料']):
            keywords.add(word)
    
    return keywords

def generate_advanced_learning_prompt(text, reviews):
    """高度な学習プロンプトを生成"""
    if not reviews:
        return ""
    
    # 修正パターンを抽出
    patterns = extract_correction_patterns(reviews)
    
    # 統計情報を生成
    total_reviews = len(reviews)
    corrected_reviews = sum(1 for r in reviews if r.get('is_corrected', False))
    accuracy_rate = ((total_reviews - corrected_reviews) / total_reviews * 100) if total_reviews > 0 else 0
    
    # 頻出する修正パターンを特定
    frequent_patterns = {k: v for k, v in patterns.items() if v['count'] >= 2}
    
    learning_prompt = f"\n\n【学習データ統計】\n"
    learning_prompt += f"総レビュー数: {total_reviews}件\n"
    learning_prompt += f"修正された仕訳: {corrected_reviews}件\n"
    learning_prompt += f"現在の正解率: {accuracy_rate:.1f}%\n"
    
    if frequent_patterns:
        learning_prompt += f"\n【頻出修正パターン】\n"
        for pattern, data in sorted(frequent_patterns.items(), key=lambda x: x[1]['count'], reverse=True)[:5]:
            learning_prompt += f"• {pattern} ({data['count']}回)\n"
            if data['examples']:
                example = data['examples'][0]
                learning_prompt += f"  例: {example['text']}\n"
                if example['comments']:
                    learning_prompt += f"  理由: {example['comments']}\n"
    
    # 類似レビューを検索
    similar_reviews = find_similar_reviews_advanced(text, reviews)
    
    if similar_reviews:
        learning_prompt += f"\n【類似修正例】\n"
        for i, review in enumerate(similar_reviews[:3], 1):
            ai_journal = review.get('ai_journal', '')
            corrected_journal = review.get('corrected_journal', '')
            comments = review.get('comments', '')
            
            learning_prompt += f"例{i}:\n"
            learning_prompt += f"AI推測: {ai_journal}\n"
            learning_prompt += f"正解: {corrected_journal}\n"
            if comments:
                learning_prompt += f"修正理由: {comments}\n"
            learning_prompt += "\n"
    
    learning_prompt += "上記の学習データを参考に、より正確な仕訳を行ってください。"
    
    return learning_prompt

def find_similar_reviews_advanced(text, reviews):
    """高度な類似レビュー検索"""
    if not reviews:
        return []
    
    # テキストの特徴を抽出
    text_features = extract_text_features(text)
    
    similarities = []
    for review in reviews:
        if not review.get('is_corrected', False):
            continue
            
        review_features = extract_text_features(review.get('original_text', ''))
        similarity_score = calculate_similarity(text_features, review_features)
        
        if similarity_score > 0.3:  # 類似度閾値
            similarities.append((similarity_score, review))
    
    # 類似度でソート
    similarities.sort(key=lambda x: x[0], reverse=True)
    
    return [review for score, review in similarities[:5]]

def extract_text_features(text):
    """テキストの特徴を抽出"""
    features = {
        'keywords': set(),
        'amounts': [],
        'companies': set(),
        'services': set()
    }
    
    import re
    
    # 金額を抽出
    amounts = re.findall(r'\d{1,3}(?:,\d{3})*円', text)
    features['amounts'] = amounts
    
    # キーワードを抽出
    words = text.lower().split()
    for word in words:
        if len(word) > 2:
            features['keywords'].add(word)
    
    # 会社名・サービス名を抽出
    company_patterns = ['株式会社', '有限会社', '合同会社', 'サービス', '事務所', 'センター']
    for pattern in company_patterns:
        if pattern in text:
            features['companies'].add(pattern)
    
    return features

def calculate_similarity(features1, features2):
    """2つのテキスト特徴の類似度を計算"""
    # キーワードの重複度
    keyword_overlap = len(features1['keywords'] & features2['keywords'])
    keyword_union = len(features1['keywords'] | features2['keywords'])
    keyword_similarity = keyword_overlap / keyword_union if keyword_union > 0 else 0
    
    # 金額の類似度
    amount_similarity = 0
    if features1['amounts'] and features2['amounts']:
        # 金額範囲の類似度を計算
        amounts1 = [int(amt.replace(',', '').replace('円', '')) for amt in features1['amounts']]
        amounts2 = [int(amt.replace(',', '').replace('円', '')) for amt in features2['amounts']]
        
        if amounts1 and amounts2:
            avg1 = sum(amounts1) / len(amounts1)
            avg2 = sum(amounts2) / len(amounts2)
            amount_diff = abs(avg1 - avg2) / max(avg1, avg2) if max(avg1, avg2) > 0 else 1
            amount_similarity = 1 - min(amount_diff, 1)
    
    # 会社・サービスの重複度
    company_overlap = len(features1['companies'] & features2['companies'])
    company_union = len(features1['companies'] | features2['companies'])
    company_similarity = company_overlap / company_union if company_union > 0 else 0
    
    # 総合類似度
    total_similarity = (keyword_similarity * 0.5 + amount_similarity * 0.3 + company_similarity * 0.2)
    
    return total_similarity

def generate_learning_prompt_from_reviews(text, similar_reviews):
    """レビューデータから学習プロンプトを生成"""
    if not similar_reviews:
        return ""
    
    learning_examples = []
    for review in similar_reviews:
        original_text = review.get('original_text', '')
        ai_journal = review.get('ai_journal', '')
        corrected_journal = review.get('corrected_journal', '')
        comments = review.get('comments', '')
        
        # 修正があった場合のみ学習例として追加
        if review.get('is_corrected', False) and ai_journal != corrected_journal:
            learning_examples.append({
                'original_text': original_text[:200] + "..." if len(original_text) > 200 else original_text,
                'ai_journal': ai_journal,
                'corrected_journal': corrected_journal,
                'comments': comments
            })
    
    if not learning_examples:
        return ""
    
    # 学習例をプロンプトに変換
    learning_prompt = "\n\n【過去の修正例から学習】\n"
    learning_prompt += "以下の修正例を参考にして、より正確な仕訳を行ってください：\n"
    
    for i, example in enumerate(learning_examples[:3], 1):  # 最大3例まで
        learning_prompt += f"\n例{i}:\n"
        learning_prompt += f"元のテキスト: {example['original_text']}\n"
        learning_prompt += f"AI推測: {example['ai_journal']}\n"
        learning_prompt += f"正解: {example['corrected_journal']}\n"
        if example['comments']:
            learning_prompt += f"修正理由: {example['comments']}\n"
    
    learning_prompt += "\n上記の修正例を参考に、今回のテキストに対してより正確な仕訳を行ってください。"
    
    return learning_prompt

def get_cached_learning_data():
    """キャッシュされた学習データを取得"""
    cache_key = 'learning_data_cache'
    cache_timestamp_key = 'learning_data_timestamp'
    
    if cache_key in st.session_state and cache_timestamp_key in st.session_state:
        # キャッシュの有効期限をチェック（1時間）
        cache_age = time.time() - st.session_state[cache_timestamp_key]
        if cache_age < 3600:  # 1時間 = 3600秒
            return st.session_state[cache_key]
    
    return None

def set_cached_learning_data(learning_data):
    """学習データをキャッシュに保存"""
    cache_key = 'learning_data_cache'
    cache_timestamp_key = 'learning_data_timestamp'
    
    st.session_state[cache_key] = learning_data
    st.session_state[cache_timestamp_key] = time.time()

def prepare_learning_data_for_cache():
    """キャッシュ用の学習データを準備"""
    try:
        reviews = get_all_reviews_for_learning()
        if not reviews:
            return None
        
        return {
            'reviews': reviews,
            'total_reviews': len(reviews),
            'timestamp': time.time()
        }
    except Exception as e:
        st.warning(f"学習データの準備に失敗しました: {e}")
        return None

def generate_cached_learning_prompt(text, cached_data):
    """キャッシュされた学習データからプロンプトを生成"""
    if not cached_data or not cached_data.get('reviews'):
        return ""
    
    try:
        # ハイブリッド検索を使用
        vector_model = None
        if VECTOR_SEARCH_AVAILABLE:
            vector_model = initialize_vector_model()
        
        similar_reviews = hybrid_search_similar_reviews(
            text, 
            cached_data['reviews'], 
            vector_model, 
            top_k=5
        )
        
        return generate_hybrid_learning_prompt(text, similar_reviews)
    except Exception as e:
        st.warning(f"キャッシュされた学習データからのプロンプト生成に失敗しました: {e}")
        return ""

def guess_account_ai_with_learning(text, stance='received', extra_prompt='', client_id: str = ''):
    """レビューデータを活用したAI推測（キャッシュ機能付き）"""
    if not OPENAI_API_KEY:
        st.warning("OpenAI APIキーが設定されていません。AI推測はスキップされます。")
        return None
    
    # 顧問先別RAGデータの準備（なければグローバル）
    client_reviews = []
    if client_id:
        client_reviews = get_all_client_learning_entries(client_id)
    use_client_scope = bool(client_reviews)
    # キャッシュ（従来のグローバル）
    cached_learning_data = get_cached_learning_data() if not use_client_scope else None
    
    if use_client_scope:
        # 顧問先スコープのベクトル/RAG
        vector_model = initialize_vector_model() if VECTOR_SEARCH_AVAILABLE else None
        similar_reviews = hybrid_search_similar_reviews(text, client_reviews, vector_model, top_k=5)
        learning_prompt = generate_hybrid_learning_prompt(text, similar_reviews)
        cache_status = f"👤 顧問先別学習データを使用 ({len(client_reviews)}件)"
    elif cached_learning_data:
        # キャッシュが有効な場合はキャッシュを使用
        learning_prompt = generate_cached_learning_prompt(text, cached_learning_data)
        cache_status = f"📚 キャッシュされた学習データを使用 ({cached_learning_data['total_reviews']}件)"
    else:
        # キャッシュが無効な場合は新しく学習データを準備
        learning_data = prepare_learning_data_for_cache()
        if learning_data:
            set_cached_learning_data(learning_data)
            learning_prompt = generate_cached_learning_prompt(text, learning_data)
            cache_status = f"🔄 新しい学習データを準備しました ({learning_data['total_reviews']}件)"
        else:
            learning_prompt = ""
            cache_status = "⚠️ 学習データの準備に失敗しました"
    
    if stance == 'issued':
        stance_prompt = "あなたは請求書を発行した側（売上計上側）の経理担当者です。売上・収入に該当する勘定科目のみを選んでください。"
        account_list = "売上高、雑収入、受取手形、売掛金"
    else:
        stance_prompt = "あなたは請求書を受領した側（費用計上側）の経理担当者です。費用・仕入・販管費に該当する勘定科目のみを選んでください。"
        account_list = "研修費、教育研修費、旅費交通費、通信費、消耗品費、会議費、交際費、広告宣伝費、外注費、支払手数料、仮払金、修繕費、仕入高、減価償却費"
    
    # 顧問先別special_promptを合成
    client_special = get_client_special_prompt(client_id) if client_id else ''
    composed_extra = '\n'.join([p for p in [extra_prompt, client_special] if p])
    
    prompt = (
        f"{stance_prompt}\n"
        "以下のテキストは領収書や請求書から抽出されたものです。\n"
        f"必ず下記の勘定科目リストから最も適切なものを1つだけ日本語で出力してください。\n"
        "\n【勘定科目リスト】\n{account_list}\n"
        "\n摘要や商品名・サービス名・講義名をそのまま勘定科目にしないでください。\n"
        "たとえば『SNS講義費』や『○○セミナー費』などは『研修費』や『教育研修費』に分類してください。\n"
        "分からない場合は必ず『仮払金』と出力してください。\n"
        "\n※『レターパック』『切手』『郵便』『ゆうパック』『ゆうメール』『ゆうパケット』『スマートレター』『ミニレター』など郵便・配送サービスに該当する場合は必ず『通信費』としてください。\n"
        "※『飲料』『食品』『お菓子』『ペットボトル』『弁当』『パン』『コーヒー』『お茶』『水』『ジュース』など飲食物や軽食・会議用の食べ物・飲み物が含まれる場合は、会議費または消耗品費を優先してください。\n"
        "\n【良い例】\n"
        "テキスト: SNS講義費 10,000円\n→ 勘定科目：研修費\n"
        "テキスト: レターパックプラス 1,200円\n→ 勘定科目：通信費\n"
        "テキスト: ペットボトル飲料・お菓子 2,000円\n→ 勘定科目：会議費\n"
        "テキスト: 食品・飲料・パン 1,500円\n→ 勘定科目：消耗品費\n"
        "\n【悪い例】\n"
        "テキスト: SNS講義費 10,000円\n→ 勘定科目：SNS講義費（×）\n"
        "テキスト: レターパックプラス 1,200円\n→ 勘定科目：広告宣伝費（×）\n"
        "テキスト: ペットボトル飲料・お菓子 2,000円\n→ 勘定科目：通信費（×）\n"
        "テキスト: 食品・飲料・パン 1,500円\n→ 勘定科目：通信費（×）\n"
        f"\n【テキスト】\n{text}\n\n勘定科目："
    ) + (f"\n【追加指示】\n{composed_extra}" if composed_extra else "") + learning_prompt
    
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }
    data = {
        "model": "gpt-4.1-nano",
        "messages": [
            {"role": "system", "content": stance_prompt},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 20,
        "temperature": 0
    }
    try:
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json=data,
            timeout=10
        )
        response.raise_for_status()
        result = response.json()
        content = result["choices"][0]["message"]["content"].strip()
        account = content.split("\n")[0].replace("勘定科目：", "").strip()
        
        # キャッシュステータスを表示
        if learning_prompt:
            st.info(cache_status)
            
            # ベクトル検索の詳細情報を表示（デバッグモード時）
            if st.session_state.get('debug_mode', False):
                # ハイブリッド検索の詳細を表示
                vector_model = None
                if VECTOR_SEARCH_AVAILABLE:
                    vector_model = initialize_vector_model()
                
                if cached_learning_data:
                    # ベクトル検索の設定を取得
                    top_k = st.session_state.get('top_k_results', 5)
                    similarity_threshold = st.session_state.get('similarity_threshold', 0.3)
                    
                    similar_reviews = hybrid_search_similar_reviews(
                        text, 
                        cached_learning_data['reviews'], 
                        vector_model, 
                        top_k=top_k
                    )
                    
                    if similar_reviews:
                        with st.expander("🔍 ベクトル検索結果（デバッグ）"):
                            st.write("**類似度の高い過去の修正例：**")
                            for i, result in enumerate(similar_reviews):
                                review = result['review']
                                similarity = result['similarity']
                                search_method = result.get('search_method', 'unknown')
                                
                                st.write(f"**{i+1}. 類似度: {similarity:.3f} ({search_method})**")
                                st.write(f"元テキスト: {review.get('original_text', '')[:100]}...")
                                st.write(f"AI推測: {review.get('ai_journal', '')}")
                                st.write(f"修正後: {review.get('corrected_journal', '')}")
                                if review.get('comments'):
                                    st.write(f"コメント: {review.get('comments', '')}")
                                st.write("---")
        
        return account
    except Exception as e:
        st.warning(f"AIによる勘定科目推測でエラー: {e}")
        return None

def get_saved_reviews(limit=10):
    """保存されたレビューデータを取得"""
    if db is None:
        return []
    
    try:
        reviews_ref = db.collection('reviews').limit(limit).stream()
        reviews = []
        for doc in reviews_ref:
            review_data = doc.to_dict()
            review_data['doc_id'] = doc.id
            reviews.append(review_data)
        return reviews
    except Exception as e:
        st.warning(f"保存されたレビューの取得に失敗しました: {e}")
        return []

def export_reviews_to_csv():
    """保存されたレビューデータをCSVファイルとしてエクスポート"""
    if db is None:
        return None
    
    try:
        reviews_ref = db.collection('reviews').stream()
        reviews = []
        for doc in reviews_ref:
            review_data = doc.to_dict()
            review_data['doc_id'] = doc.id
            reviews.append(review_data)
        
        if not reviews:
            return None
        
        # DataFrameに変換
        df_data = []
        for review in reviews:
            df_data.append({
                'ドキュメントID': review.get('doc_id', ''),
                '保存日時': review.get('timestamp', ''),
                'レビュー担当者': review.get('reviewer_name', ''),
                '修正あり': review.get('is_corrected', False),
                'コメント': review.get('comments', ''),
                '元のAI仕訳': review.get('ai_journal', ''),
                '修正後の仕訳': review.get('corrected_journal', ''),
                '元のテキスト': review.get('original_text', '')[:500] + '...' if len(review.get('original_text', '')) > 500 else review.get('original_text', '')
            })
        
        df = pd.DataFrame(df_data)
        
        # CSVファイルとして保存
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'reviews_export_{timestamp}.csv'
        filepath = os.path.join('output', filename)
        
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        return {'filename': filename, 'path': filepath, 'mime_type': 'text/csv'}
    except Exception as e:
        st.error(f"レビューデータのエクスポートに失敗しました: {e}")
        return None

def extract_text_from_pdf(pdf_bytes):
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        text = ""
        for page in reader.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
        return text.strip()
    except Exception:
        return ""

def is_text_sufficient(text):
    # 日本語が含まれ、金額や日付などの会計情報があるか簡易判定
    if len(text) < 30:
        return False
    if not re.search(r'[一-龥ぁ-んァ-ン]', text):
        return False
    if not re.search(r'\d{4}年|\d{1,2}月|\d{1,2}日|円|合計|金額', text):
        return False
    return True

# PDF.coでPDF→画像化
import base64

def upload_pdf_to_pdfco(pdf_bytes, api_key):
    url = "https://api.pdf.co/v1/file/upload"
    headers = {"x-api-key": api_key}
    files = {"file": ("file.pdf", pdf_bytes, "application/pdf")}
    response = requests.post(url, headers=headers, files=files)
    result = response.json()
    if not result.get("url"):
        raise Exception(f"PDF.co Upload APIエラー: {result.get('message', 'Unknown error')}")
    return result["url"]

def pdf_to_images_pdfco(pdf_bytes, api_key):
    # 1. まずアップロード
    file_url = upload_pdf_to_pdfco(pdf_bytes, api_key)
    # 2. 画像化
    url = "https://api.pdf.co/v1/pdf/convert/to/jpg"
    headers = {"x-api-key": api_key}
    params = {"url": file_url}
    response = requests.post(url, headers=headers, json=params)
    result = response.json()
    if result.get("error"):
        raise Exception(f"PDF.co APIエラー: {result.get('message', 'Unknown error')}")
    image_urls = result.get("urls", [])
    if not image_urls:
        raise Exception("PDF.co APIエラー: 画像URLが取得できませんでした")
    images = []
    for img_url in image_urls:
        img_resp = requests.get(img_url)
        img_resp.raise_for_status()
        images.append(img_resp.content)
    return images

# freeeインポート用カラム
FREEE_COLUMNS = [
    '[表題行]', '日付', '伝票番号', '決算整理仕訳',
    '借方勘定科目', '借方科目コード', '借方補助科目', '借方取引先', '借方取引先コード', '借方部門', '借方品目', '借方メモタグ',
    '借方セグメント1', '借方セグメント2', '借方セグメント3', '借方金額', '借方税区分', '借方税額',
    '貸方勘定科目', '貸方科目コード', '貸方補助科目', '貸方取引先', '貸方取引先コード', '貸方部門', '貸方品目', '貸方メモタグ',
    '貸方セグメント1', '貸方セグメント2', '貸方セグメント3', '貸方金額', '貸方税区分', '貸方税額', '摘要'
]

# freee税区分マッピング関数

def get_freee_tax_category(info, stance):
    """
    info: 仕訳情報（dict）
    stance: 'received' or 'issued'
    """
    tax = info.get('tax', '')
    tax_mode = info.get('tax_mode', '') if 'tax_mode' in info else ''
    description = info.get('description', '')
    account = info.get('account', '')
    # 10%/8%/5%/非課税/対象外/免税/不課税 などを判定
    # 立場で売上/仕入を分岐
    # まず税率判定
    if tax_mode:
        if '10%' in tax_mode:
            rate = '10%'
        elif '8%' in tax_mode:
            rate = '8%'
        elif '5%' in tax_mode:
            rate = '5%'
        else:
            rate = ''
    else:
        # descriptionやaccountからも判定
        if '10%' in description or '１０％' in description:
            rate = '10%'
        elif '8%' in description or '８％' in description:
            rate = '8%'
        elif '5%' in description or '５％' in description:
            rate = '5%'
        else:
            rate = ''
    # 非課税・対象外・免税
    if '非課税' in tax_mode or '非課税' in description:
        return '非課税'
    if '対象外' in tax_mode or '対象外' in description:
        return '対象外'
    if '免税' in tax_mode or '免税' in description:
        return '免税'
    if '不課税' in tax_mode or '不課税' in description:
        return '不課税'
    # 立場で売上/仕入
    if stance == 'issued':
        if rate:
            return f'課税売上{rate}'
        else:
            return '課税売上10%'
    else:
        if rate:
            return f'課税仕入{rate}'
        else:
            return '課税仕入10%'

def create_freee_journal_row(info):
    try:
        amount = int(info['amount']) if info['amount'] else 0
    except Exception:
        amount = 0
    # 借方・貸方の判定（シンプルなルール）
    if info['account'] in ['研修費', '教育研修費', '旅費交通費', '通信費', '消耗品費', '会議費', '交際費', '広告宣伝費', '外注費', '支払手数料', '仮払金', '修繕費', '仕入高', '減価償却費']:
        debit_account = info['account']
        credit_account = '現金'
        debit_amount = amount
        credit_amount = amount
        stance = 'received'
    elif info['account'] in ['売上高', '雑収入', '受取手形', '売掛金']:
        debit_account = '現金'
        credit_account = info['account']
        debit_amount = amount
        credit_amount = amount
        stance = 'issued'
    else:
        debit_account = info['account']
        credit_account = '現金'
        debit_amount = amount
        credit_amount = amount
        stance = 'received'
    # freee税区分
    debit_tax_category = get_freee_tax_category(info, stance)
    credit_tax_category = ''
    # 摘要
    description = info.get('description', '')
    # 日付
    date = info.get('date', '')
    row = [
        '仕訳', date, '', '',
        debit_account, '', '', '', '', '', '', '', '', '', '',
        debit_amount, debit_tax_category, info.get('tax', ''),
        credit_account, '', '', '', '', '', '', '', '', '', '',
        credit_amount, credit_tax_category, '',
        description
    ]
    if len(row) < len(FREEE_COLUMNS):
        row += [''] * (len(FREEE_COLUMNS) - len(row))
    elif len(row) > len(FREEE_COLUMNS):
        row = row[:len(FREEE_COLUMNS)]
    return row

def generate_freee_csv(info_list, output_filename):
    rows = [FREEE_COLUMNS]
    for info in info_list:
        rows.append(create_freee_journal_row(info))
    df = pd.DataFrame(data=rows[1:], columns=pd.Index(rows[0]))
    output_path = os.path.join('output', output_filename + '_freee.csv')
    df.to_csv(output_path, index=False, encoding='shift_jis')
    return {
        'path': output_path,
        'filename': output_filename + '_freee.csv',
        'mime_type': 'text/csv'
    }

FREEE_IMPORT_COLUMNS = [
    '収支区分', '管理番号', '発生日', '決済期日', '取引先コード', '取引先', '勘定科目', '税区分', '金額',
    '税計算区分', '税額', '備考', '品目', '部門', 'メモタグ（複数指定可、カンマ区切り）',
    'セグメント1', 'セグメント2', 'セグメント3', '決済日', '決済口座', '決済金額'
]

def get_freee_import_tax_category(info, stance):
    # サンプルに合わせた税区分表現
    tax_mode = info.get('tax_mode', '') if 'tax_mode' in info else ''
    description = info.get('description', '')
    account = info.get('account', '')
    # 10%/8%/軽/控80/課税/非課税/対象外/免税/不課税
    if '非課税' in tax_mode or '非課税' in description:
        return '非課税'
    if '対象外' in tax_mode or '対象外' in description:
        return '対象外'
    if '免税' in tax_mode or '免税' in description:
        return '免税'
    if '不課税' in tax_mode or '不課税' in description:
        return '不課税'
    # 軽減税率
    is_reduced = '軽' in tax_mode or '軽' in description or '8%' in tax_mode or '8%' in description
    # 控除80%（仕入）
    is_kou80 = '控80' in tax_mode or '控80' in description
    # 立場で売上/仕入
    if stance == 'issued':
        if is_reduced:
            return '課税売上8%（軽）'
        elif '10%' in tax_mode or '10%' in description:
            return '課税売上10%'
        elif '8%' in tax_mode or '8%' in description:
            return '課税売上8%（軽）'
        else:
            return '課税売上10%'
    else:
        if is_kou80 and is_reduced:
            return '課対仕入（控80）8%（軽）'
        elif is_kou80:
            return '課対仕入（控80）10%'
        elif is_reduced:
            return '課対仕入8%（軽）'
        elif '10%' in tax_mode or '10%' in description:
            return '課対仕入10%'
        elif '8%' in tax_mode or '8%' in description:
            return '課対仕入8%（軽)'
        else:
            return '課対仕入10%'

def get_freee_import_income_expense(info, stance):
    # 収支区分: 収入/支出
    if stance == 'issued':
        return '収入'
    else:
        return '支出'

def get_freee_import_tax_calc_mode(info):
    # 税計算区分: 内税/外税/非課税/対象外 など
    tax_mode = info.get('tax_mode', '') if 'tax_mode' in info else ''
    description = info.get('description', '')
    if '内税' in tax_mode or '内税' in description:
        return '内税'
    if '外税' in tax_mode or '外税' in description:
        return '外税'
    if '非課税' in tax_mode or '非課税' in description:
        return '非課税'
    if '対象外' in tax_mode or '対象外' in description:
        return '対象外'
    return '内税'

def create_freee_import_row(info):
    try:
        amount = int(info['amount']) if info['amount'] else 0
    except Exception:
        amount = 0
    # 立場判定
    if info['account'] in ['研修費', '教育研修費', '旅費交通費', '通信費', '消耗品費', '会議費', '交際費', '広告宣伝費', '外注費', '支払手数料', '仮払金', '修繕費', '仕入高', '減価償却費']:
        stance = 'received'
    elif info['account'] in ['売上高', '雑収入', '受取手形', '売掛金']:
        stance = 'issued'
    else:
        stance = 'received'
    # 収支区分
    income_expense = get_freee_import_income_expense(info, stance)
    # 税区分
    tax_category = get_freee_import_tax_category(info, stance)
    # 税計算区分
    tax_calc_mode = get_freee_import_tax_calc_mode(info)
    # 税額
    tax = info.get('tax', '')
    # 日付
    date = info.get('date', '')
    # 取引先
    company = info.get('company', '')
    # 勘定科目
    account = info.get('account', '')
    # 備考
    description = info.get('description', '')
    # 品目・部門・メモタグ・セグメント等は空欄でOK
    row = [
        income_expense, '', date, '', '', company, account, tax_category, amount,
        tax_calc_mode, tax, description, '', '', '', '', '', '', '', '', ''
    ]
    if len(row) < len(FREEE_IMPORT_COLUMNS):
        row += [''] * (len(FREEE_IMPORT_COLUMNS) - len(row))
    elif len(row) > len(FREEE_IMPORT_COLUMNS):
        row = row[:len(FREEE_IMPORT_COLUMNS)]
    return row

def generate_freee_import_csv(info_list, output_filename):
    rows = [FREEE_IMPORT_COLUMNS]
    for info in info_list:
        rows.append(create_freee_import_row(info))
    import pandas as pd
    df = pd.DataFrame(data=rows[1:], columns=rows[0])
    output_path = os.path.join('output', output_filename + '_freee_import.csv')
    # UTF-8 BOM付きで保存
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    return {
        'path': output_path,
        'filename': output_filename + '_freee_import.csv',
        'mime_type': 'text/csv'
    }

def generate_freee_import_txt(info_list, output_filename):
    rows = [FREEE_IMPORT_COLUMNS]
    for info in info_list:
        rows.append(create_freee_import_row(info))
    import pandas as pd
    df = pd.DataFrame(data=rows[1:], columns=pd.Index(rows[0]))
    output_path = os.path.join('output', output_filename + '_freee_import.txt')
    # UTF-8 BOM付き・タブ区切り
    df.to_csv(output_path, index=False, sep='\t', encoding='utf-8-sig')
    return {
        'path': output_path,
        'filename': output_filename + '_freee_import.txt',
        'mime_type': 'text/plain'
    }

st.title('領収書・請求書AI仕訳 Webアプリ')
if st.session_state.get('debug_mode', False):
    st.success('✅ Firebase接続が確立されました。レビュー機能が利用できます。')

# Build ID 表示（反映確認用）
def _get_build_id() -> str:
    try:
        import subprocess  # 遅延import
        rev = subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD'], text=True).strip()
        return rev
    except Exception:
        try:
            # 最終更新時刻を代替表示
            return datetime.utcnow().strftime('%Y%m%d-%H%M%S')
        except Exception:
            return 'unknown'

st.caption(f"Build: {_get_build_id()}")

# --- セッション状態の初期化 ---
if 'uploaded_files_data' not in st.session_state:
    st.session_state.uploaded_files_data = []
if 'processed_results' not in st.session_state:
    st.session_state.processed_results = []
if 'csv_file_info' not in st.session_state:
    st.session_state.csv_file_info = None
if 'current_stance' not in st.session_state:
    st.session_state.current_stance = 'received'
if 'current_tax_mode' not in st.session_state:
    st.session_state.current_tax_mode = '自動判定'
if 'current_output_mode' not in st.session_state:
    st.session_state.current_output_mode = '汎用CSV'
if 'force_pdf_ocr' not in st.session_state:
    st.session_state.force_pdf_ocr = False
if 'current_client_id' not in st.session_state:
    st.session_state.current_client_id = ''

# --- 統合UI: 共通設定エリア ---
st.subheader("🎛️ 共通設定")

# 顧問先選択（全モード共通）
# 初期起動時にバックグラウンド更新を起動しておく（起動時IOを避けるため遅延でキック）
if 'clients_cache' not in st.session_state:
    st.session_state['clients_cache'] = []
    st.session_state['clients_cache_time'] = 0
    st.session_state['clients_loading'] = False
    refresh_clients_cache(background=True)

# ステータスと手動更新UI
col_btn, col_info = st.columns([1,4])
with col_btn:
    if st.button('顧問先を読み込む', key='load_clients_btn'):
        # RESTで直接全件ページング取得（タイムアウトに強い）
        try:
            with st.spinner('顧問先を読み込み中...（RESTでページング取得）'):
                data = fetch_clients_via_rest()
            st.session_state['clients_cache'] = data
            st.session_state['clients_cache_time'] = time.time()
            st.success(f"読み込み完了: {len(data)} 件")
        except Exception:
            st.warning('読み込みに失敗しました。ネットワークまたはFirestoreを確認してください。')
with col_info:
    ts_val = st.session_state.get('clients_cache_time', 0)
    ts_str = datetime.fromtimestamp(ts_val).strftime('%Y-%m-%d %H:%M:%S') if ts_val else '未取得'
    st.caption(f"顧問先リスト 最終更新: {ts_str}")
    proj_id_for_caption = _get_project_id_from_secrets()
    if proj_id_for_caption:
        st.caption(f"Firebase project_id: {proj_id_for_caption}")
    # 診断用: Firestore件数を直接取得
    if st.button('🔎 Firestoreから直接取得（診断）'):
        data = fetch_clients_via_rest()
        st.session_state['clients_cache'] = data
        st.session_state['clients_cache_time'] = time.time()
        st.success(f"Firestoreから取得: {len(data)} 件")
    # 追加診断: プロジェクトIDとAdmin SDKでの最初の1件
    def _probe_adminsdk():
        res = {'ok': False, 'err': '', 'count': 0}
        try:
            if get_db() is None:
                res['err'] = 'Admin SDK未接続'
                return res
            docs = list(get_db().collection('clients').limit(1).stream())
            res['count'] = len(docs)
            res['ok'] = True
        except Exception as e:  # noqa: BLE001
            res['err'] = str(e)
        return res
    if st.button('🧪 Admin SDK診断（5秒）'):
        try:
            import json as _json
            sa_raw = st.secrets.get('FIREBASE_SERVICE_ACCOUNT_JSON', '{}')
            proj = ''
            try:
                proj = _json.loads(sa_raw).get('project_id', '')
            except Exception:
                proj = ''
            st.caption(f"Secretsのproject_id: {proj or '不明'}")
            import threading as _th
            holder = {'res': None}
            def _run():
                holder['res'] = _probe_adminsdk()
            th = _th.Thread(target=_run, daemon=True)
            th.start()
            th.join(5.0)
            if th.is_alive():
                st.warning('Admin SDKの読み込みがタイムアウトしました（5秒）')
            else:
                r = holder['res'] or {}
                if r.get('ok'):
                    st.success(f"Admin SDKでの読み込み: {r.get('count',0)} 件（limit=1）")
                else:
                    st.error(f"Admin SDKエラー: {r.get('err','不明')}")
        except Exception as e:  # noqa: BLE001
            st.error(f"診断でエラー: {e}")

# 自動ロード: キャッシュが空でロード中でない場合、BG読み込み開始し、オートリフレッシュ
if (not st.session_state.get('clients_cache')) and (not st.session_state.get('clients_loading', False)):
    refresh_clients_cache(background=True)
    # すぐに使えるよう、短時間の同期ロードも併用（最大6秒）
    data_now = _load_with_timeout(6.0)
    if data_now is not None and data_now:
        st.session_state['clients_cache'] = data_now
        st.session_state['clients_cache_time'] = time.time()
if st.session_state.get('clients_loading', False):
    st.caption('顧問先リストを読み込み中…')
    # セーフティ: 30秒経過したら強制的にフラグを下ろしてUIを解放し、再試行ボタンに誘導
    try:
        started = st.session_state.get('clients_loading_started_at', 0.0)
        if started and (time.time() - started) > 120:
            st.warning('読み込みがタイムアウトしました。もう一度「顧問先を読み込む」を押してください。')
            st.session_state['clients_loading'] = False
            st.session_state['clients_loading_started_at'] = 0.0
        else:
            st.autorefresh(interval=1000, key='clients_autorefresh', limit=300)
    except Exception:
        pass

def _get_clients_with_stats():
    all_clients_local = get_all_clients_raw()
    ok_clients_local = [c for c in all_clients_local if c.get('contract_ok') is True]
    return ok_clients_local, all_clients_local

clients_ok, clients_all = _get_clients_with_stats()
clients = clients_ok
total_count = len(clients_all)
ok_count = len(clients_ok)
st.caption(f"顧問先: 総件数{total_count} / 契約区分OK {ok_count}")
if total_count > 0 and ok_count == 0:
    st.warning('契約区分OKが0件です。未判定/NGを含めて一覧表示する場合は下のチェックをオンにしてください。')
    if st.checkbox('全件表示（未判定/NGを含む）', key='show_all_clients_checkbox'):
        clients = clients_all
if total_count == 0:
    st.info('顧問先が0件です。Notion同期の完了後に自動で読み込みます。必要なら「Notion顧客マスタと同期」→「BG実行」を押してください。')

def _label(c: dict) -> str:
    name = c.get('name', f"{c.get('id','')}*")
    code = str(c.get('customer_code', '')).strip()
    code_part = f"（{code}）" if code else ''
    return f"{name}{code_part}"

client_display = [_label(c) for c in clients]
label_to_id = { _label(c): c.get('id') for c in clients }
placeholder_option = '顧問先を検索して選択…'
client_display.insert(0, placeholder_option)
client_display.insert(1, '未選択（純AIフォールバック）')
selected_client = st.selectbox('顧問先を選択', client_display, index=0, key='client_select')
if selected_client and not selected_client.startswith(placeholder_option) and not selected_client.startswith('未選択'):
    st.session_state.current_client_id = label_to_id.get(selected_client, '')
else:
    # プレースホルダ/未選択は純AIフォールバック
    st.session_state.current_client_id = ''
current_client_id = st.session_state.current_client_id

# 顧問先special_prompt編集
with st.expander('顧問先の特殊事情・特徴（special_prompt）'):
    # Notion プロンプトDB IDをセッションで上書き可能に
    st.session_state['notion_prompt_db_id'] = st.text_input('Notion Prompt DB ID（別DBのID）', value=st.session_state.get('notion_prompt_db_id', st.secrets.get('NOTION_PROMPT_DATABASE_ID', '')), key='prompt_db_id_input')
    def _refetch_prompt(cid: str):
        ck = f"client_sp_prompt_{cid}"
        ct = f"client_sp_prompt_ts_{cid}"
        for k in [ck, ct]:
            if k in st.session_state:
                del st.session_state[k]
        text = get_client_special_prompt(cid) or ''
        # セット後に再実行してテキストエリアへ反映
        st.session_state['client_special_prompt_area'] = text
    colp1, colp2 = st.columns([4,1])
    with colp1:
        existing = get_client_special_prompt(current_client_id) if current_client_id else ''
        if st.session_state.get('last_client_id_for_prompt') != current_client_id:
            st.session_state['last_client_id_for_prompt'] = current_client_id
            st.session_state['client_special_prompt_area'] = existing
        new_text = st.text_area('顧問先別 special_prompt（Notion本文／参照のみ）', key='client_special_prompt_area', height=160)
    with colp2:
        st.caption('')
        st.button('🔄 再取得', disabled=not bool(current_client_id), key='refetch_prompt_btn', on_click=_refetch_prompt, args=(current_client_id,))
    # 顧問先切替時にテキストエリアをその顧問先の内容で初期化
    st.caption('編集はNotion側で行ってください（この画面は参照用）。')

# 顧問先別 学習CSV取り込み
with st.expander('📥 顧問先別 学習データ取り込み（CSV）'):
    st.caption('original_text/ai_journal/corrected_journal/comments/会社名/日付/金額/税/摘要/勘定科目 などの列があれば自動正規化します')
    csv_file = st.file_uploader('顧問先学習CSVをアップロード', type=['csv'], key='client_learning_csv')
    if csv_file and current_client_id:
        if st.button('取り込む'):
            res = add_learning_entries_from_csv(current_client_id, csv_file.getvalue())
            st.success(f"取り込み完了: 保存 {res['saved']} 件 / スキップ {res['skipped']} 件")
    elif csv_file and not current_client_id:
        st.warning('顧問先を先に選択してください')

# Notion同期
with st.expander('🔄 Notion顧客マスタと同期'):
    if NOTION_AVAILABLE:
        notion_db_id = st.text_input('Notion Database ID', value=st.secrets.get('NOTION_DATABASE_ID', ''), key='notion_db_id')
        col_n1, col_n2, col_n3 = st.columns([1,1,1])
        with col_n1:
            if st.button('Notionから同期（BG実行）'):
                start_notion_sync_bg(notion_db_id)
        with col_n2:
            if st.button('ステータス更新'):
                pass
        with col_n3:
            if st.button('同期キャンセル'):
                ns = st.session_state.setdefault('notion_sync', {})
                ns['cancel'] = True
                st.info('キャンセル要求を送信しました')

        # 接続テストはデバッグモードのみ表示
        if st.session_state.get('debug_mode', False):
            col_t1, col_t2 = st.columns([1,1])
            with col_t1:
                if st.button('Notion接続テスト'):
                    try:
                        import time as _t
                        t0 = _t.time()
                        import requests as _rq
                        token = st.secrets.get('NOTION_TOKEN', '')
                        if not token:
                            raise RuntimeError('NOTION_TOKEN 未設定')
                        hdr = {
                            'Authorization': f'Bearer {token}',
                            'Notion-Version': '2025-09-03',
                        }
                        r = _rq.get(f'https://api.notion.com/v1/databases/{notion_db_id}', headers=hdr, timeout=10)
                        r.raise_for_status()
                        st.success(f"Notion OK ({int((_t.time()-t0)*1000)}ms)")
                    except Exception as e:  # noqa: BLE001
                        st.error(f"Notion接続エラー: {e}")
            with col_t2:
                if st.button('Firestore接続テスト'):
                    try:
                        t0 = time.time()
                        result_holder = {'ok': False, 'err': ''}
                        def _grpc_probe():
                            try:
                                if get_db() is None:
                                    raise RuntimeError('Firestore未接続')
                                list(get_db().collection(clients_collection_name()).limit(1).stream())
                                result_holder['ok'] = True
                            except Exception as _e:  # noqa: BLE001
                                result_holder['err'] = str(_e)
                        th = threading.Thread(target=_grpc_probe, daemon=True)
                        th.start()
                        th.join(5.0)
                        if th.is_alive() or not result_holder['ok']:
                            from google.oauth2 import service_account as _sa
                            from google.auth.transport.requests import Request as _GARequest
                            import json as _json
                            import requests as _rq
                            sa = _json.loads(st.secrets.get('FIREBASE_SERVICE_ACCOUNT_JSON', '{}'))
                            if not sa:
                                raise RuntimeError('FIREBASE_SERVICE_ACCOUNT_JSON 未設定')
                            creds = _sa.Credentials.from_service_account_info(sa, scopes=['https://www.googleapis.com/auth/datastore'])
                            creds.refresh(_GARequest())
                            token = creds.token
                            url = f"https://firestore.googleapis.com/v1/projects/{sa.get('project_id')}/databases/(default)/documents:runQuery"
                            body = {"structuredQuery": {"from": [{"collectionId": clients_collection_name()}], "limit": 1}}
                            resp = _rq.post(url, headers={"Authorization": f"Bearer {token}"}, json=body, timeout=10)
                            resp.raise_for_status()
                            st.warning(f"gRPCは失敗またはタイムアウト。RESTはOK ({int((time.time()-t0)*1000)}ms)")
                        else:
                            st.success(f"Firestore OK ({int((time.time()-t0)*1000)}ms)")
                    except Exception as e:  # noqa: BLE001
                        st.error(f"Firestore接続エラー: {e}")
        ns = st.session_state.get('notion_sync', {})
        if ns.get('running'):
            secs = int(time.time() - ns.get('started_at', time.time()))
            st.info(f"Notion同期をバックグラウンドで実行中です… {secs}s 経過（phase: {ns.get('phase','-')}）")
            fetched = ns.get('fetched', 0)
            processed = ns.get('processed', 0)
            st.write(f"取得: {fetched} 件 / 書き込み: {processed} 件")
            # 進捗バー（取得中はフェッチ件数、書込み中は処理件数）
            denom = max(fetched if fetched > 0 else 1, 1)
            st.progress(min(1.0, processed / denom if fetched > 0 else 0.0))
            # 1秒間隔で自動リフレッシュ
            last = st.session_state.get('notion_sync_rerun_ts', 0)
            now = time.time()
            if (now - last) > 1.0:
                st.session_state['notion_sync_rerun_ts'] = now
                st.rerun()
        elif ns.get('result'):
            r = ns['result']
            st.success(f"Notion同期 完了: 更新{r['updated']} 作成{r['created']} スキップ{r['skipped']}")
            # 同期完了後に顧問先キャッシュを同期更新（UI一貫性のためスレッドを使わない）
            refresh_clients_cache(background=False)
            # 直前に表示された自動読み込みのタイムアウト表示を確実に消すため、即時再実行
            try:
                st.rerun()
            except Exception:
                pass
        elif ns.get('error'):
            st.error(f"Notion同期エラー: {ns['error']}")
            # エラー時も読み込みフラグを解除（UIが固まらないように）
            st.session_state['clients_loading'] = False
            st.session_state['clients_loading_started_at'] = 0.0
        else:
            # 実際に実行中でないのに残っている場合の見かけ上のタイムアウト表示を抑止
            st.session_state['clients_loading'] = False
            st.session_state['clients_loading_started_at'] = 0.0
    else:
        st.warning('notion-clientが利用できません。requirementsを確認してください。')

# --- v2メンテナンス（全削除） ---
with st.expander('🧨 v2メンテナンス（上級者向け）'):
    st.caption('clients_v2 を全削除してから Notion 同期で再作成します。不可逆のため注意。学習データはv2には通常未移行なので影響は限定的です。')
    if st.button('clients_v2 を全削除（不可逆）'):
        try:
            if get_db() is None:
                st.error('Firestore接続がありません。')
            else:
                # RESTで全件取得 → Admin SDKのバッチで削除
                all_docs = fetch_clients_via_rest() or []
                total = len(all_docs)
                if total == 0:
                    st.info('削除対象はありません。')
                else:
                    prog = st.progress(0.0)
                    batch = get_db().batch()
                    count = 0
                    committed = 0
                    BATCH_LIMIT = 500
                    def _commit(b, current_count):
                        if current_count == 0:
                            return b, 0
                        b.commit()
                        # ローカルでカウントするだけなので画面表示には使用しない
                        # committed += 1
                        time.sleep(0.1)
                        return get_db().batch(), 0
                    done = 0
                    for c in all_docs:
                        doc_id = c.get('id')
                        if not doc_id:
                            continue
                        batch.delete(get_db().collection(clients_collection_name()).document(doc_id))
                        count += 1
                        done += 1
                        if count >= BATCH_LIMIT:
                            batch, count = _commit(batch, count)
                        prog.progress(min(1.0, done/max(total,1)))
                    if count > 0:
                        batch, count = _commit(batch, count)
                    st.success(f"clients_v2 全削除 完了: {done} 件")
                    # キャッシュもクリア
                    st.session_state['clients_cache'] = []
                    st.session_state['clients_cache_time'] = 0
                    st.session_state['clients_loading'] = False
                    st.session_state['clients_loading_started_at'] = 0.0
        except Exception as e:  # noqa: BLE001
            st.error(f"全削除に失敗しました: {e}")

# 顧問先一覧のCSV出力
with st.expander('📤 顧問先一覧をエクスポート（CSV）'):
    if clients:
        import pandas as _pd
        df = _pd.DataFrame([
            {
                'id': c.get('id', ''),
                'name': c.get('name', ''),
                'customer_code': c.get('customer_code', ''),
                'accounting_app': c.get('accounting_app', ''),
                # v2では外部IDは出力しない（空欄）
                'external_company_id': '',
                'contract_ok': c.get('contract_ok', ''),
                'updated_at': c.get('updated_at', '')
            }
            for c in clients
        ])
        import io as _io
        csv_bytes = df.to_csv(index=False).encode('utf-8-sig')
        st.download_button('CSVをダウンロード', data=csv_bytes, file_name='clients_export.csv', mime='text/csv')
    else:
        st.caption('顧問先がありません。Notion同期後にお試しください。')

# --- 重複クリーンアップ（ドライラン/実行） ---
with st.expander('🧹 顧問先の重複クリーンアップ'):
    st.caption('キー: notion_page_id（最優先）→ なければ正規化name で同一群を特定。最新updated_atを残し、他は削除候補。')
    run_dry = st.button('重複をドライランで検出（一覧表示）')
    run_apply = st.button('削除を実行（不可逆・注意）')
    def _norm_name_key(s: str) -> str:
        import unicodedata as _ud
        return _ud.normalize('NFKC', (s or '').strip()).lower()
    if run_dry or run_apply:
        # 重複検出はキャッシュのユニーク化を避け、RESTで生データ全件を取得
        try:
            all_clients_local = fetch_clients_via_rest()
        except Exception:
            all_clients_local = get_all_clients_raw()
        # group by key
        groups = {}
        for c in all_clients_local:
            # name正規化だけでグルーピング（同名の別顧客が存在する場合は注意）
            key = _norm_name_key(c.get('name','')) or c.get('id')
            groups.setdefault(key, []).append(c)
        dup_targets = {k: v for k, v in groups.items() if len(v) > 1}
        if not dup_targets:
            st.success('重複は見つかりませんでした。')
        else:
            import pandas as _pd
            rows = []
            for k, arr in dup_targets.items():
                for c in arr:
                    rows.append({'group_key': k, 'id': c.get('id'), 'name': c.get('name'), 'updated_at': c.get('updated_at', '')})
            st.dataframe(_pd.DataFrame(rows))
            if run_apply:
                if get_db() is None:
                    st.error('Firestore接続がありません。')
                else:
                    removed = 0
                    # 進捗バー
                    total_delete = sum(max(len(v)-1, 0) for v in dup_targets.values())
                    prog = st.progress(0.0)
                    done = 0
                    batch = get_db().batch()
                    batch_count = 0
                    BATCH_LIMIT = 500
                    def _commit_with_retry(b):
                        wait = 0.5
                        for _ in range(6):
                            try:
                                b.commit()
                                break
                            except Exception:
                                time.sleep(wait)
                                wait = min(wait * 2, 8)
                        time.sleep(0.1)
                        return get_db().batch()
                    for k, arr in dup_targets.items():
                        # 最新 updated_at を残す
                        def _ts_val(v):
                            try:
                                if hasattr(v, 'timestamp'):
                                    return float(v.timestamp())
                                return float(v)
                            except Exception:
                                return 0.0
                        keep = max(arr, key=lambda c: _ts_val(c.get('updated_at') or c.get('created_at') or 0))
                        for c in arr:
                            if c.get('id') == keep.get('id'):
                                continue
                            try:
                                batch.delete(get_db().collection(clients_collection_name()).document(c.get('id')))
                                removed += 1
                                batch_count += 1
                                done += 1
                                if batch_count >= BATCH_LIMIT:
                                    batch = _commit_with_retry(batch)
                                    batch_count = 0
                                prog.progress(min(1.0, done / max(total_delete, 1)))
                            except Exception as e:  # noqa: BLE001
                                st.warning(f"削除失敗: {c.get('id')} ({e})")
                    batch = _commit_with_retry(batch)
                    batch_count = 0
                    prog.progress(1.0)
                    st.success(f"重複削除 完了: {removed} 件")
                    # 削除後のキャッシュ更新
                    refresh_clients_cache(background=False)

# 立場選択
stance = st.radio('この請求書はどちらの立場ですか？', ['受領（自社が支払う/費用）', '発行（自社が受け取る/売上）'], key='stance_radio')
stance_value = 'received' if stance.startswith('受領') else 'issued'
st.session_state.current_stance = stance_value

# 消費税区分選択UI
st_tax_mode = st.selectbox('消費税区分（自動/内税/外税/税率/非課税）', ['自動判定', '内税10%', '外税10%', '内税8%', '外税8%', '非課税'], key='tax_mode_select')
st.session_state.current_tax_mode = st_tax_mode

# PDF画像化OCR強制オプション
force_pdf_ocr = st.checkbox('PDFは常に画像化してOCRする（推奨：レイアウト崩れやフッター誤認識対策）', value=False, key='force_pdf_ocr_checkbox')
st.session_state.force_pdf_ocr = force_pdf_ocr

# --- UIの出力形式選択肢をデバッグモードで切り替え ---
if st.session_state.get('debug_mode', False):
    output_choices = ['汎用CSV', '汎用TXT', 'マネーフォワードCSV', 'マネーフォワードTXT', 'freee CSV', 'freee TXT', 'freee API直接登録']
else:
    output_choices = ['汎用CSV', 'マネーフォワードCSV', 'freee CSV', 'freee API直接登録']
def choose_output_mode_by_client(default_mode: str) -> str:
    cid = st.session_state.get('current_client_id', '')
    if not cid or get_db() is None:
        return default_mode
    try:
        doc = get_db().collection(clients_collection_name()).document(cid).get()
        if not doc.exists:
            return default_mode
        data = doc.to_dict() or {}
        app = (data.get('accounting_app') or '').lower()
        if app == 'freee':
            return 'freee API直接登録'
        if app == 'mf' or app == 'マネーフォワード':
            return 'マネーフォワードCSV'
        if app == 'csv':
            return '汎用CSV'
        return default_mode
    except Exception:
        return default_mode

auto_output = choose_output_mode_by_client(st.session_state.get('current_output_mode', '汎用CSV'))
output_mode = st.selectbox('出力形式を選択', output_choices, index=output_choices.index(auto_output) if auto_output in output_choices else 0, key='output_mode_select')
st.session_state.current_output_mode = output_mode

# --- AIへの追加指示・ヒント欄を復活 ---
extra_prompt = st.text_area('AIへの追加指示・ヒント', st.session_state.get('extra_prompt', ''), key='extra_prompt_textarea')
st.session_state.extra_prompt = extra_prompt

st.write("---")

# --- ファイルアップロード ---
st.subheader("📁 ファイルアップロード")
uploaded_files = st.file_uploader('画像またはPDFをアップロード（複数可）\n※HEICは未対応。JPEG/PNG/PDFでアップロードしてください', type=['png', 'jpg', 'jpeg', 'pdf'], accept_multiple_files=True, key='file_uploader')

# ファイルアップロード時の処理
if uploaded_files:
    # 新しいファイルがアップロードされた場合のみ処理
    current_files = [(f.name, f.getvalue()) for f in uploaded_files]
    if current_files != st.session_state.uploaded_files_data:
        st.session_state.uploaded_files_data = current_files
        st.session_state.processed_results = []  # 結果をリセット
        st.session_state.csv_file_info = None  # CSVファイル情報をリセット
        
        for uploaded_file in uploaded_files:
            file_path = os.path.join('input', uploaded_file.name)
            with open(file_path, 'wb') as f:
                f.write(uploaded_file.getbuffer())
        st.success(f'{len(uploaded_files)}個のファイルをアップロードしました。')

st.write("---")

# --- 統合処理UI ---
st.subheader("🔄 仕訳処理")

# --- デバッグモード設定 ---
def on_debug_mode_change():
    st.session_state.debug_mode = not st.session_state.get('debug_mode', False)
    st.rerun()
debug_mode = st.sidebar.checkbox('デバッグモード', value=st.session_state.get('debug_mode', False), on_change=on_debug_mode_change)
st.session_state.debug_mode = debug_mode

# ベクトル検索の設定
st.sidebar.write("---")
st.sidebar.write("**🔍 ベクトル検索設定**")

# ベクトル検索の利用可能性を確認
try:
    # ベクトル検索ライブラリの確認
    if not VECTOR_SEARCH_AVAILABLE:
        vector_status = {
            'available': False,
            'message': 'ベクトル検索ライブラリがインストールされていません',
            'recommendation': 'sentence-transformers、scikit-learn、faiss-cpuをインストールしてください'
        }
    else:
        # ベクトル検索ライブラリは利用可能だが、実際の初期化は後で行う
        vector_status = {
            'available': True,
            'message': 'ベクトル検索が利用可能です',
            'model': None  # 実際のモデルは必要時に初期化
        }
except Exception as e:
    vector_status = {
        'available': False,
        'message': f'ベクトル検索の確認に失敗: {e}',
        'recommendation': 'sentence-transformers、scikit-learn、faiss-cpuをインストールしてください'
    }
if vector_status['available']:
    st.sidebar.success("✅ ベクトル検索利用可能")
    
    # ベクトル検索の詳細設定
    vector_search_enabled = st.sidebar.checkbox('ベクトル検索を有効にする', value=True, key='vector_search_enabled')
    # セッション状態の設定は不要（checkboxのkeyで自動管理される）
    
    if vector_search_enabled:
        similarity_threshold = st.sidebar.slider(
            '類似度閾値', 
            min_value=0.1, 
            max_value=0.9, 
            value=0.3, 
            step=0.1,
            help='この値以上の類似度を持つ過去の修正例のみを参考にします',
            key='similarity_threshold'
        )
        # セッション状態の設定は不要（sliderのkeyで自動管理される）
        
        top_k_results = st.sidebar.slider(
            '検索結果数', 
            min_value=1, 
            max_value=10, 
            value=5, 
            step=1,
            help='参考にする過去の修正例の数',
            key='top_k_results'
        )
        # セッション状態の設定は不要（sliderのkeyで自動管理される）
    
    # ベクトル検索の統計情報を表示
    if st.sidebar.checkbox('ベクトル検索統計を表示', value=False, key='show_vector_stats'):
        try:
            # レビューデータの統計を取得
            reviews = get_all_reviews_for_learning()
            if reviews:
                st.sidebar.write("**📊 ベクトル検索統計**")
                st.sidebar.write(f"総レビュー数: {len(reviews)}件")
                
                # 修正ありのレビュー数をカウント
                corrected_count = sum(1 for r in reviews if r.get('is_corrected', False))
                st.sidebar.write(f"修正あり: {corrected_count}件")
                st.sidebar.write(f"正解率: {((len(reviews) - corrected_count) / len(reviews) * 100):.1f}%")
                
                # 詳細な修正統計を表示
                patterns = extract_correction_patterns(reviews)
                if patterns and 'field_correction_stats' in patterns:
                    st.sidebar.write("**📈 項目別修正統計**")
                    field_stats = patterns['field_correction_stats']
                    for field, stats in field_stats.items():
                        if stats['total'] > 0:
                            correction_rate = (stats['corrected'] / stats['total']) * 100
                            field_name_map = {
                                'company': '会社名',
                                'date': '日付',
                                'amount': '金額',
                                'tax': '消費税',
                                'description': '摘要',
                                'account': '勘定科目'
                            }
                            field_name = field_name_map.get(field, field)
                            st.sidebar.write(f"{field_name}: {correction_rate:.1f}%修正")
                    
                    # よくある修正パターンを表示
                    if 'common_corrections' in patterns:
                        st.sidebar.write("**🔧 よくある修正**")
                        common_corrections = patterns['common_corrections']
                        for field, corrections in common_corrections.items():
                            if corrections:
                                field_name_map = {
                                    'company': '会社名',
                                    'date': '日付',
                                    'amount': '金額',
                                    'tax': '消費税',
                                    'description': '摘要',
                                    'account': '勘定科目'
                                }
                                field_name = field_name_map.get(field, field)
                                # 最も多い修正パターンを表示
                                most_common = max(corrections.items(), key=lambda x: x[1])
                                if most_common[1] >= 2:  # 2回以上ある場合のみ表示
                                    st.sidebar.write(f"{field_name}: {most_common[0]} ({most_common[1]}回)")
                
                # ベクトルインデックスの構築テスト
                if st.sidebar.button('ベクトルインデックス構築テスト', key='test_vector_index'):
                    with st.spinner('インデックス構築中...'):
                        try:
                            # ベクトル検索機能が利用可能かチェック
                            if VECTOR_SEARCH_AVAILABLE:
                                # 必要時にモデルを初期化
                                model = initialize_vector_model()
                                if model:
                                    vector_index = build_vector_index(reviews, model)
                                    if vector_index:
                                        st.sidebar.success(f"✅ インデックス構築成功 ({len(reviews)}件)")
                                    else:
                                        st.sidebar.error("❌ インデックス構築失敗")
                                else:
                                    st.sidebar.error("❌ ベクトルモデルの初期化に失敗")
                            else:
                                st.sidebar.error("❌ ベクトル検索ライブラリが利用できません")
                        except Exception as e:
                            st.sidebar.error(f"❌ インデックス構築エラー: {e}")
        except Exception as e:
            st.sidebar.error(f"統計取得エラー: {e}")
else:
    st.sidebar.warning("⚠️ ベクトル検索利用不可")
    st.sidebar.write(vector_status['message'])
    if 'recommendation' in vector_status:
        st.sidebar.write(f"推奨: {vector_status['recommendation']}")
    st.session_state.vector_search_enabled = False

# 統合処理の実行
if uploaded_files and st.button("🔄 仕訳処理を開始", type="primary", key="process_button"):
    # 追加プロンプトを取得
    extra_prompt = st.session_state.get('extra_prompt', '')
    
    with st.spinner('仕訳処理中...'):
        all_results = []
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for i, uploaded_file in enumerate(uploaded_files):
            status_text.text(f"処理中: {uploaded_file.name} ({i+1}/{len(uploaded_files)})")
            
            try:
                file_path = os.path.join('input', uploaded_file.name)
                
                # OCR処理
                if uploaded_file.name.lower().endswith('.pdf'):
                    if st.session_state.get('force_pdf_ocr', False):
                        # PDFを画像化してOCR
                        try:
                            with open(file_path, 'rb') as f:
                                pdf_content = f.read()
                            images = pdf_to_images_pdfco(pdf_content, PDFCO_API_KEY)
                            text = ""
                            for img_content in images:
                                img_temp_path = os.path.join('input', f'temp_img_{int(time.time())}.jpg')
                                with open(img_temp_path, 'wb') as f:
                                    f.write(img_content)
                                text += ocr_image(img_temp_path, mode='gcv') + "\n"
                                os.remove(img_temp_path)
                        except Exception as e:
                            st.warning(f"PDF画像化OCRに失敗: {e}")
                            text = extract_text_from_pdf(uploaded_file.getvalue())
                    else:
                        text = extract_text_from_pdf(uploaded_file.getvalue())
                else:
                    text = ocr_image(file_path, mode='gcv')
                
                # テキストが十分かチェック
                if not is_text_sufficient(text):
                    st.warning(f'{uploaded_file.name}: テキストが不十分です')
                    continue
                
                # 仕訳情報抽出（共通設定の値を使用）
                entries = extract_multiple_entries(text, st.session_state.current_stance, st.session_state.current_tax_mode, debug_mode, extra_prompt)
                
                # ファイル名を追加
                for result in entries:
                    result['filename'] = uploaded_file.name
                
                all_results.extend(entries)
                st.success(f"✅ {uploaded_file.name}: {len(entries)}件の仕訳を抽出")
                
            except Exception as e:
                st.error(f"❌ {uploaded_file.name}: 処理エラー - {str(e)}")
            
            # プログレスバーを更新
            progress_bar.progress((i + 1) / len(uploaded_files))
        
        status_text.text("処理完了！")
        
        # 結果をセッション状態に保存
        st.session_state.processed_results = all_results
        
        if all_results:
            st.success(f"📊 合計 {len(all_results)}件の仕訳を抽出しました！")
            
            # 修正内容を適用したデータを作成
            corrected_results = []
            for i, result in enumerate(all_results):
                corrected_key = f"corrected_data_{i}"
                if corrected_key in st.session_state:
                    # 修正内容がある場合は修正版を使用
                    corrected_result = result.copy()
                    corrected_result.update(st.session_state[corrected_key])
                    corrected_results.append(corrected_result)
                else:
                    # 修正内容がない場合は元のデータを使用
                    corrected_results.append(result)
            
            # CSV生成
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'journal_{timestamp}'
            
            mode_map = {
                '汎用CSV': 'default',
                '汎用TXT': 'default',
                'マネーフォワードCSV': 'mf',
                'マネーフォワードTXT': 'mf',
                'freee CSV': 'freee',
                'freee TXT': 'freee'
            }
            
            if st.session_state.current_output_mode == 'freee CSV':
                csv_result = generate_freee_import_csv(corrected_results, filename)
            elif st.session_state.current_output_mode == 'freee TXT':
                csv_result = generate_freee_import_txt(corrected_results, filename)
            elif st.session_state.current_output_mode == 'マネーフォワードTXT':
                as_txt = True
                csv_result = generate_csv(corrected_results, filename, mode_map.get('マネーフォワードTXT', 'mf'), as_txt)
            else:
                as_txt = st.session_state.current_output_mode.endswith('TXT')
                csv_result = generate_csv(corrected_results, filename, mode_map.get(st.session_state.current_output_mode, 'default'), as_txt)
            
            if csv_result:
                st.session_state.csv_file_info = csv_result
                st.success(f'✅ 仕訳処理結果のCSVファイルを生成しました！')
                st.rerun()
        else:
            st.error("❌ 処理可能な仕訳が見つかりませんでした")



# CSVダウンロードボタン
if 'csv_file_info' in st.session_state and st.session_state.csv_file_info:
    try:
        csv_info = st.session_state.csv_file_info
        if isinstance(csv_info, dict) and 'path' in csv_info and 'filename' in csv_info:
            with open(csv_info['path'], 'rb') as f:
                st.download_button(
                    f"📥 {csv_info['filename']} をダウンロード",
                    f,
                    file_name=csv_info['filename'],
                    mime=csv_info.get('mime_type', 'text/csv')
                )
    except Exception as e:
        st.error(f"CSVファイルの読み込みに失敗しました: {e}")
        # セッション状態をクリア
        if 'csv_file_info' in st.session_state:
            del st.session_state.csv_file_info

# --- 処理結果表示 ---
if st.session_state.processed_results:
    st.subheader("📊 処理結果")
    
    # freee API直接登録の場合の特別処理
    if output_mode == 'freee API直接登録':
        # freee API設定の初期化
        freee_api_config = initialize_freee_api()
        freee_enabled = freee_api_config is not None

        if freee_enabled:
            # freee API直接登録UIを表示（顧客選択機能付き）
            # --- ここで推測値を明示表示（expanderをやめて常時表示） ---
            for i, result in enumerate(st.session_state.processed_results):
                st.write(f"**仕訳 {i+1} のAI推測内容プレビュー（AI推測値含む）**")
                st.info(f"日付: {result.get('date', '')}  金額: {result.get('amount', '')}円  消費税: {result.get('tax', '')}円  摘要: {result.get('description', '')}")
                st.info(f"AI推測 勘定科目: {result.get('account', '')}")
                st.info(f"AI推測 取引先: {result.get('company', '')}")
            # レビュー機能は停止中のため第三引数で無効化
            render_freee_api_ui(
                st.session_state.processed_results,
                freee_api_config,
                freee_enabled,
                review_enabled=REVIEW_FEATURE_ENABLED,
            )
        else:
            st.error("❌ freee API設定が不完全です。Streamlit Secretsで設定を確認してください。")
    else:
        # 通常のCSV/TXT出力処理
        for i, result in enumerate(st.session_state.processed_results):
            # レビュー機能オミット中でも編集フォームは残す
            st.markdown(f"### 🧾 仕訳 {i+1} の内容確認")
            # 画像表示
            if result['filename'].lower().endswith(('.jpg', '.jpeg', '.png')):
                image_path = os.path.join('input', result['filename'])
                if os.path.exists(image_path):
                    st.image(image_path, caption=f"仕訳{i+1}の画像: {result['filename']}", use_container_width=True)
            # --- 編集可能な抽出内容フォーム（2列） ---
            col1, col2 = st.columns(2)
            with col1:
                company = st.text_input("🏢 会社名", value=result['company'], key=f"company_{i}")
                date = st.text_input("📅 日付", value=result['date'], key=f"date_{i}")
                amount = st.text_input("💴 金額", value=result['amount'], key=f"amount_{i}")
            with col2:
                tax = st.text_input("🧾 消費税", value=result['tax'], key=f"tax_{i}")
                description = st.text_input("📝 摘要", value=result['description'], key=f"desc_{i}")
                account = st.text_input("📚 勘定科目", value=result['account'], key=f"account_{i}")
            # --- レビュー操作（機能停止中） ---
            if REVIEW_FEATURE_ENABLED:
                st.markdown("#### 🔍 仕訳レビュー")
                # 将来復活用
                pass

else:
    st.info("📁 ファイルをアップロードして仕訳処理を開始してください")
