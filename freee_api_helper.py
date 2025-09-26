#!/usr/bin/env python3
"""
freee API機能のヘルパーファイル
"""

import streamlit as st
import requests
import json
import os
from datetime import datetime

def initialize_freee_api():
    """freee API設定を初期化"""
    try:
        freee_config = {
            'client_id': st.secrets.get('FREEE_CLIENT_ID', ''),
            'client_secret': st.secrets.get('FREEE_CLIENT_SECRET', ''),
            'access_token': st.secrets.get('FREEE_ACCESS_TOKEN', ''),
            'company_id': st.secrets.get('FREEE_COMPANY_ID', '')
        }
        
        # 必須項目の確認
        required_fields = ['client_id', 'client_secret', 'access_token']
        missing_fields = [field for field in required_fields if not freee_config[field]]
        
        if missing_fields:
            st.error(f"freee API設定が不完全です。不足項目: {', '.join(missing_fields)}")
            return None
            
        return freee_config
    except Exception as e:
        st.error(f"freee API設定の初期化に失敗しました: {e}")
        return None

def get_freee_companies(api_config):
    """freeeの顧客企業一覧を取得（会計事務所向け）"""
    try:
        headers = {
            'Authorization': f'Bearer {api_config["access_token"]}',
            'Content-Type': 'application/json'
        }
        
        url = "https://api.freee.co.jp/api/1/companies"
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        return data.get('companies', [])
    except Exception as e:
        st.error(f"顧客企業一覧の取得に失敗しました: {e}")
        return []

def get_freee_accounts(api_config, company_id=None):
    """freeeの勘定科目一覧を取得"""
    try:
        headers = {
            'Authorization': f'Bearer {api_config["access_token"]}',
            'Content-Type': 'application/json'
        }
        
        url = f"https://api.freee.co.jp/api/1/account_items"
        params = {
            'company_id': company_id or api_config.get('company_id', '')
        }
        
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        data = response.json()
        return data.get('account_items', [])
    except Exception as e:
        st.error(f"勘定科目の取得に失敗しました: {e}")
        return []

def get_freee_partners(api_config, company_id=None):
    """freeeの取引先一覧を取得"""
    try:
        headers = {
            'Authorization': f'Bearer {api_config["access_token"]}',
            'Content-Type': 'application/json'
        }
        
        url = f"https://api.freee.co.jp/api/1/partners"
        params = {
            'company_id': company_id or api_config.get('company_id', '')
        }
        
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        data = response.json()
        return data.get('partners', [])
    except Exception as e:
        st.error(f"取引先の取得に失敗しました: {e}")
        return []

def create_freee_journal_entry(api_config, journal_data, image_path=None, company_id=None):
    """freeeに手動仕訳を登録"""
    try:
        headers = {
            'Authorization': f'Bearer {api_config["access_token"]}',
            'Content-Type': 'application/json'
        }
        
        # 仕訳データの準備
        entry_data = {
            'company_id': company_id or api_config.get('company_id', ''),
            'issue_date': journal_data['date'],
            'description': journal_data['description'],
            'details': []
        }
        
        # 借方・貸方の設定
        amount = int(journal_data['amount'])
        stance = journal_data.get('stance', 'received')
        
        if stance == 'received':  # 受領側（費用）
            # 借方：費用科目
            entry_data['details'].append({
                'account_item_id': journal_data['account_id'],
                'tax_code': journal_data.get('tax_code', 0),
                'amount': amount,
                'entry_side': 'debit'
            })
            # 貸方：現金
            entry_data['details'].append({
                'account_item_id': 1,  # 現金の勘定科目ID（要調整）
                'tax_code': 0,
                'amount': amount,
                'entry_side': 'credit'
            })
        else:  # 発行側（売上）
            # 借方：現金
            entry_data['details'].append({
                'account_item_id': 1,  # 現金の勘定科目ID（要調整）
                'tax_code': 0,
                'amount': amount,
                'entry_side': 'debit'
            })
            # 貸方：売上科目
            entry_data['details'].append({
                'account_item_id': journal_data['account_id'],
                'tax_code': journal_data.get('tax_code', 0),
                'amount': amount,
                'entry_side': 'credit'
            })
        
        # 取引先の設定
        if journal_data.get('partner_id'):
            entry_data['partner_id'] = journal_data['partner_id']
        
        url = "https://api.freee.co.jp/api/1/manual_journals"
        response = requests.post(url, headers=headers, json=entry_data)
        response.raise_for_status()
        
        result = response.json()
        
        # 証憑画像のアップロード
        if image_path and os.path.exists(image_path):
            journal_id = result.get('manual_journal', {}).get('id')
            if journal_id:
                upload_result = upload_freee_receipt(api_config, journal_id, image_path, company_id)
                if upload_result:
                    st.success("証憑画像も登録しました")
        
        return result, None
    except Exception as e:
        return None, str(e)

def upload_freee_receipt(api_config, journal_id, image_path, company_id=None):
    """freeeに証憑画像をアップロード"""
    try:
        headers = {
            'Authorization': f'Bearer {api_config["access_token"]}'
        }
        
        with open(image_path, 'rb') as f:
            files = {'receipt': f}
            data = {
                'company_id': company_id or api_config.get('company_id', ''),
                'manual_journal_id': journal_id
            }
            
            url = "https://api.freee.co.jp/api/1/receipts"
            response = requests.post(url, headers=headers, data=data, files=files)
            response.raise_for_status()
            
            return response.json()
    except Exception as e:
        st.error(f"証憑画像のアップロードに失敗しました: {e}")
        return None

def find_freee_account_by_name(accounts, account_name):
    """勘定科目名からIDを検索"""
    for account in accounts:
        if account['name'] == account_name:
            return account['id']
    return None

def find_freee_partner_by_name(partners, partner_name):
    """取引先名からIDを検索"""
    for partner in partners:
        if partner['name'] == partner_name:
            return partner['id']
    return None

def render_customer_selection_ui(freee_api_config):
    """顧客選択UIを表示"""
    if not freee_api_config:
        return None
    
    st.subheader("🏢 顧客企業選択")
    
    # 顧客一覧を取得
    if 'freee_companies' not in st.session_state:
        with st.spinner("顧客企業一覧を取得中..."):
            companies = get_freee_companies(freee_api_config)
            st.session_state.freee_companies = companies
    
    companies = st.session_state.freee_companies
    
    if not companies:
        st.error("❌ 顧客企業が見つかりません。")
        return None
    
    # 顧客選択
    company_options = [f"{company['name']} (ID: {company['id']})" for company in companies]
    selected_company = st.selectbox(
        "顧客企業を選択してください",
        company_options,
        key="selected_customer_company"
    )
    
    if selected_company:
        selected_company_id = int(selected_company.split('(ID: ')[1].rstrip(')'))
        st.success(f"✅ 選択された顧客: {selected_company.split(' (ID:')[0]}")
        return selected_company_id
    
    return None

def render_freee_api_ui(processed_results, freee_api_config, freee_enabled, review_enabled=False):
    """freee API直接登録のUIを表示"""
    if not freee_enabled:
        st.error("❌ freee APIが利用できません。設定を確認してください。")
        return
    
    st.info("🔗 freee API直接登録モード")
    
    # 顧客選択UI
    selected_company_id = render_customer_selection_ui(freee_api_config)
    
    if not selected_company_id:
        st.warning("⚠️ 顧客企業を選択してください")
        return
    
    # 選択された顧客の勘定科目と取引先を取得
    accounts_key = f'freee_accounts_{selected_company_id}'
    partners_key = f'freee_partners_{selected_company_id}'
    
    if accounts_key not in st.session_state:
        with st.spinner(f"顧客企業の勘定科目を取得中..."):
            accounts = get_freee_accounts(freee_api_config, selected_company_id)
            st.session_state[accounts_key] = accounts
    
    if partners_key not in st.session_state:
        with st.spinner(f"顧客企業の取引先を取得中..."):
            partners = get_freee_partners(freee_api_config, selected_company_id)
            st.session_state[partners_key] = partners
    
    accounts = st.session_state[accounts_key]
    partners = st.session_state[partners_key]
    
    if accounts and partners:
        for i, result in enumerate(processed_results):
            st.markdown(f"### 🧾 仕訳 {i+1} のfreee登録レビュー")
            # 画像表示
            if result['filename'].lower().endswith(('.jpg', '.jpeg', '.png')):
                image_path = os.path.join('input', result['filename'])
                if os.path.exists(image_path):
                    st.image(image_path, caption=f"仕訳{i+1}の画像: {result['filename']}", use_container_width=True)
            # --- 編集可能な抽出内容フォーム（2列・セレクトボックス） ---
            col1, col2 = st.columns(2)
            with col1:
                date = st.text_input("📅 日付", value=result.get('date', ''), key=f"freee_date_{i}")
                amount = st.text_input("💴 金額", value=result.get('amount', ''), key=f"freee_amount_{i}")
                tax = st.text_input("🧾 消費税", value=result.get('tax', ''), key=f"freee_tax_{i}")
            with col2:
                description = st.text_input("📝 摘要", value=result.get('description', ''), key=f"freee_desc_{i}")
                # 勘定科目セレクトボックス
                account_options = [f"{acc['name']} (ID: {acc['id']})" for acc in accounts]
                ai_account = result.get('account', '')
                default_account_idx = 0
                for idx, acc in enumerate(accounts):
                    if ai_account and ai_account in acc['name']:
                        default_account_idx = idx
                        break
                selected_account = st.selectbox(
                    "📚 勘定科目を選択",
                    account_options,
                    index=default_account_idx,
                    key=f"freee_account_{i}"
                )
                account_id = int(selected_account.split('(ID: ')[1].rstrip(')'))
                # 取引先セレクトボックス
                partner_options = [f"{partner['name']} (ID: {partner['id']})" for partner in partners]
                partner_options.insert(0, "取引先なし")
                ai_partner = result.get('company', '')
                default_partner_idx = 0
                for idx, partner in enumerate(partners):
                    if ai_partner and ai_partner in partner['name']:
                        default_partner_idx = idx + 1
                        break
                selected_partner = st.selectbox(
                    "🏢 取引先を選択",
                    partner_options,
                    index=default_partner_idx,
                    key=f"freee_partner_{i}"
                )
                partner_id = None
                if selected_partner != "取引先なし":
                    partner_id = int(selected_partner.split('(ID: ')[1].rstrip(')'))
            # --- レビュー操作（フラグで無効化可） ---
            if review_enabled:
                st.markdown("#### 🔍 仕訳レビュー")
                reviewer_key = f"freee_reviewer_name_{i}"
                review_status_key = f"freee_review_status_{i}"
                corrected_key = f"freee_corrected_data_{i}"
                comments_key = f"freee_comments_{i}"
                reviewer_name = st.text_input("👤 レビュアー名", value=st.session_state.get(reviewer_key, ''), key=reviewer_key)
                colb1, colb2 = st.columns(2)
                with colb1:
                    if st.button("✅ 正しい", key=f"freee_correct_btn_{i}", type="primary" if st.session_state.get(review_status_key) == "正しい" else "secondary"):
                        st.session_state[review_status_key] = "正しい"
                with colb2:
                    if st.button("❌ 修正が必要", key=f"freee_incorrect_btn_{i}", type="primary" if st.session_state.get(review_status_key) == "修正が必要" else "secondary"):
                        st.session_state[review_status_key] = "修正が必要"
                if st.session_state.get(review_status_key) == "修正が必要":
                    st.markdown("**🛠️ 修正内容を入力してください：**")
                    if corrected_key not in st.session_state:
                        st.session_state[corrected_key] = {
                            'company': ai_partner,
                            'date': date,
                            'amount': amount,
                            'tax': tax,
                            'description': description,
                            'account': ai_account
                        }
                    colr1, colr2 = st.columns(2)
                    with colr1:
                        st.session_state[corrected_key]['date'] = st.text_input("📅 修正後の日付", value=st.session_state[corrected_key]['date'], key=f"fix_freee_date_{i}")
                        st.session_state[corrected_key]['amount'] = st.text_input("💴 修正後の金額", value=st.session_state[corrected_key]['amount'], key=f"fix_freee_amount_{i}")
                        st.session_state[corrected_key]['tax'] = st.text_input("🧾 修正後の消費税", value=st.session_state[corrected_key]['tax'], key=f"fix_freee_tax_{i}")
                    with colr2:
                        st.session_state[corrected_key]['description'] = st.text_input("📝 修正後の摘要", value=st.session_state[corrected_key]['description'], key=f"fix_freee_desc_{i}")
                        # 勘定科目修正用セレクトボックス
                        st.session_state[corrected_key]['account'] = st.selectbox(
                            "📚 修正後の勘定科目",
                            account_options,
                            index=default_account_idx,
                            key=f"fix_freee_account_{i}"
                        )
                        # 取引先修正用セレクトボックス
                        st.session_state[corrected_key]['company'] = st.selectbox(
                            "🏢 修正後の取引先",
                            partner_options,
                            index=default_partner_idx,
                            key=f"fix_freee_partner_{i}"
                        )
                    comments = st.text_area("💬 修正理由・コメント", value=st.session_state.get(comments_key, ''), key=comments_key)
                elif st.session_state.get(review_status_key) == "正しい":
                    st.success("この仕訳は正しいとマークされました。")
            else:
                st.caption("レビュー機能は現在オフです（将来復活可能）")
            # 登録ボタン
            if st.button(f"📤 freeeに登録 (仕訳{i+1})", key=f"register_freee_{i}"):
                with st.spinner(f"仕訳{i+1}をfreeeに登録中..."):
                    journal_data = {
                        'date': date,
                        'description': description,
                        'amount': amount,
                        'stance': st.session_state.current_stance,
                        'account_id': account_id,
                        'partner_id': partner_id,
                        'tax_code': 0
                    }
                    image_path = None
                    if result['filename'].lower().endswith(('.jpg', '.jpeg', '.png')):
                        image_path = os.path.join('input', result['filename'])
                    response, error = create_freee_journal_entry(freee_api_config, journal_data, image_path, selected_company_id)
                    if error:
                        st.error(f"❌ 登録失敗: {error}")
                    else:
                        st.success(f"✅ 仕訳{i+1}をfreeeに登録しました！")
                        if response:
                            journal_id = response.get('manual_journal', {}).get('id')
                            st.info(f"登録された仕訳ID: {journal_id}")
    else:
        st.warning("⚠️ 勘定科目・取引先データを取得してください") 