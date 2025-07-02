import streamlit as st
import os
import re
import pandas as pd
from datetime import datetime
import json
from google.cloud import vision
import requests

# OpenAI APIキーをSecretsから取得
OPENAI_API_KEY = st.secrets.get("OPENAI_API_KEY", "")

# Streamlit CloudのSecretsからサービスアカウントJSONを一時ファイルに保存
if "GOOGLE_APPLICATION_CREDENTIALS_JSON" in st.secrets:
    key_path = "/tmp/gcp_key.json"
    key_dict = json.loads(st.secrets["GOOGLE_APPLICATION_CREDENTIALS_JSON"].strip())
    with open(key_path, "w") as f:
        json.dump(key_dict, f)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

# フォルダ準備
def ensure_dirs():
    os.makedirs('input', exist_ok=True)
    os.makedirs('output', exist_ok=True)

ensure_dirs()

# Google Cloud Vision APIでOCR
def ocr_image_gcv(image_path):
    client = vision.ImageAnnotatorClient()
    with open(image_path, "rb") as image_file:
        content = image_file.read()
    image = vision.Image(content=content)
    response = client.text_detection(image=image)
    texts = response.text_annotations
    if texts:
        return texts[0].description
    return ""

# ChatGPT APIで勘定科目を推測
def guess_account_ai(text):
    if not OPENAI_API_KEY:
        st.warning("OpenAI APIキーが設定されていません。AI推測はスキップされます。")
        return None
    prompt = (
        "以下は日本の会計仕訳に使う領収書や請求書のテキストです。"
        "内容（用途・目的）まで考慮し、最も適切な勘定科目を、必ず日本の会計実務で一般的に使われる標準的な勘定科目（例：研修費、教育研修費、旅費交通費、通信費、消耗品費、会議費、交際費、広告宣伝費、外注費、支払手数料、仮払金など）から1つだけ日本語で出力してください。"
        "摘要や商品名・サービス名をそのまま勘定科目にせず、必ず会計実務で使う正式な勘定科目名にしてください。"
        "分からない場合は必ず「仮払金」と出力してください。"
        "\n\nテキスト:\n" + text + "\n\n勘定科目："
    )
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }
    data = {
        "model": "gpt-3.5-turbo",
        "messages": [
            {"role": "system", "content": "あなたは日本の会計仕訳に詳しい経理担当者です。会計事務所や税理士が実務で使う正式な勘定科目のみを使ってください。摘要や商品名・サービス名をそのまま勘定科目にしないでください。"},
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
def guess_description_ai(text):
    if not OPENAI_API_KEY:
        return ""
    prompt = (
        "以下は日本の会計仕訳に使う領収書や請求書のテキストです。"
        "摘要欄には、何に使ったか・サービス名・講義名など、領収書から読み取れる具体的な用途や内容を簡潔に日本語で記載してください。"
        "金額や『消費税』などの単語だけを摘要にしないでください。"
        "\n\nテキスト:\n" + text + "\n\n摘要："
    )
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }
    data = {
        "model": "gpt-3.5-turbo",
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

# テキストから情報を抽出
def extract_info_from_text(text):
    info = {
        'company': '',
        'date': '',
        'amount': '',
        'tax': '',
        'description': '',
        'account': '',
        'account_source': ''
    }
    lines = text.split('\n')
    for line in lines:
        if any(keyword in line for keyword in ['株式会社', '有限会社', '合同会社', 'Studio', 'Inc', 'Corp']):
            company_name = line.strip()
            # 敬称を除去
            for suffix in ['御中', '様', '殿', 'さん', '君', 'ちゃん']:
                if company_name.endswith(suffix):
                    company_name = company_name[:-len(suffix)]
                    break
            info['company'] = company_name.strip()
            break
    date_patterns = [
        r'(\d{4})[年\-/](\d{1,2})[月\-/](\d{1,2})',
        r'(\d{1,2})[月\-/](\d{1,2})[日]',
        r'(\d{4})[年](\d{1,2})[月](\d{1,2})[日]'
    ]
    for pattern in date_patterns:
        match = re.search(pattern, text)
        if match:
            if len(match.groups()) == 3:
                year, month, day = match.groups()
                if len(year) == 4:
                    info['date'] = f"{year}/{month.zfill(2)}/{day.zfill(2)}"
                else:
                    current_year = datetime.now().year
                    info['date'] = f"{current_year}/{year.zfill(2)}/{month.zfill(2)}"
            break
    amount_patterns = [
        r'合計[：:]*¥?([0-9,]+)',
        r'金額[：:]*¥?([0-9,]+)',
        r'¥([0-9,]+)',
        r'([0-9,]+)円',
        r'([0-9,]+)'
    ]
    for pattern in amount_patterns:
        match = re.search(pattern, text)
        if match:
            amount_str = match.group(1).replace(',', '')
            if amount_str.isdigit():
                amount = int(amount_str)
                if 100 <= amount <= 10000000:
                    info['amount'] = str(amount)
                    info['tax'] = str(int(amount * 0.1))
                    break
    # AIで摘要を生成
    info['description'] = guess_description_ai(text)
    # まずAIで推測
    account_ai = guess_account_ai(text)
    if account_ai:
        info['account'] = account_ai
        info['account_source'] = 'AI'
    else:
        # ルールベースで推測
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

# CSVファイルを生成
def generate_csv(info_list, output_filename):
    df = pd.DataFrame(info_list)
    df = df[['date', 'account', 'account_source', 'amount', 'tax', 'company', 'description']]
    df.columns = ['取引日', '勘定科目', '推測方法', '金額', '消費税', '取引先', '摘要']
    output_path = os.path.join('output', output_filename)
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    return output_path

st.title('領収書・請求書AI仕訳 Webアプリ')

uploaded_files = st.file_uploader('画像またはPDFをアップロード（複数可）', type=['png', 'jpg', 'jpeg'], accept_multiple_files=True)

if uploaded_files:
    for uploaded_file in uploaded_files:
        file_path = os.path.join('input', uploaded_file.name)
        with open(file_path, 'wb') as f:
            f.write(uploaded_file.getbuffer())
    st.success(f'{len(uploaded_files)}個のファイルをアップロードしました。')

    if st.button('仕訳CSVを作成'):
        with st.spinner('OCR処理中...'):
            info_list = []
            for uploaded_file in uploaded_files:
                file_path = os.path.join('input', uploaded_file.name)
                text = ocr_image_gcv(file_path)
                if text:
                    st.text_area(f"抽出されたテキスト ({uploaded_file.name}):", text, height=100)
                    info = extract_info_from_text(text)
                    info_list.append(info)
                    st.write(f"**抽出結果 ({uploaded_file.name}):**")
                    st.write(f"- 会社名: {info['company']}")
                    st.write(f"- 日付: {info['date']}")
                    st.write(f"- 金額: {info['amount']}")
                    st.write(f"- 消費税: {info['tax']}")
                    st.write(f"- 摘要: {info['description']}")
                    st.write(f"- 勘定科目: {info['account']}")
                    st.write(f"- 推測方法: {info['account_source']}")
                    st.write("---")
                else:
                    st.error(f"{uploaded_file.name} からテキストを抽出できませんでした。")
            if info_list:
                first_info = info_list[0]
                company = first_info['company'] if first_info['company'] else 'Unknown'
                date_str = first_info['date'].replace('/', '') if first_info['date'] else datetime.now().strftime('%Y%m%d')
                company_clean = re.sub(r'[^\w\s-]', '', company).strip()
                if not company_clean:
                    company_clean = 'Unknown'
                output_filename = f'{company_clean}_{date_str}_output.csv'
                output_path = generate_csv(info_list, output_filename)
                st.success('仕訳CSVを作成しました。')
                df = pd.read_csv(output_path, encoding='utf-8-sig')
                st.write("**生成されたCSV内容:**")
                st.dataframe(df)
                with open(output_path, 'rb') as f:
                    st.download_button('CSVをダウンロード', f, file_name=output_filename, mime='text/csv')
            else:
                st.error('有効な情報を抽出できませんでした。')

# OCR方式を切り替えられるように
def ocr_image(image_path, mode='gcv'):
    if mode == 'gcv':
        return ocr_image_gcv(image_path)
    # 将来tesseract対応も追加可能
    else:
        raise ValueError("Unknown OCR mode") 