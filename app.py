# 再デプロイ用ダミーコメント
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
def guess_account_ai(text, stance='received'):
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
        f"\n【勘定科目リスト】\n{account_list}\n"
        "\n摘要や商品名・サービス名・講義名をそのまま勘定科目にしないでください。\n"
        "たとえば『SNS講義費』や『○○セミナー費』などは『研修費』や『教育研修費』に分類してください。\n"
        "分からない場合は必ず『仮払金』と出力してください。\n"
        "\n【良い例】\n"
        "テキスト: SNS講義費 10,000円\n→ 勘定科目：研修費\n"
        "\n【悪い例】\n"
        "テキスト: SNS講義費 10,000円\n→ 勘定科目：SNS講義費（×）\n"
        f"\n【テキスト】\n{text}\n\n勘定科目："
    )
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }
    data = {
        "model": "gpt-3.5-turbo",
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

def guess_description_ai(text):
    if not OPENAI_API_KEY:
        return ""
    prompt = (
        "あなたは日本の会計実務に詳しい経理担当者です。\n"
        "以下のテキストは領収書や請求書から抽出されたものです。\n"
        "摘要欄には、何に使ったか・サービス名・講義名など、領収書から読み取れる具体的な用途や内容を20文字以内で簡潔に日本語で記載してください。\n"
        "金額や『消費税』などの単語だけを摘要にしないでください。\n"
        "\n【良い例】\n"
        "テキスト: 交通費 1,000円 タクシー利用\n→ 摘要：タクシー移動\n"
        "\n【悪い例】\n"
        "テキスト: 交通費 1,000円 タクシー利用\n→ 摘要：1,000円（×）\n"
        f"\n【テキスト】\n{text}\n\n摘要："
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

# 金額をAIで抽出

def guess_amount_ai(text):
    if not OPENAI_API_KEY:
        return None
    prompt = (
        "以下は日本の請求書や領収書から抽出したテキストです。"
        "この請求書の合計金額（支払金額、税込）を数字のみで出力してください。"
        "絶対に口座番号・登録番号・電話番号・振込先・連絡先・登録番号・TEL・No.などの数字や、10桁以上の数字、カンマ区切りでない長い数字は金額として出力しないでください。"
        "合計金額は『合計』『小計』『ご請求金額』『請求金額』などのラベルの直後に記載されていることが多いです。"
        "単位やカンマ、小数点、余計な文字は一切付けず、金額の数字のみを1つだけ出力してください。"
        "分からない場合は空欄で出力してください。"
        "【良い例】\nテキスト: 合計 18,000円 振込先: 2688210\n→ 18000\n【悪い例】\nテキスト: 合計 18,000円 振込先: 2688210\n→ 2688210（×）"
        f"\n\nテキスト:\n{text}\n\n合計金額："
    )
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }
    data = {
        "model": "gpt-3.5-turbo",
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
        amount_str = content.split("\n")[0].replace("合計金額：", "").replace(",", "").strip()
        if amount_str.isdigit():
            return int(amount_str)
        return None
    except Exception as e:
        st.warning(f"AIによる金額抽出でエラー: {e}")
        return None

# テキストから情報を抽出
def extract_info_from_text(text, stance='received'):
    info = {
        'company': '',
        'date': '',
        'amount': '',
        'tax': '',
        'description': '',
        'account': '',
        'account_source': '',
        'ocr_text': text  # OCR全文を格納
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
    # 金額抽出：AI・ルールベース両方で候補を出し、両方一致時のみ採用。AI値が口座番号等の行に含まれていれば除外。
    amount_ai = guess_amount_ai(text)
    amount_candidates = []
    label_amount = None
    label_keywords = r'(合計|小計|総額|ご請求金額|請求金額)'
    for line in lines:
        if re.search(r'(電話|TEL|登録|番号|No\.|NO\.|連絡先)', line, re.IGNORECASE):
            continue
        if re.search(label_keywords, line):
            match = re.search(r'([0-9,]+)円|¥([0-9,]+)|([0-9,]+)', line)
            if match:
                for g in match.groups():
                    if g:
                        if len(g.replace(',', '')) >= 10:
                            continue
                        val = int(g.replace(',', ''))
                        amount_candidates.append(val)
                        if label_amount is None:
                            label_amount = val
    if not amount_candidates:
        for pattern in [r'([0-9,]+)円', r'¥([0-9,]+)']:
            for m in re.findall(pattern, text):
                if isinstance(m, tuple):
                    m = [x for x in m if x][0] if any(m) else None
                if m and m.isdigit():
                    if len(m.replace(',', '')) >= 10:
                        continue
                    amount_candidates.append(int(m.replace(',', '')))
        for m in re.findall(r'([0-9,]{4,})', text):
            if m and m.isdigit():
                if len(m.replace(',', '')) >= 10:
                    continue
                idx = text.find(m)
                context = text[max(0, idx-5):idx]
                if re.search(r'(電話|TEL|登録|番号|No\.|NO\.|連絡先)', context, re.IGNORECASE):
                    continue
                amount_candidates.append(int(m.replace(',', '')))
    # AI値が口座番号・登録番号・電話番号などの行に含まれていれば除外
    def is_in_exclude_line(val):
        for line in lines:
            if str(val) in line and re.search(r'(口座|銀行|登録|番号|TEL|電話|連絡先|No\.|NO\.|振込|支店)', line, re.IGNORECASE):
                return True
        return False
    amount = None
    if amount_ai and not is_in_exclude_line(amount_ai):
        # AIとラベル付き行の金額が一致すればそれを採用
        if label_amount and amount_ai == label_amount:
            amount = amount_ai
        # ラベル付き行がなければAI値を採用
        elif not label_amount:
            amount = amount_ai
    # それ以外はラベル付き行の金額を優先
    if not amount and label_amount:
        amount = label_amount
    # それでもなければ最大値
    if not amount and amount_candidates:
        amount = max(amount_candidates)
    if amount:
        info['amount'] = str(amount)
        # 内税判定
        if re.search(r'内税|税込|消費税.*内税', text):
            tax = amount - int(round(amount / 1.1))
        else:
            tax = int(amount * 0.1)
        info['tax'] = str(tax)
    # AIで摘要を生成
    info['description'] = guess_description_ai(text)
    # まずAIで推測
    account_ai = guess_account_ai(text, stance)
    if account_ai:
        info['account'] = account_ai
        info['account_source'] = 'AI'
    else:
        # ルールベースで推測
        if stance == 'issued':
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
def generate_csv(info_list, output_filename, mode='default'):
    if mode == 'mf':
        rows = [MF_COLUMNS]
        for info in info_list:
            rows.append(create_mf_journal_row(info))
        df = pd.DataFrame(data=rows[1:], columns=rows[0])
        output_path = os.path.join('output', output_filename)
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        # txtファイルも保存
        txt_path = output_path.replace('.csv', '.txt')
        df.to_csv(txt_path, index=False, header=True, encoding='utf-8-sig')
        return output_path
    else:
        df = pd.DataFrame(info_list)
        df = df[['date', 'account', 'account_source', 'amount', 'tax', 'company', 'description']]
        df.columns = ['取引日', '勘定科目', '推測方法', '金額', '消費税', '取引先', '摘要']
        output_path = os.path.join('output', output_filename)
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        # txtファイルも保存
        txt_path = output_path.replace('.csv', '.txt')
        df.to_csv(txt_path, index=False, header=True, encoding='utf-8-sig')
        return output_path

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

st.title('領収書・請求書AI仕訳 Webアプリ')

# 立場選択を追加
stance = st.radio('この請求書はどちらの立場ですか？', ['受領（自社が支払う/費用）', '発行（自社が受け取る/売上）'])
stance_value = 'received' if stance.startswith('受領') else 'issued'

output_mode = st.selectbox('出力形式を選択', ['汎用', 'マネーフォワード'])

uploaded_files = st.file_uploader('画像またはPDFをアップロード（複数可）', type=['png', 'jpg', 'jpeg', 'pdf'], accept_multiple_files=True)

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
                # PDFの場合はまずテキスト抽出を試みる
                if uploaded_file.name.lower().endswith('.pdf'):
                    pdf_bytes = uploaded_file.getvalue()
                    text = extract_text_from_pdf(pdf_bytes)
                    if not is_text_sufficient(text):
                        # テキストが不十分なら画像化
                        images = None
                        if platform.system() == "Darwin":
                            try:
                                images = convert_from_bytes(pdf_bytes)
                            except Exception as e:
                                st.warning(f"ローカル画像化失敗: {e}。PDF.co APIで画像化を試みます。")
                        if images is None:
                            if not PDFCO_API_KEY:
                                st.error("PDF.co APIキーが設定されていません。secrets.tomlを確認してください。")
                                st.stop()
                            try:
                                images_bytes = pdf_to_images_pdfco(pdf_bytes, PDFCO_API_KEY)
                                import PIL.Image
                                images = [PIL.Image.open(io.BytesIO(img)) for img in images_bytes]
                            except Exception as e:
                                st.error(f"PDF.co APIによるPDF画像化に失敗しました: {e}")
                                st.stop()
                        text = ''
                        for i, image in enumerate(images):
                            with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp_img:
                                image.save(tmp_img.name, format='PNG')
                                page_text = ocr_image_gcv(tmp_img.name)
                                text += page_text + '\n'
                else:
                    text = ocr_image_gcv(file_path)
                if text:
                    st.text_area(f"抽出されたテキスト ({uploaded_file.name}):", text, height=100)
                    info = extract_info_from_text(text, stance_value)
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
                company_clean = re.sub(r'[\W\s-]', '', company).strip()
                if not company_clean:
                    company_clean = 'Unknown'
                if output_mode == 'マネーフォワード':
                    output_filename = f'{company_clean}_{date_str}_mf.csv'
                    output_path = generate_csv(info_list, output_filename, mode='mf')
                else:
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