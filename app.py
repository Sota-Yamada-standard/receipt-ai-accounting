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
from PIL import Image
import unicodedata
# HEIC対応（将来的に対応予定）
# try:
#     import pillow_heif
#     HEIC_SUPPORT = True
# except ImportError:
#     HEIC_SUPPORT = False

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
    )
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

def guess_description_ai(text, period_hint=None):
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
    )
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
        "合計金額は『合計』『小計』『ご請求金額』『請求金額』『総額』などのラベルの直後に記載されていることが多いです。"
        "『お預り』『お預かり』『お釣り』『現金』などのラベルが付いた金額は絶対に選ばないでください。"
        "複数の金額がある場合は、合計・総額などのラベル付きで最も下にあるものを選んでください。"
        "分からない場合は空欄で出力してください。"
        "【良い例】\nテキスト: 合計 18,000円 振込先: 2688210\n→ 18000\n【悪い例】\nテキスト: 合計 18,000円 振込先: 2688210\n→ 2688210（×）"
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
        amount_str = content.split("\n")[0].replace("合計金額：", "").replace(",", "").strip()
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
    # 括弧内の外8%/外10%パターンを1行に連結
    text = re.sub(r'\((外\s*[810]{1,2}[%％][^)]*)\n([^)]*)\)', lambda m: '(' + m.group(1) + ' ' + m.group(2) + ')', text)
    text = '\n'.join([line.strip() for line in text.split('\n') if line.strip()])
    return text

# 金額・税率ごとの複数仕訳生成関数
def extract_multiple_entries(text, stance='received', tax_mode='自動判定'):
    """10%・8%混在レシートに対応した複数仕訳生成（堅牢な正規表現・税率ごとの内税/外税判定・バリデーション強化）"""
    text = preprocess_receipt_text(text)
    entries = []
    # (外8% 対象 ¥962)や(外10% 対象 ¥420)のパターン抽出（複数行対応）
    pattern_8 = re.compile(r'外\s*8[%％][^\d\n]*?対象[^\d\n]*?¥?([0-9,]+)', re.IGNORECASE | re.DOTALL)
    pattern_10 = re.compile(r'外\s*10[%％][^\d\n]*?対象[^\d\n]*?¥?([0-9,]+)', re.IGNORECASE | re.DOTALL)
    match_8 = pattern_8.search(text)
    match_10 = pattern_10.search(text)
    amount_8 = int(match_8.group(1).replace(',', '')) if match_8 and match_8.group(1) else None
    amount_10 = int(match_10.group(1).replace(',', '')) if match_10 and match_10.group(1) else None
    # 8%仕訳
    if amount_8 and amount_8 > 10:
        entry_8 = extract_info_from_text(text, stance, '外税8%')
        entry_8['amount'] = str(amount_8)
        entry_8['tax'] = str(int(amount_8 * 0.08))
        entry_8['description'] = f"{entry_8['description']}（8%対象）"
        entries.append(entry_8)
    # 10%仕訳
    if amount_10 and amount_10 > 10:
        entry_10 = extract_info_from_text(text, stance, '外税10%')
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
        entry_8 = extract_info_from_text(text, stance, f'{mode_8}8%')
        entry_8['amount'] = str(amount_8)
        entry_8['tax'] = str(tax_8 if tax_8 is not None else (amount_8 - int(round(amount_8 / 1.08)) if mode_8 == '内税' else int(amount_8 * 0.08)))
        entry_8['description'] = f"{entry_8['description']}（8%対象）"
        entries.append(entry_8)
    # 10%仕訳
    if amount_10 and amount_10 > 10:
        entry_10 = extract_info_from_text(text, stance, f'{mode_10}10%')
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
            entry_10 = extract_info_from_text(text, stance, '内税10%' if is_inclusive else '外税10%')
            entry_10['amount'] = str(amount_10)
            entry_10['tax'] = str(tax_10 if tax_10 is not None else (amount_10 - int(round(amount_10 / 1.1)) if is_inclusive else int(amount_10 * 0.1)))
            entry_10['description'] = f"{entry_10['description']}（10%対象）"
            entries.append(entry_10)
        if amount_8:
            entry_8 = extract_info_from_text(text, stance, '内税8%' if is_inclusive else '外税8%')
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
            entry_10 = extract_info_from_text(text, stance, '内税10%' if is_inclusive else '外税10%')
            entry_10['amount'] = str(total_10)
            entry_10['tax'] = str(int(total_10 * 0.1))
            entry_10['description'] = f"{entry_10['description']}（10%対象）"
            entries.append(entry_10)
        if amounts_8:
            total_8 = sum(item['amount'] for item in amounts_8)
            entry_8 = extract_info_from_text(text, stance, '内税8%' if is_inclusive else '外税8%')
            entry_8['amount'] = str(total_8)
            entry_8['tax'] = str(int(total_8 * 0.08))
            entry_8['description'] = f"{entry_8['description']}（8%対象）"
            entries.append(entry_8)
        return entries
    # 単一税率または混在でない場合
    entry = extract_info_from_text(text, stance, tax_mode)
    entries.append(entry)
    return entries

# テキストから情報を抽出（金額抽出精度強化版）
def extract_info_from_text(text, stance='received', tax_mode='自動判定'):
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
    label_amounts = []
    for i, line in enumerate(lines):
        if re.search(label_keywords, line) and not re.search(exclude_keywords, line):
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
        if re.search(exclude_keywords, line):
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
        text_lower = text.lower()
        if tax_mode == '内税10%':
            info['tax'] = str(tax_10 if tax_10 is not None else (amount - int(round(amount / 1.1))))
        elif tax_mode == '外税10%':
            info['tax'] = str(tax_10 if tax_10 is not None else int(amount * 0.1))
        elif tax_mode == '内税8%':
            info['tax'] = str(tax_8 if tax_8 is not None else (amount - int(round(amount / 1.08))))
        elif tax_mode == '外税8%':
            info['tax'] = str(tax_8 if tax_8 is not None else int(amount * 0.08))
        else:
            if re.search(r'内税|内消費税|税込|消費税込|tax in|tax-in|taxin', text_lower):
                if '8%' in text or '８％' in text:
                    info['tax'] = str(tax_8 if tax_8 is not None else (amount - int(round(amount / 1.08))))
                else:
                    info['tax'] = str(tax_10 if tax_10 is not None else (amount - int(round(amount / 1.1))))
            elif re.search(r'外税|別途消費税|tax out|tax-out|taxout', text_lower):
                if '8%' in text or '８％' in text:
                    info['tax'] = str(tax_8 if tax_8 is not None else int(amount * 0.08))
                else:
                    info['tax'] = str(tax_10 if tax_10 is not None else int(amount * 0.1))
            elif '8%' in text or '８％' in text:
                info['tax'] = str(tax_8 if tax_8 is not None else int(amount * 0.08))
            else:
                info['tax'] = str(tax_10 if tax_10 is not None else int(amount * 0.1))
    
    # 摘要をAIで生成（期間情報を渡す）
    info['description'] = guess_description_ai(text, period_hint)
    
    # まずAIで推測
    account_ai = guess_account_ai(text, stance)
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
        df = pd.DataFrame(data=rows[1:], columns=rows[0])
        output_path = os.path.join('output', output_filename)
        if as_txt:
            df.to_csv(output_path, index=False, header=True, encoding='utf-8-sig')
        else:
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
        return output_path
    else:
        df = pd.DataFrame(info_list)
        df = df[['date', 'account', 'account_source', 'amount', 'tax', 'company', 'description']]
        df.columns = ['取引日', '勘定科目', '推測方法', '金額', '消費税', '取引先', '摘要']
        output_path = os.path.join('output', output_filename)
        if as_txt:
            df.to_csv(output_path, index=False, header=True, encoding='utf-8-sig')
        else:
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
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

# 消費税区分選択UI
st_tax_mode = st.selectbox('消費税区分（自動/内税/外税/税率）', ['自動判定', '内税10%', '外税10%', '内税8%', '外税8%'])

# PDF画像化OCR強制オプション
force_pdf_ocr = st.checkbox('PDFは常に画像化してOCRする（推奨：レイアウト崩れやフッター誤認識対策）', value=False)

output_mode = st.selectbox('出力形式を選択', ['汎用CSV', '汎用TXT', 'マネーフォワードCSV', 'マネーフォワードTXT'])

uploaded_files = st.file_uploader('画像またはPDFをアップロード（複数可）\n※HEICは未対応。JPEG/PNG/PDFでアップロードしてください', type=['png', 'jpg', 'jpeg', 'pdf'], accept_multiple_files=True)

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
                # PDFの場合はオプションに応じて画像化
                if uploaded_file.name.lower().endswith('.pdf'):
                    pdf_bytes = uploaded_file.getvalue()
                    text = ''
                    if force_pdf_ocr:
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
                        for i, image in enumerate(images):
                            with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp_img:
                                image.save(tmp_img.name, format='PNG')
                                page_text = ocr_image_gcv(tmp_img.name)
                                text += page_text + '\n'
                    else:
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
                    # HEICファイルの場合はJPEGに変換
                    if uploaded_file.name.lower().endswith(('.heic', '.heif')):
                        # jpeg_path = convert_heic_to_jpeg(file_path)
                        # if jpeg_path:
                        #     text = ocr_image_gcv(jpeg_path)
                        #     # 一時ファイルを削除
                        #     try:
                        #         os.remove(jpeg_path)
                        #     except:
                        #         pass
                        # else:
                        #     text = ""
                        st.error("HEICファイルの変換は現在未対応です。JPEG/PNGでアップロードしてください。")
                        text = ""
                    else:
                        text = ocr_image_gcv(file_path)
                if text:
                    st.text_area(f"抽出されたテキスト ({uploaded_file.name}):", text, height=100)
                    # 複数仕訳生成を試みる
                    entries = extract_multiple_entries(text, stance_value, st_tax_mode)
                    if len(entries) > 1:
                        st.warning(f"{uploaded_file.name} は10%と8%の混在レシートと判断されました。複数の仕訳を生成します。")
                        for i, entry in enumerate(entries):
                            st.write(f"**仕訳 {i+1} ({uploaded_file.name}):**")
                            st.write(f"- 会社名: {entry['company']}")
                            st.write(f"- 日付: {entry['date']}")
                            st.write(f"- 金額: {entry['amount']}")
                            st.write(f"- 消費税: {entry['tax']}")
                            st.write(f"- 摘要: {entry['description']}")
                            st.write(f"- 勘定科目: {entry['account']}")
                            st.write(f"- 推測方法: {entry['account_source']}")
                            st.write("---")
                            info_list.append(entry)
                    else:
                        entry = entries[0]
                        info_list.append(entry)
                        st.write(f"**抽出結果 ({uploaded_file.name}):**")
                        st.write(f"- 会社名: {entry['company']}")
                        st.write(f"- 日付: {entry['date']}")
                        st.write(f"- 金額: {entry['amount']}")
                        st.write(f"- 消費税: {entry['tax']}")
                        st.write(f"- 摘要: {entry['description']}")
                        st.write(f"- 勘定科目: {entry['account']}")
                        st.write(f"- 推測方法: {entry['account_source']}")
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
                # 出力ファイル名と形式を決定
                if output_mode == 'マネーフォワードCSV':
                    output_filename = f'{company_clean}_{date_str}_mf.csv'
                    output_path = generate_csv(info_list, output_filename, mode='mf')
                    mime_type = 'text/csv'
                elif output_mode == 'マネーフォワードTXT':
                    output_filename = f'{company_clean}_{date_str}_mf.txt'
                    output_path = generate_csv(info_list, output_filename, mode='mf', as_txt=True)
                    mime_type = 'text/plain'
                elif output_mode == '汎用TXT':
                    output_filename = f'{company_clean}_{date_str}_output.txt'
                    output_path = generate_csv(info_list, output_filename, as_txt=True)
                    mime_type = 'text/plain'
                else:
                    output_filename = f'{company_clean}_{date_str}_output.csv'
                    output_path = generate_csv(info_list, output_filename)
                    mime_type = 'text/csv'
                st.success('仕訳ファイルを作成しました。')
                if output_path.endswith('.csv'):
                    df = pd.read_csv(output_path, encoding='utf-8-sig')
                    st.write("**生成されたCSV内容:**")
                    st.dataframe(df)
                else:
                    with open(output_path, encoding='utf-8-sig') as f:
                        st.write("**生成されたTXT内容:**")
                        st.text(f.read())
                with open(output_path, 'rb') as f:
                    st.download_button('ファイルをダウンロード', f, file_name=output_filename, mime=mime_type)
            else:
                st.error('有効な情報を抽出できませんでした。')

# OCR方式を切り替えられるように
def ocr_image(image_path, mode='gcv'):
    if mode == 'gcv':
        return ocr_image_gcv(image_path)
    # 将来tesseract対応も追加可能
    else:
        raise ValueError("Unknown OCR mode") 