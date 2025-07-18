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
import firebase_admin
from firebase_admin import credentials, firestore
import time

# ベクトル検索用ライブラリ
try:
    from sentence_transformers import SentenceTransformer
    from sklearn.metrics.pairwise import cosine_similarity
    import faiss
    VECTOR_SEARCH_AVAILABLE = True
except ImportError:
    VECTOR_SEARCH_AVAILABLE = False
    st.warning("⚠️ ベクトル検索機能を利用するには、sentence-transformers、scikit-learn、faiss-cpuをインストールしてください。")
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

# Firestoreクライアントを初期化
try:
    db = initialize_firebase()
except Exception as e:
    st.error(f"Firebase初期化で予期しないエラーが発生しました: {e}")
    db = None

# Firebase接続のデバッグ表示（デバッグモード時のみ表示）
if st.sidebar.checkbox('Firebase接続テストを表示', value=False, key='show_firebase_debug'):
    st.write("🔍 Firebase接続テスト開始...")

    # Firebaseコンソールへのリンクを表示
    st.write("### 📊 保存されたデータの確認方法")
    st.write("**Firebaseコンソールで確認する場合：**")
    st.write("1. [Firebase Console](https://console.firebase.google.com/) にアクセス")
    st.write("2. プロジェクトを選択")
    st.write("3. 左メニューから「Firestore Database」をクリック")
    st.write("4. `reviews`コレクションを確認")

    # Secretsの存在確認
    if "FIREBASE_SERVICE_ACCOUNT_JSON" in st.secrets:
        st.write("✅ FIREBASE_SERVICE_ACCOUNT_JSON が見つかりました")
        try:
            # JSONの解析テスト
            service_account_info = json.loads(st.secrets["FIREBASE_SERVICE_ACCOUNT_JSON"])
            st.write("✅ JSONの解析に成功しました")
            st.write(f"📋 Project ID: {service_account_info.get('project_id', 'N/A')}")
        except json.JSONDecodeError as e:
            st.error(f"❌ JSONの解析に失敗しました: {e}")
            st.write("🔍 現在の設定値:")
            st.code(st.secrets["FIREBASE_SERVICE_ACCOUNT_JSON"][:200] + "...")
    else:
        st.error("❌ FIREBASE_SERVICE_ACCOUNT_JSON が見つかりません")

    # Firebase接続テスト
    if db is None:
        st.error("⚠️ Firebase接続に失敗しました。secrets.tomlの設定を確認してください。")
    else:
        st.success("✅ Firebase接続が確立されました。")
        try:
            # 簡単な接続テスト
            test_doc = db.collection('test').document('connection_test')
            test_doc.set({'timestamp': 'test'})
            st.success("✅ Firestoreへの書き込みテストに成功しました")
        except Exception as e:
            st.error(f"❌ Firestoreへの書き込みテストに失敗しました: {e}")

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
    
    # まずAIで推測
    account_ai = guess_account_ai(text, stance, extra_prompt=extra_prompt)
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
def save_review_to_firestore(original_text, ai_journal, corrected_journal, reviewer_name, comments=""):
    """レビュー内容をFirestoreに保存"""
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
        
        review_data = {
            'original_text': original_text,
            'ai_journal': ai_journal,
            'corrected_journal': corrected_journal,
            'reviewer_name': reviewer_name.strip(),
            'comments': comments.strip() if comments else "",
            'timestamp': datetime.now(),
            'is_corrected': ai_journal != corrected_journal
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
    """修正パターンを統計的に抽出"""
    if not reviews:
        return {}
    
    patterns = {}
    
    for review in reviews:
        if not review.get('is_corrected', False):
            continue
            
        ai_journal = review.get('ai_journal', '')
        corrected_journal = review.get('corrected_journal', '')
        
        # AI推測と修正後の勘定科目を抽出
        ai_account = extract_account_from_journal(ai_journal)
        corrected_account = extract_account_from_journal(corrected_journal)
        
        if ai_account and corrected_account and ai_account != corrected_account:
            pattern_key = f"{ai_account} → {corrected_account}"
            if pattern_key not in patterns:
                patterns[pattern_key] = {
                    'count': 0,
                    'examples': [],
                    'keywords': set()
                }
            
            patterns[pattern_key]['count'] += 1
            
            # キーワードを抽出
            original_text = review.get('original_text', '').lower()
            keywords = extract_keywords_from_text(original_text)
            patterns[pattern_key]['keywords'].update(keywords)
            
            # 例を保存（最大5例まで）
            if len(patterns[pattern_key]['examples']) < 5:
                patterns[pattern_key]['examples'].append({
                    'text': original_text[:100] + "..." if len(original_text) > 100 else original_text,
                    'comments': review.get('comments', '')
                })
    
    return patterns

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

def guess_account_ai_with_learning(text, stance='received', extra_prompt=''):
    """レビューデータを活用したAI推測（キャッシュ機能付き）"""
    if not OPENAI_API_KEY:
        st.warning("OpenAI APIキーが設定されていません。AI推測はスキップされます。")
        return None
    
    # キャッシュされた学習データを取得
    cached_learning_data = get_cached_learning_data()
    
    if cached_learning_data:
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
    ) + (f"\n【追加指示】\n{extra_prompt}" if extra_prompt else "") + learning_prompt
    
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

st.title('領収書・請求書AI仕訳 Webアプリ')

# Firebase接続状態の簡易表示
if db is None:
    st.warning("⚠️ Firebase接続に失敗しました。レビュー機能が利用できません。")
else:
    st.success("✅ Firebase接続が確立されました。レビュー機能が利用できます。")

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

# --- タブによる処理モード選択 ---
tab1, tab2 = st.tabs(["📄 単一処理", "🚀 バッチ処理"])

with tab1:
    st.subheader("📄 単一処理モード")
    
    # --- UIにデバッグモード追加 ---
    debug_mode = st.sidebar.checkbox('デバッグモード', value=False)

    # 立場選択を追加
    stance = st.radio('この請求書はどちらの立場ですか？', ['受領（自社が支払う/費用）', '発行（自社が受け取る/売上）'], key='stance_radio')
    stance_value = 'received' if stance.startswith('受領') else 'issued'
    st.session_state.current_stance = stance_value

    # 消費税区分選択UI
    st_tax_mode = st.selectbox('消費税区分（自動/内税/外税/税率/非課税）', ['自動判定', '内税10%', '外税10%', '内税8%', '外税8%', '非課税'], key='tax_mode_select')
    st.session_state.current_tax_mode = st_tax_mode

    # PDF画像化OCR強制オプション
    force_pdf_ocr = st.checkbox('PDFは常に画像化してOCRする（推奨：レイアウト崩れやフッター誤認識対策）', value=False, key='force_pdf_ocr_checkbox')
    st.session_state.force_pdf_ocr = force_pdf_ocr

    output_mode = st.selectbox('出力形式を選択', ['汎用CSV', '汎用TXT', 'マネーフォワードCSV', 'マネーフォワードTXT'], key='output_mode_select')
    st.session_state.current_output_mode = output_mode

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

    # 単一処理モードの追加プロンプト
    extra_prompt = st.text_area('AIへの追加指示・ヒント', '', key='extra_prompt_textarea')
    
    # 仕訳CSV作成ボタン
    if st.button('仕訳CSVを作成', type='primary', key='create_csv_button'):
        with st.spinner('仕訳処理中...'):
            all_entries = []
            for uploaded_file in uploaded_files:
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
                
                # 仕訳情報抽出
                entries = extract_multiple_entries(text, stance_value, st_tax_mode, debug_mode, extra_prompt)
                all_entries.extend(entries)
            
            # 結果をセッション状態に保存
            st.session_state.processed_results = all_entries
            
            # CSV生成
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'journal_{timestamp}'
            
            mode_map = {
                '汎用CSV': 'default',
                '汎用TXT': 'default',
                'マネーフォワードCSV': 'mf',
                'マネーフォワードTXT': 'mf'
            }
            
            as_txt = output_mode.endswith('TXT')
            csv_result = generate_csv(all_entries, filename, mode_map[output_mode], as_txt)
            
            if csv_result:
                st.session_state.csv_file_info = csv_result
                st.success(f'✅ {len(all_entries)}件の仕訳を含むCSVファイルを生成しました！')
                st.rerun()
    
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

    # 処理済み結果がある場合は表示
    if st.session_state.processed_results:
        st.write("### 📋 処理済みの仕訳結果")
        st.success("✅ 仕訳処理が完了しました！以下の結果を確認し、レビューを行ってください。")
        
        for i, result in enumerate(st.session_state.processed_results):
            st.write(f"**📄 仕訳 {i+1}:**")
            col1, col2 = st.columns(2)
            with col1:
                st.write(f"🏢 **会社名:** {result['company']}")
                st.write(f"📅 **日付:** {result['date']}")
                st.write(f"💰 **金額:** {result['amount']}")
            with col2:
                st.write(f"🧾 **消費税:** {result['tax']}")
                st.write(f"📝 **摘要:** {result['description']}")
                st.write(f"🏷️ **勘定科目:** {result['account']}")
            st.write(f"🤖 **推測方法:** {result['account_source']}")
            
            # レビュー機能を追加
            st.write("---")
            st.subheader(f"仕訳 {i+1} のレビュー")
            
            # セッション状態の初期化
            review_key = f"review_state_{i}"
            if review_key not in st.session_state:
                st.session_state[review_key] = "正しい"
            
            reviewer_name = st.text_input("レビュー担当者名", key=f"reviewer_{i}")
            
            # 現在の選択状態を表示
            st.write(f"**現在の選択: {st.session_state[review_key]}**")
            
            # ラジオボタンの代わりにボタンを使用
            col1, col2 = st.columns(2)
            with col1:
                if st.button("✅ 正しい", key=f"correct_btn_{i}", type="primary" if st.session_state[review_key] == "正しい" else "secondary"):
                    st.session_state[review_key] = "正しい"
                    st.rerun()
            with col2:
                if st.button("❌ 修正が必要", key=f"incorrect_btn_{i}", type="primary" if st.session_state[review_key] == "修正が必要" else "secondary"):
                    st.session_state[review_key] = "修正が必要"
                    st.rerun()
            
            # 条件分岐を別セクションに分離
            if st.session_state[review_key] == "修正が必要":
                st.write("**修正内容を入力してください：**")
                corrected_account = st.text_input("修正後の勘定科目", value=result['account'], key=f"account_{i}")
                corrected_description = st.text_input("修正後の摘要", value=result['description'], key=f"desc_{i}")
                comments = st.text_area("修正理由・コメント", placeholder="修正が必要な理由や追加のコメントを入力してください", key=f"comments_{i}")
                
                # 修正内容を保存ボタン
                if st.button("💾 修正内容を保存", key=f"save_corrected_{i}", type="primary"):
                    # 修正後の仕訳を作成
                    corrected_journal = f"仕訳: {corrected_account} {result['amount']}円"
                    if result['tax'] != '0':
                        corrected_journal += f" (消費税: {result['tax']}円)"
                    corrected_journal += f" - {corrected_description}"
                    
                    # 元の仕訳を作成
                    original_journal = f"仕訳: {result['account']} {result['amount']}円"
                    if result['tax'] != '0':
                        original_journal += f" (消費税: {result['tax']}円)"
                    original_journal += f" - {result['description']}"
                    
                    # レビューを保存
                    if save_review_to_firestore(
                        result.get('original_text', ''),
                        original_journal,
                        corrected_journal,
                        reviewer_name,
                        comments
                    ):
                        st.success("✅ レビューを保存しました！")
                        # キャッシュをクリアして学習データを更新
                        cache_key = 'learning_data_cache'
                        cache_timestamp_key = 'learning_data_timestamp'
                        if cache_key in st.session_state:
                            del st.session_state[cache_key]
                        if cache_timestamp_key in st.session_state:
                            del st.session_state[cache_timestamp_key]
                        st.rerun()
                    else:
                        st.error("❌ レビューの保存に失敗しました")
            
            elif st.session_state[review_key] == "正しい":
                # 正しいとして保存ボタン
                if st.button("✅ 正しいとして保存", key=f"save_correct_{i}", type="primary"):
                    # 正しい仕訳を作成
                    correct_journal = f"仕訳: {result['account']} {result['amount']}円"
                    if result['tax'] != '0':
                        correct_journal += f" (消費税: {result['tax']}円)"
                    correct_journal += f" - {result['description']}"
                    
                    # 元のテキストがない場合は仕訳情報から再構築
                    original_text = result.get('original_text', '')
                    if not original_text:
                        original_text = f"取引先: {result.get('company', 'N/A')}, 日付: {result.get('date', 'N/A')}, 金額: {result.get('amount', 'N/A')}円, 摘要: {result.get('description', 'N/A')}"
                    
                    # レビューを保存（修正なし）
                    if save_review_to_firestore(
                        original_text,
                        correct_journal,
                        correct_journal,  # 修正なしなので同じ
                        reviewer_name,
                        "正しい仕訳として確認"
                    ):
                        st.success("✅ 正しい仕訳として保存しました！")
                        # キャッシュをクリアして学習データを更新
                        cache_key = 'learning_data_cache'
                        cache_timestamp_key = 'learning_data_timestamp'
                        if cache_key in st.session_state:
                            del st.session_state[cache_key]
                        if cache_timestamp_key in st.session_state:
                            del st.session_state[cache_timestamp_key]
                        st.rerun()
                    else:
                        st.error("❌ レビューの保存に失敗しました")

with tab2:
    st.subheader("🚀 バッチ処理モード")
    
    # バッチ処理のUI
    st.write("複数のファイルを一括処理できます。")
    
    # ファイルアップロード
    uploaded_files = st.file_uploader(
        "複数の画像またはPDFをアップロード",
        type=['png', 'jpg', 'jpeg', 'pdf'],
        accept_multiple_files=True,
        help="複数のファイルを選択してください"
    )
    
    if uploaded_files:
        st.write(f"📁 {len(uploaded_files)}個のファイルがアップロードされました")
        
        # 処理設定
        col1, col2 = st.columns(2)
        with col1:
            batch_stance = st.radio(
                "この請求書はどちらの立場ですか?",
                ["受領 (自社が支払う/費用)", "発行 (自社が受け取る/売上)"],
                key="batch_stance"
            )
        
        with col2:
            batch_tax_mode = st.selectbox(
                "消費税区分",
                ["自動判定", "内税", "外税", "非課税"],
                key="batch_tax_mode"
            )
        
        batch_output_format = st.selectbox(
            "出力形式を選択",
            ["汎用CSV", "汎用TXT", "マネーフォワードCSV", "マネーフォワードTXT"],
            key="batch_output_format"
        )
        
        batch_extra_prompt = st.text_area(
            "AIへの追加指示・ヒント",
            placeholder="例: この会社の仕訳は通常、通信費として処理します",
            key="batch_extra_prompt"
        )
        
        # 処理実行ボタン
        if st.button("🚀 バッチ処理を開始", type="primary"):
            if uploaded_files:
                # バッチ処理を実行
                st.write("🔄 バッチ処理を開始します...")
                
                all_results = []
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                for i, uploaded_file in enumerate(uploaded_files):
                    status_text.text(f"処理中: {uploaded_file.name} ({i+1}/{len(uploaded_files)})")
                    
                    try:
                        # ファイルの内容を読み込み
                        file_content = uploaded_file.read()
                        uploaded_file.seek(0)  # ポインタをリセット
                        
                        # ファイルタイプを判定
                        if uploaded_file.type == "application/pdf":
                            # PDF処理
                            text = extract_text_from_pdf(file_content)
                        else:
                            # 画像処理
                            with tempfile.NamedTemporaryFile(delete=False, suffix=f".{uploaded_file.name.split('.')[-1]}") as tmp_file:
                                tmp_file.write(file_content)
                                tmp_file.flush()
                                text = ocr_image_gcv(tmp_file.name)
                                os.unlink(tmp_file.name)
                        
                        if text and is_text_sufficient(text):
                            # 仕訳情報を抽出
                            stance_value = 'received' if '受領' in batch_stance else 'issued'
                            results = extract_multiple_entries(text, stance_value, batch_tax_mode, False, batch_extra_prompt)
                            
                            # ファイル名を追加
                            for result in results:
                                result['filename'] = uploaded_file.name
                            
                            all_results.extend(results)
                            st.success(f"✅ {uploaded_file.name}: {len(results)}件の仕訳を抽出")
                        else:
                            st.warning(f"⚠️ {uploaded_file.name}: テキストが不十分です")
                            
                    except Exception as e:
                        st.error(f"❌ {uploaded_file.name}: 処理エラー - {str(e)}")
                    
                    # プログレスバーを更新
                    progress_bar.progress((i + 1) / len(uploaded_files))
                
                status_text.text("処理完了！")
                
                if all_results:
                    # 結果を表示
                    st.write(f"📊 合計 {len(all_results)}件の仕訳を抽出しました")
                    
                    # CSVファイルを生成
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"batch_processing_{timestamp}"
                    
                    # 出力形式に応じてファイルを生成
                    if "CSV" in batch_output_format:
                        csv_result = generate_csv(all_results, filename, 
                                              'mf' if 'マネーフォワード' in batch_output_format else 'default', 
                                              False)
                        with open(csv_result['path'], 'rb') as f:
                            csv_data = f.read()
                        st.download_button(
                            label="📥 バッチ処理結果をダウンロード (CSV)",
                            data=csv_data,
                            file_name=csv_result['filename'],
                            mime=csv_result['mime_type']
                        )
                    else:
                        txt_result = generate_csv(all_results, filename, 
                                              'mf' if 'マネーフォワード' in batch_output_format else 'default', 
                                              True)
                        with open(txt_result['path'], 'rb') as f:
                            txt_data = f.read()
                        st.download_button(
                            label="📥 バッチ処理結果をダウンロード (TXT)",
                            data=txt_data,
                            file_name=txt_result['filename'],
                            mime=txt_result['mime_type']
                        )
                    
                    # 結果の詳細表示
                    with st.expander("📋 処理結果の詳細"):
                        for result in all_results:
                            st.write(f"**ファイル: {result['filename']}**")
                            st.write(f"取引先: {result.get('company', 'N/A')}")
                            st.write(f"金額: {result.get('amount', 'N/A')}")
                            st.write(f"勘定科目: {result.get('account', 'N/A')}")
                            st.write("---")
                else:
                    st.error("❌ 処理可能な仕訳が見つかりませんでした")
            else:
                st.error("ファイルがアップロードされていません")
    else:
        st.info("📁 複数のファイルをアップロードしてバッチ処理を開始してください")

def process_batch_files(uploaded_files, stance, tax_mode, output_format, extra_prompt):
    """バッチ処理で複数ファイルを処理"""
    st.write("🔄 バッチ処理を開始します...")
    
    all_results = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i, uploaded_file in enumerate(uploaded_files):
        status_text.text(f"処理中: {uploaded_file.name} ({i+1}/{len(uploaded_files)})")
        
        try:
            # ファイルの内容を読み込み
            file_content = uploaded_file.read()
            uploaded_file.seek(0)  # ポインタをリセット
            
            # ファイルタイプを判定
            if uploaded_file.type == "application/pdf":
                # PDF処理
                text = extract_text_from_pdf(file_content)
            else:
                # 画像処理
                with tempfile.NamedTemporaryFile(delete=False, suffix=f".{uploaded_file.name.split('.')[-1]}") as tmp_file:
                    tmp_file.write(file_content)
                    tmp_file.flush()
                    text = ocr_image_gcv(tmp_file.name)
                    os.unlink(tmp_file.name)
            
            if text and is_text_sufficient(text):
                # 仕訳情報を抽出
                stance_value = 'received' if '受領' in stance else 'issued'
                results = extract_multiple_entries(text, stance_value, tax_mode, False, extra_prompt)
                
                # ファイル名を追加
                for result in results:
                    result['filename'] = uploaded_file.name
                
                all_results.extend(results)
                st.success(f"✅ {uploaded_file.name}: {len(results)}件の仕訳を抽出")
            else:
                st.warning(f"⚠️ {uploaded_file.name}: テキストが不十分です")
                
        except Exception as e:
            st.error(f"❌ {uploaded_file.name}: 処理エラー - {str(e)}")
        
        # プログレスバーを更新
        progress_bar.progress((i + 1) / len(uploaded_files))
    
    status_text.text("処理完了！")
    
    if all_results:
        # 結果を表示
        st.write(f"📊 合計 {len(all_results)}件の仕訳を抽出しました")
        
        # CSVファイルを生成
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"batch_processing_{timestamp}"
        
        # 出力形式に応じてファイルを生成
        if "CSV" in output_format:
            csv_result = generate_csv(all_results, filename, 
                                  'mf' if 'マネーフォワード' in output_format else 'default', 
                                  False)
            with open(csv_result['path'], 'rb') as f:
                csv_data = f.read()
            st.download_button(
                label="📥 バッチ処理結果をダウンロード (CSV)",
                data=csv_data,
                file_name=csv_result['filename'],
                mime=csv_result['mime_type']
            )
        else:
            txt_result = generate_csv(all_results, filename, 
                                  'mf' if 'マネーフォワード' in output_format else 'default', 
                                  True)
            with open(txt_result['path'], 'rb') as f:
                txt_data = f.read()
            st.download_button(
                label="📥 バッチ処理結果をダウンロード (TXT)",
                data=txt_data,
                file_name=txt_result['filename'],
                mime=txt_result['mime_type']
            )
        
        # 結果の詳細表示
        with st.expander("📋 処理結果の詳細"):
            for result in all_results:
                st.write(f"**ファイル: {result['filename']}**")
                st.write(f"取引先: {result.get('company', 'N/A')}")
                st.write(f"金額: {result.get('amount', 'N/A')}")
                st.write(f"勘定科目: {result.get('account', 'N/A')}")
                st.write("---")
    else:
        st.error("❌ 処理可能な仕訳が見つかりませんでした")

def batch_processing_ui():
    """バッチ処理UIのプレースホルダー関数"""
    st.info("バッチ処理機能は現在開発中です。")

# ベクトル検索機能の実装
def initialize_vector_model():
    """ベクトル検索用のモデルを初期化"""
    if not VECTOR_SEARCH_AVAILABLE:
        return None
    
    try:
        # 日本語対応のSentence Transformerモデルを使用
        model = SentenceTransformer('sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2')
        return model
    except Exception as e:
        st.error(f"ベクトル検索モデルの初期化に失敗しました: {e}")
        return None

def create_text_embeddings(texts, model):
    """テキストの埋め込みベクトルを生成"""
    if not VECTOR_SEARCH_AVAILABLE or model is None:
        return None
    
    try:
        embeddings = model.encode(texts, show_progress_bar=False)
        return embeddings
    except Exception as e:
        st.error(f"テキストの埋め込み生成に失敗しました: {e}")
        return None

def build_vector_index(reviews, model):
    """レビューデータからベクトルインデックスを構築"""
    if not VECTOR_SEARCH_AVAILABLE or model is None:
        return None
    
    try:
        # レビューテキストを準備
        texts = []
        for review in reviews:
            # 元のテキスト、AI仕訳、修正後仕訳を結合
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
        
        # ベクトル化
        embeddings = create_text_embeddings(texts, model)
        if embeddings is None:
            return None
        
        # FAISSインデックスを構築
        dimension = embeddings.shape[1]
        index = faiss.IndexFlatIP(dimension)  # Inner Product (cosine similarity)
        
        # 正規化してcosine similarityを計算
        faiss.normalize_L2(embeddings)
        index.add(embeddings.astype('float32'))
        
        return {
            'index': index,
            'reviews': reviews,
            'texts': texts,
            'embeddings': embeddings
        }
    except Exception as e:
        st.error(f"ベクトルインデックスの構築に失敗しました: {e}")
        return None

def search_similar_reviews_vector(query_text, vector_index, model, top_k=5, similarity_threshold=0.3):
    """ベクトル検索による類似レビューの検索"""
    if not VECTOR_SEARCH_AVAILABLE or vector_index is None or model is None:
        return []
    
    try:
        # クエリテキストをベクトル化
        query_embedding = model.encode([query_text], show_progress_bar=False)
        faiss.normalize_L2(query_embedding)
        
        # 類似度検索
        similarities, indices = vector_index['index'].search(
            query_embedding.astype('float32'), 
            min(top_k, len(vector_index['reviews']))
        )
        
        # 結果をフィルタリング
        results = []
        for i, (similarity, idx) in enumerate(zip(similarities[0], indices[0])):
            if similarity >= similarity_threshold:
                review = vector_index['reviews'][idx]
                results.append({
                    'review': review,
                    'similarity': float(similarity),
                    'rank': i + 1
                })
        
        return results
    except Exception as e:
        st.error(f"ベクトル検索に失敗しました: {e}")
        return []

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

def hybrid_search_similar_reviews(text, reviews, vector_model=None, top_k=5):
    """ハイブリッド検索（統計的検索 + ベクトル検索）"""
    results = []
    
    # 1. 統計的検索（従来の方法）
    statistical_results = find_similar_reviews_advanced(text, reviews)
    
    # 2. ベクトル検索（利用可能な場合）
    vector_results = []
    if VECTOR_SEARCH_AVAILABLE and vector_model is not None:
        vector_index = build_vector_index(reviews, vector_model)
        if vector_index is not None:
            vector_results = search_similar_reviews_vector(text, vector_index, vector_model, top_k)
    
    # 3. 結果の統合と重複除去
    seen_review_ids = set()
    
    # 統計的検索結果を追加
    for result in statistical_results:
        review_id = result.get('doc_id', '')
        if review_id not in seen_review_ids:
            results.append({
                'review': result,
                'similarity': result.get('similarity', 0.0),
                'search_method': 'statistical',
                'rank': len(results) + 1
            })
            seen_review_ids.add(review_id)
    
    # ベクトル検索結果を追加
    for result in vector_results:
        review_id = result['review'].get('doc_id', '')
        if review_id not in seen_review_ids:
            results.append({
                'review': result['review'],
                'similarity': result['similarity'],
                'search_method': 'vector',
                'rank': len(results) + 1
            })
            seen_review_ids.add(review_id)
    
    # 類似度でソート
    results.sort(key=lambda x: x['similarity'], reverse=True)
    
    return results[:top_k]

def generate_hybrid_learning_prompt(text, similar_reviews):
    """ハイブリッド検索結果から学習プロンプトを生成"""
    if not similar_reviews:
        return ""
    
    prompt_parts = []
    prompt_parts.append("【過去の類似事例（ベクトル検索 + 統計的検索）】")
    
    for i, result in enumerate(similar_reviews):
        review = result['review']
        similarity = result['similarity']
        search_method = result.get('search_method', 'unknown')
        
        # 検索方法のアイコン
        method_icon = "🚀" if search_method == 'vector' else "📊"
        
        prompt_parts.append(f"\n{method_icon} 類似度 {similarity:.2f} - 事例 {i+1}:")
        
        # 元のテキスト（短縮版）
        original_text = review.get('original_text', '')
        if len(original_text) > 100:
            original_text = original_text[:100] + "..."
        prompt_parts.append(f"元のテキスト: {original_text}")
        
        # AI推測と修正
        ai_journal = review.get('ai_journal', '')
        corrected_journal = review.get('corrected_journal', '')
        
        if ai_journal and corrected_journal:
            if ai_journal != corrected_journal:
                prompt_parts.append(f"AI推測: {ai_journal}")
                prompt_parts.append(f"修正後: {corrected_journal}")
                prompt_parts.append("→ 修正が必要でした")
            else:
                prompt_parts.append(f"仕訳: {ai_journal}")
                prompt_parts.append("→ 正しい仕訳でした")
        
        # コメントがあれば追加
        comments = review.get('comments', '')
        if comments:
            prompt_parts.append(f"コメント: {comments}")
    
    prompt_parts.append("\n【学習ポイント】")
    prompt_parts.append("上記の類似事例を参考に、同じような間違いを避けて正確な勘定科目を選択してください。")
    
    return "\n".join(prompt_parts)
