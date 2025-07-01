import streamlit as st
import os
import re
import pandas as pd
from datetime import datetime
import pytesseract
from PIL import Image
import cv2
import numpy as np

# フォルダ準備
def ensure_dirs():
    os.makedirs('input', exist_ok=True)
    os.makedirs('output', exist_ok=True)

ensure_dirs()

# OCRでテキスト抽出
def extract_text_from_image(image_path):
    try:
        # 画像を読み込み
        image = cv2.imread(image_path)
        if image is None:
            return ""
        
        # グレースケール変換
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        
        # ノイズ除去
        denoised = cv2.medianBlur(gray, 3)
        
        # 二値化
        _, binary = cv2.threshold(denoised, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        
        # OCR実行（日本語対応）
        text = pytesseract.image_to_string(binary, lang='jpn+eng')
        
        return text
    except Exception as e:
        st.error(f"OCR処理でエラーが発生しました: {e}")
        return ""

# テキストから情報を抽出
def extract_info_from_text(text):
    info = {
        'company': '',
        'date': '',
        'amount': '',
        'tax': '',
        'description': '',
        'account': ''
    }
    
    lines = text.split('\n')
    
    # 会社名の抽出（最初の行や「株式会社」「有限会社」などのキーワードから）
    for line in lines:
        if any(keyword in line for keyword in ['株式会社', '有限会社', '合同会社', 'Studio', 'Inc', 'Corp']):
            info['company'] = line.strip()
            break
    
    # 日付の抽出
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
                    # 年が2桁の場合、現在の年を基準に推測
                    current_year = datetime.now().year
                    info['date'] = f"{current_year}/{year.zfill(2)}/{month.zfill(2)}"
            break
    
    # 金額の抽出
    amount_patterns = [
        r'合計[：:]\s*¥?([0-9,]+)',
        r'金額[：:]\s*¥?([0-9,]+)',
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
                if 100 <= amount <= 10000000:  # 妥当な金額範囲
                    info['amount'] = str(amount)
                    # 消費税を計算（10%と仮定）
                    info['tax'] = str(int(amount * 0.1))
                    break
    
    # 摘要の抽出
    description_keywords = ['として', '代', '費', '料', '講義', '研修', 'サービス']
    for line in lines:
        if any(keyword in line for keyword in description_keywords):
            info['description'] = line.strip()
            break
    
    # 勘定科目の推測
    if '講義' in text or '研修' in text:
        info['account'] = '研修費'
    elif '交通' in text or 'タクシー' in text:
        info['account'] = '旅費交通費'
    elif '通信' in text or '電話' in text:
        info['account'] = '通信費'
    elif '事務用品' in text or '文具' in text:
        info['account'] = '事務用品費'
    else:
        info['account'] = '雑費'
    
    return info

# CSVファイルを生成
def generate_csv(info_list, output_filename):
    df = pd.DataFrame(info_list)
    df = df[['date', 'account', 'amount', 'tax', 'company', 'description']]
    df.columns = ['取引日', '勘定科目', '金額', '消費税', '取引先', '摘要']
    
    output_path = os.path.join('output', output_filename)
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    return output_path

st.title('領収書・請求書AI仕訳 Webアプリ')

uploaded_files = st.file_uploader('画像またはPDFをアップロード（複数可）', type=['png', 'jpg', 'jpeg'], accept_multiple_files=True)

if uploaded_files:
    for uploaded_file in uploaded_files:
        # input/に保存
        file_path = os.path.join('input', uploaded_file.name)
        with open(file_path, 'wb') as f:
            f.write(uploaded_file.getbuffer())
    st.success(f'{len(uploaded_files)}個のファイルをアップロードしました。')

    if st.button('仕訳CSVを作成'):
        with st.spinner('OCR処理中...'):
            info_list = []
            
            for uploaded_file in uploaded_files:
                file_path = os.path.join('input', uploaded_file.name)
                
                # OCRでテキスト抽出
                text = extract_text_from_image(file_path)
                
                if text:
                    st.text_area(f"抽出されたテキスト ({uploaded_file.name}):", text, height=100)
                    
                    # 情報を抽出
                    info = extract_info_from_text(text)
                    info_list.append(info)
                    
                    st.write(f"**抽出結果 ({uploaded_file.name}):**")
                    st.write(f"- 会社名: {info['company']}")
                    st.write(f"- 日付: {info['date']}")
                    st.write(f"- 金額: {info['amount']}")
                    st.write(f"- 消費税: {info['tax']}")
                    st.write(f"- 摘要: {info['description']}")
                    st.write(f"- 勘定科目: {info['account']}")
                    st.write("---")
                else:
                    st.error(f"{uploaded_file.name} からテキストを抽出できませんでした。")
            
            if info_list:
                # 会社名と日付からファイル名を生成
                first_info = info_list[0]
                company = first_info['company'] if first_info['company'] else 'Unknown'
                date_str = first_info['date'].replace('/', '') if first_info['date'] else datetime.now().strftime('%Y%m%d')
                
                # ファイル名をクリーンアップ
                company_clean = re.sub(r'[^\w\s-]', '', company).strip()
                if not company_clean:
                    company_clean = 'Unknown'
                
                output_filename = f'{company_clean}_{date_str}_output.csv'
                
                # CSVを生成
                output_path = generate_csv(info_list, output_filename)
                
                st.success('仕訳CSVを作成しました。')
                
                # CSVの内容を表示
                df = pd.read_csv(output_path, encoding='utf-8-sig')
                st.write("**生成されたCSV内容:**")
                st.dataframe(df)
                
                # ダウンロードボタン
                with open(output_path, 'rb') as f:
                    st.download_button('CSVをダウンロード', f, file_name=output_filename, mime='text/csv')
            else:
                st.error('有効な情報を抽出できませんでした。') 