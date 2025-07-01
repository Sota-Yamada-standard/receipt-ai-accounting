# 領収書・請求書AI仕訳 Webアプリ

領収書や請求書の画像からAIで仕訳を推測し、会計ソフト用のCSVファイルを自動生成するWebアプリケーションです。

## 機能

- 📷 画像ファイル（PNG、JPG、JPEG）のアップロード
- 🔍 OCRによるテキスト抽出（日本語対応）
- 🤖 AIによる仕訳情報の自動推測
  - 会社名
  - 取引日
  - 金額
  - 消費税
  - 摘要
  - 勘定科目
- 📊 会計ソフト用CSVファイルの自動生成
- 💾 ファイル名：`会社名_日付_output.csv`形式

## セットアップ

### 前提条件

- Python 3.8以上
- macOS（Homebrew使用）

### 1. 依存関係のインストール

```bash
# Tesseract（OCRエンジン）のインストール
brew install tesseract
brew install tesseract-lang

# Pythonパッケージのインストール
pip install -r requirements.txt
```

### 2. アプリの起動

```bash
streamlit run app.py
```

ブラウザで `http://localhost:8501` にアクセスしてアプリを使用できます。

## 使い方

1. **ファイルアップロード**
   - 「画像またはPDFをアップロード」ボタンをクリック
   - 領収書・請求書の画像ファイルを選択（複数可）

2. **仕訳CSV作成**
   - 「仕訳CSVを作成」ボタンをクリック
   - OCR処理が実行され、抽出されたテキストと推測結果が表示されます

3. **CSVダウンロード**
   - 生成されたCSVファイルをダウンロード
   - ファイル名は `会社名_日付_output.csv` 形式

## 対応する勘定科目

- 研修費（講義、研修関連）
- 旅費交通費（交通、タクシー関連）
- 通信費（通信、電話関連）
- 事務用品費（事務用品、文具関連）
- 雑費（その他）

## 注意事項

- 画像の品質が良いほど、OCRの精度が向上します
- 手書き文字の認識精度は印刷文字より低くなります
- 消費税は10%で計算されます（軽減税率には対応していません）

## トラブルシューティング

### OCRが動作しない場合

```bash
# Tesseractのインストール確認
tesseract --version

# 日本語言語パックの確認
tesseract --list-langs
```

### パッケージのインストールエラー

```bash
# パッケージの再インストール
pip install --upgrade pip
pip install -r requirements.txt --force-reinstall
```

## ライセンス

このプロジェクトはMITライセンスの下で公開されています。 