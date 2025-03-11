# 台灣證券交易所股票資料擷取工具

這個專案用於從台灣證券交易所 (TWSE) 擷取每日股票收盤資料，並將其存儲到 Google BigQuery 中，方便後續分析和查詢。

## 功能特點

- 自動從 TWSE 擷取指定股票的每日交易資料
- 檢查是否為交易日，避免在非交易日執行
- 過濾已存在的資料，避免重複插入
- 將資料存儲到 Google BigQuery
- 支援 Docker 容器化部署

## 環境需求

- Python 3.11 或更高版本
- Google Cloud 帳號及 BigQuery 資料集
- 服務帳號金鑰檔案 (用於 BigQuery 認證)

## 安裝與設定

### 使用 uv 建立虛擬環境

[uv](https://github.com/astral-sh/uv) 是一個快速的 Python 套件安裝工具和虛擬環境管理器。以下是使用 uv 設定專案的步驟：

1. 安裝 uv (如果尚未安裝)：

```bash
pip install uv
```

2. 複製專案並進入專案目錄：

```bash
git clone <專案網址>
cd <專案目錄>
```

3. 使用 uv 建立虛擬環境：

```bash
uv venv
```

4. 啟動虛擬環境：

```bash
# 在 Linux/macOS 上
source .venv/bin/activate

# 在 Windows 上
.venv\Scripts\activate
```

5. 使用 uv 安裝依賴套件：

```bash
uv pip install -r requirements.txt
```

### 環境變數設定

複製 `.env.example` 檔案並重命名為 `.env`，然後根據需求修改其中的設定：

```bash
cp .env.example .env
```

主要設定項目：

- `GCP_PROJECT_ID`: Google Cloud 專案 ID
- `GOOGLE_APPLICATION_CREDENTIALS`: GCP 服務帳號金鑰檔案路徑
- `BQ_DATASET_ID`: BigQuery 資料集 ID
- `BQ_TABLE_ID`: BigQuery 表格 ID
- `STOCK_CODES`: 要擷取的股票代碼，以逗號分隔

## 使用方法

### 直接執行

確保已經設定好環境變數和服務帳號金鑰檔案，然後執行：

```bash
python main.py
```

### 使用 ruff 檢查程式碼

[ruff](https://github.com/astral-sh/ruff) 是一個快速的 Python linter 和 formatter。使用以下步驟檢查程式碼：

1. 安裝 ruff (使用 uv)：

```bash
uv pip install ruff
```

2. 執行 ruff 檢查程式碼：

```bash
# 檢查程式碼風格和潛在問題
ruff check .

# 自動修復可修復的問題
ruff check --fix .

# 格式化程式碼
ruff format .
```

## Docker 部署

### 建立 Docker 映像檔

```bash
docker build -it twse-stock-fetcher .
```

### 執行 Docker 容器

```bash
docker run -d \
  --name twse-stock-fetcher \
  -e GCP_PROJECT_ID= \
  -e BQ_DATASET= \
  -e BQ_TABLE= \
  twse-stock-fetcher
```

注意：請確保將服務帳號金鑰檔案掛載到容器中，並設定正確的環境變數。


## 常見問題

- 確保 `GOOGLE_APPLICATION_CREDENTIALS` 環境變數指向有效的服務帳號金鑰檔案
- 檢查服務帳號是否具有適當的 BigQuery 權限
- 確認網路連線可以訪問 TWSE 網站
- 檢查日誌以獲取詳細的錯誤訊息