import requests
import logging
from google.cloud import bigquery
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

log_level = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(level=getattr(logging, log_level))
logger = logging.getLogger(__name__)


# 欄位名稱對應及資料型態
STOCK_FIELDS = {
    "stock_code": "str",  # 證券代號
    "stock_name": "str",  # 證券名稱
    "trade_volume": "int",  # 成交股數
    "trade_value": "int",  # 成交金額
    "opening_price": "float",  # 開盤價
    "highest_price": "float",  # 最高價
    "lowest_price": "float",  # 最低價
    "closing_price": "float",  # 收盤價
    "price_change": "float",  # 漲跌價差
    "transaction_count": "int",  # 成交筆數
}


def get_twse_holidays():
    """從 TWSE API 取得休市日資訊"""
    url = "https://openapi.twse.com.tw/v1/holidaySchedule/holidaySchedule"
    headers = {
        "accept": "application/json",
        "If-Modified-Since": "Mon, 26 Jul 1997 05:00:00 GMT",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        holidays_data = response.json()

        # 將民國年轉換為西元年並格式化日期
        holidays = []
        for holiday in holidays_data:
            # 民國年日期格式: YYYMMDD (114 = 2025)
            roc_date = holiday["Date"]
            roc_year = int(roc_date[:3])
            month = roc_date[3:5]
            day = roc_date[5:7]

            # 轉換為西元年 (民國年+1911)
            western_year = roc_year + 1911
            formatted_date = f"{western_year}{month}{day}"

            holidays.append(formatted_date)

        return holidays
    except Exception as e:
        logger.error(f"Error fetching TWSE holidays: {e}")
        return []


def is_trading_day(date_str):
    """判斷是否為交易日"""
    holidays = get_twse_holidays()

    # 檢查日期是否在假日列表中
    if date_str in holidays:
        return False

    # 檢查是否為週末 (週六 = 5, 週日 = 6)
    date_obj = datetime.strptime(date_str, "%Y%m%d")
    if date_obj.weekday() >= 5:  # 週六或週日
        return False

    return True


def filter_existing_data(client, table_id, data_list):
    """
    過濾掉已經存在於BigQuery中的資料

    Args:
        client: BigQuery客戶端
        table_id: 表格ID
        data_list: 要插入的資料列表

    Returns:
        過濾後的資料列表
    """
    filtered_data = data_list.copy()

    for stock_data in data_list:
        date = stock_data["date"]
        stock_code = stock_data["stock_code"]

        # 構建查詢
        query = f"""
        SELECT COUNT(*) as count 
        FROM `{table_id}` 
        WHERE date = '{date}' AND stock_code = '{stock_code}'
        """

        # 執行查詢
        query_job = client.query(query)
        results = query_job.result()

        # 檢查結果
        for row in results:
            if row.count > 0:
                logger.info(
                    f"Data for {stock_code} on {date} already exists. Skipping."
                )
                filtered_data.remove(stock_data)

    return filtered_data


def insert_data_to_bigquery(client, table_id, data_list):
    """
    將資料插入到BigQuery

    Args:
        client: BigQuery客戶端
        table_id: 表格ID
        data_list: 要插入的資料列表

    Returns:
        操作結果訊息
    """
    errors = client.insert_rows_json(table_id, data_list)
    if errors == []:
        logger.info("Data inserted successfully into BigQuery")
        return "Stock data fetched and stored successfully"
    else:
        logger.error("Failed to insert data into BigQuery")
        return "Failed to insert data into BigQuery"


def fetch_twse_stock_data(target_stocks=None):
    """從 TWSE 抓取每日股票收盤價並存入 BigQuery"""
    if target_stocks is None:
        env_stocks = os.getenv("STOCK_CODES")
        target_stocks = [stock.strip() for stock in env_stocks.split(",")]

    # 驗證輸入參數
    if not all(isinstance(stock, str) and stock.isalnum() for stock in target_stocks):
        logger.error("Invalid stock codes provided")
        return "Error: Invalid stock codes"

    date_str = datetime.now().strftime("%Y%m%d")

    # 檢查是否為交易日
    if not is_trading_day(date_str):
        logger.info(f"Date {date_str} is not a trading day. Skipping data fetch.")
        return "Not a trading day, data fetch skipped"

    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL"
    data_to_insert = []

    try:
        # 發送請求
        logger.info(f"Fetching data from TWSE for date: {date_str}")
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        response.raise_for_status()
        json_data = response.json()

        # 檢查資料狀態
        if json_data.get("stat") != "OK" or "data" not in json_data:
            logger.warning(f"No data available: {json_data.get('stat')}")
            return "No data available"

        # 欄位名稱列表（與 API 返回的順序相同）
        field_names = list(STOCK_FIELDS.keys())

        # 解析資料
        for row in json_data["data"]:
            stock_code = row[0]
            if stock_code in target_stocks:
                try:
                    # 建立資料字典
                    stock_data = {
                        "date": datetime.now().strftime("%Y-%m-%d"),
                    }

                    # 處理所有欄位
                    for i, field_name in enumerate(field_names):
                        field_type = STOCK_FIELDS[field_name]
                        raw_value = row[i].replace(",", "")

                        # 根據欄位類型轉換資料
                        try:
                            if field_type == "int":
                                value = int(raw_value)
                            elif field_type == "float":
                                value = float(raw_value)
                            else:  # str 或其他類型
                                value = raw_value
                        except ValueError:
                            logger.warning(f"Invalid value for {field_name}: {row[i]}")
                            value = None

                        stock_data[field_name] = value

                    data_to_insert.append(stock_data)
                    logger.info(f"Parsed {stock_code}: {stock_data['closing_price']}")
                except Exception as e:
                    logger.error(f"Error parsing data for {stock_code}: {e}")

    except requests.RequestException as e:
        logger.error(f"Error fetching TWSE data: {e}")
        return f"Error: {e}"

    # 儲存到 BigQuery
    if data_to_insert:
        try:
            client = bigquery.Client()
            # Use environment variables
            project_id = os.getenv("GCP_PROJECT_ID")
            dataset = os.getenv("BQ_DATASET_ID")
            table = os.getenv("BQ_TABLE_ID")
            table_id = f"{project_id}.{dataset}.{table}"

            # 過濾掉已存在的資料
            filtered_data = filter_existing_data(client, table_id, data_to_insert)

            # 如果沒有新數據需要插入，則直接返回
            if not filtered_data:
                logger.info("No new data to insert")
                return "No new data to insert"

            # 插入資料到 BigQuery
            result = insert_data_to_bigquery(client, table_id, filtered_data)
            return result

        except Exception as e:
            logger.error(f"BigQuery error: {str(e)}")
            return "BigQuery error occurred"

    return "No data to insert"


if __name__ == "__main__":
    fetch_twse_stock_data()
