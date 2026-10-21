import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

import requests
import yfinance as yf
from bs4 import BeautifulSoup


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0 Safari/537.36",
    "Referer": "https://fubon-ebrokerdj.fbs.com.tw/",
}
MAX_CONCURRENT_STOCK_FETCHES = 3


def to_yahoo_symbols(stock_code):
    return [f"{stock_code}.TW", f"{stock_code}.TWO"]


def fetch_institution_data(stock_list):
    today = date.today()
    start_date = f"{today.year}-1-1"
    end_date = today.strftime("%Y-%m-%d")
    all_data = {}

    for stock in stock_list:
        url = (
            "https://fubon-ebrokerdj.fbs.com.tw/z/zc/zcl/zcl.djhtm"
            f"?a={stock}&c={start_date}&d={end_date}"
        )
        print(f"抓取 {stock} 的法人持股明細：{url}")
        try:
            response = requests.get(url, headers=HEADERS, timeout=20)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            target_header = soup.find(
                "td",
                class_="t10",
                string=lambda text: text and "法人持股明細" in text,
            )
            table = target_header.find_parent("table") if target_header else None
            all_data[stock] = [
                [cell.get_text(strip=True) for cell in row.find_all(["td", "th"])]
                for row in table.find_all("tr")
                if row.find_all(["td", "th"])
            ] if table else []
        except requests.RequestException as error:
            print(f"無法取得 {stock} 的資料，錯誤：{error}")
            all_data[stock] = []

    return all_data


def fetch_margin_short_data(stock_list):
    today = date.today()
    start_date = f"{today.year}-1-1"
    end_date = today.strftime("%Y-%m-%d")
    all_data = {}

    for stock in stock_list:
        url = (
            "https://fubon-ebrokerdj.fbs.com.tw/z/zc/zcn/zcn.djhtm"
            f"?a={stock}&c={start_date}&d={end_date}"
        )
        print(f"抓取 {stock} 的融資融券明細：{url}")
        try:
            response = requests.get(url, headers=HEADERS, timeout=20)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            table = soup.find("table", class_="t01")
            table_rows = [
                [cell.get_text(strip=True) for cell in row.find_all(["td", "th"])]
                for row in table.find_all("tr")
            ] if table else []
            first_data_index = next(
                (
                    index
                    for index, row in enumerate(table_rows)
                    if row and re.fullmatch(r"\d{2,3}/\d{1,2}/\d{1,2}", row[0])
                ),
                None,
            )
            if first_data_index is None or first_data_index == 0:
                print(f"無法辨識 {stock} 的融資融券資料表")
                all_data[stock] = []
                continue
            all_data[stock] = [
                table_rows[first_data_index - 1],
                *(
                    row
                    for row in table_rows[first_data_index:]
                    if row and re.fullmatch(r"\d{2,3}/\d{1,2}/\d{1,2}", row[0])
                ),
            ]
        except requests.RequestException as error:
            print(f"無法取得 {stock} 的融資融券資料，錯誤：{error}")
            all_data[stock] = []

    return all_data


def _cell_value(row, column):
    value = row[column]
    if hasattr(value, "iloc"):
        value = value.iloc[0]
    if hasattr(value, "item"):
        value = value.item()
    try:
        if value != value:
            return None
        return value
    except (TypeError, ValueError):
        return None


def fetch_price_data(stock_list):
    all_price_data = {}

    for stock in stock_list:
        stock_symbol = None
        try:
            price_data = None
            for candidate_symbol in to_yahoo_symbols(stock):
                print(f"抓取 {candidate_symbol} 的近 30 日股價")
                try:
                    candidate_data = yf.download(
                        candidate_symbol,
                        period="30d",
                        auto_adjust=False,
                        progress=False,
                        timeout=15,
                    )
                except Exception as error:
                    print(f"無法取得 {candidate_symbol} 的股價資料，錯誤：{error}")
                    continue

                if not candidate_data.empty:
                    stock_symbol = candidate_symbol
                    price_data = candidate_data
                    break

            if price_data is None:
                all_price_data[stock] = []
                continue

            stock_price_data = []
            for date_value, row in price_data.iterrows():
                stock_price_data.append({
                    "日期": str(date_value.date()),
                    "開盤價": _cell_value(row, "Open"),
                    "最高價": _cell_value(row, "High"),
                    "最低價": _cell_value(row, "Low"),
                    "收盤價": _cell_value(row, "Close"),
                    "成交量": _cell_value(row, "Volume"),
                })

            all_price_data[stock] = stock_price_data
        except Exception as error:
            print(f"無法取得 {stock_symbol or stock} 的近 30 日股價資料，錯誤：{error}")
            all_price_data[stock] = []

    return all_price_data


def fetch_stock_data(stock_list, progress_callback=None):
    if not stock_list:
        return {}, {}, {}

    def fetch_one_stock(stock):
        return (
            stock,
            fetch_institution_data([stock])[stock],
            fetch_price_data([stock])[stock],
            fetch_margin_short_data([stock])[stock],
        )

    institution_data = {}
    price_data = {}
    margin_short_data = {}
    with ThreadPoolExecutor(
        max_workers=min(len(stock_list), MAX_CONCURRENT_STOCK_FETCHES)
    ) as executor:
        futures = {
            executor.submit(fetch_one_stock, stock): stock
            for stock in stock_list
        }
        for completed, future in enumerate(as_completed(futures), start=1):
            stock, institution, prices, margin_short = future.result()
            institution_data[stock] = institution
            price_data[stock] = prices
            margin_short_data[stock] = margin_short
            if progress_callback:
                progress_callback(completed, len(stock_list))

    return (
        {stock: institution_data[stock] for stock in stock_list},
        {stock: price_data[stock] for stock in stock_list},
        {stock: margin_short_data[stock] for stock in stock_list},
    )


def main():
    from app import read_stock_list

    stock_list = read_stock_list()

    institution_data, price_data, margin_short_data = fetch_stock_data(stock_list)
    output_file = Path(__file__).with_name("stock.txt")
    price_output_file = Path(__file__).with_name("stock_price_30d.txt")
    margin_short_output_file = Path(__file__).with_name("stock_margin_short.txt")
    output_file.write_text(
        json.dumps(institution_data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    price_output_file.write_text(
        json.dumps(price_data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    margin_short_output_file.write_text(
        json.dumps(margin_short_data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"法人持股明細已存成 {output_file.name}")
    print(f"近 30 日股價已存成 {price_output_file.name}")
    print(f"融資融券明細已存成 {margin_short_output_file.name}")


if __name__ == "__main__":
    main()
