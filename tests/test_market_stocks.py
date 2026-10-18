import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import app as stock_app


class MarketStockTests(unittest.TestCase):
    def setUp(self):
        stock_app.app.config.update(TESTING=True)
        stock_app.jobs.clear()
        stock_app.market_stock_cache.update({"expires_at": 0, "stocks": []})
        self.client = stock_app.app.test_client()

    def test_search_yahoo_stocks_keeps_taiwan_equities(self):
        response = mock.Mock()
        response.json.return_value = {
            "quotes": [
                {
                    "symbol": "2330.TW",
                    "quoteType": "EQUITY",
                    "shortname": "台積電",
                },
                {
                    "symbol": "6488.TWO",
                    "quoteType": "EQUITY",
                    "shortname": "環球晶",
                },
                {
                    "symbol": "2330.HK",
                    "quoteType": "EQUITY",
                    "shortname": "Foreign listing",
                },
                {
                    "symbol": "00927.TW",
                    "quoteType": "ETF",
                    "shortname": "Taiwan ETF",
                },
            ],
        }
        with mock.patch.object(stock_app.requests, "get", return_value=response) as get:
            stocks = stock_app.search_yahoo_stocks("2330")

        self.assertEqual(
            stocks,
            [
                {"code": "2330", "name": "台積電", "market": "上市", "symbol": "2330.TW"},
                {"code": "6488", "name": "環球晶", "market": "上櫃／興櫃", "symbol": "6488.TWO"},
            ],
        )
        self.assertEqual(get.call_count, 2)
        self.assertEqual(
            {call.kwargs["params"]["q"] for call in get.call_args_list},
            {"2330.TW", "2330.TWO"},
        )

    def test_market_search_uses_yahoo_finance(self):
        official_stocks = [
            {"code": "2330", "name": "台積電", "market": "上市"},
            {"code": "6488", "name": "環球晶", "market": "興櫃"},
        ]
        yahoo_stocks = [
            {"code": "2330", "name": "Taiwan Semiconductor", "market": "上市", "symbol": "2330.TW"},
        ]
        with (
            mock.patch.object(stock_app, "search_yahoo_stocks", return_value=yahoo_stocks),
            mock.patch.object(stock_app, "get_market_stock_list", return_value=official_stocks),
        ):
            response = self.client.get("/api/market-stocks?q=環球")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["stocks"][0]["code"], "2330")
        self.assertEqual(response.json["stocks"][0]["market"], "上市")

    def test_query_accepts_stock_not_in_saved_list(self):
        yahoo_matches = [[
            {"code": "2330", "name": "Taiwan Semiconductor", "market": "上市", "symbol": "2330.TW"},
        ]]

        class CapturedThread:
            def __init__(self, target, args, daemon):
                self.target = target
                self.args = args
                self.daemon = daemon

            def start(self):
                pass

        executor = mock.MagicMock()
        executor.__enter__.return_value = executor
        executor.map.side_effect = lambda function, values: map(function, values)

        with (
            mock.patch.object(stock_app, "search_yahoo_stocks", side_effect=yahoo_matches),
            mock.patch.object(
                stock_app,
                "get_market_stock_list",
                return_value=[{"code": "2330", "name": "台積電", "market": "上市"}],
            ),
            mock.patch.object(stock_app, "ThreadPoolExecutor", return_value=executor),
            mock.patch.object(stock_app.threading, "Thread", CapturedThread),
        ):
            response = self.client.post("/api/jobs", json={"stocks": ["2330"]})

        self.assertEqual(response.status_code, 202)
        job = stock_app.jobs[next(iter(stock_app.jobs))]
        self.assertEqual(job["stocks"], ["2330"])
        self.assertEqual(job["markets"], {"2330": "上市"})

    def test_query_rejects_code_outside_official_markets(self):
        with mock.patch.object(stock_app, "search_yahoo_stocks", return_value=[]):
            response = self.client.post("/api/jobs", json={"stocks": ["9999"]})

        self.assertEqual(response.status_code, 400)
        self.assertIn("Yahoo Finance 找不到", response.json["error"])

    def test_deleted_stock_stays_removed_after_loading_saved_list(self):
        with tempfile.TemporaryDirectory() as temp_directory:
            stock_list_file = Path(temp_directory) / "stock_list.json"
            stock_list_file.write_text(json.dumps(["2330", "2317"]), encoding="utf-8")
            with (
                mock.patch.object(stock_app, "stock_list_file", stock_list_file),
                mock.patch.object(stock_app, "connect_mssql", return_value=None),
            ):
                delete_response = self.client.post(
                    "/api/stock-list",
                    json={"stocks": ["2317"]},
                )
                read_response = self.client.get("/api/stock-list")

        self.assertEqual(delete_response.status_code, 200)
        self.assertEqual(read_response.status_code, 200)
        self.assertEqual(read_response.json["stocks"], ["2317"])
        self.assertEqual(read_response.headers["Cache-Control"], "no-store")


if __name__ == "__main__":
    unittest.main()
