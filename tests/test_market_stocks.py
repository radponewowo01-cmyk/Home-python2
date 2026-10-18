import unittest
from unittest import mock

import app as stock_app


class MarketStockTests(unittest.TestCase):
    def setUp(self):
        stock_app.app.config.update(TESTING=True)
        stock_app.jobs.clear()
        stock_app.market_stock_cache.update({"expires_at": 0, "stocks": []})
        self.client = stock_app.app.test_client()

    def test_fetch_market_source_parses_official_fields(self):
        response = mock.Mock()
        response.json.return_value = [{
            "SecuritiesCompanyCode": "6488",
            "CompanyName": "環球晶圓股份有限公司",
            "CompanyAbbreviation": "環球晶",
        }]
        with mock.patch.object(stock_app.requests, "get", return_value=response) as get:
            stocks = stock_app.fetch_market_source(
                "興櫃",
                stock_app.MARKET_STOCK_SOURCES["興櫃"],
            )

        self.assertEqual(stocks, [{"code": "6488", "name": "環球晶", "market": "興櫃"}])
        get.assert_called_once_with(
            "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_R",
            timeout=15,
        )

    def test_market_search_returns_matches_from_official_list(self):
        official_stocks = [
            {"code": "2330", "name": "台積電", "market": "上市"},
            {"code": "6488", "name": "環球晶", "market": "興櫃"},
        ]
        with mock.patch.object(stock_app, "get_market_stock_list", return_value=official_stocks):
            response = self.client.get("/api/market-stocks?q=環球")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["stocks"], [official_stocks[1]])

    def test_query_accepts_stock_not_in_saved_list(self):
        official_stocks = [
            {"code": "2330", "name": "台積電", "market": "上市"},
        ]

        class CapturedThread:
            def __init__(self, target, args, daemon):
                self.target = target
                self.args = args
                self.daemon = daemon

            def start(self):
                pass

        with (
            mock.patch.object(stock_app, "get_market_stock_list", return_value=official_stocks),
            mock.patch.object(stock_app.threading, "Thread", CapturedThread),
        ):
            response = self.client.post("/api/jobs", json={"stocks": ["2330"]})

        self.assertEqual(response.status_code, 202)
        job = stock_app.jobs[next(iter(stock_app.jobs))]
        self.assertEqual(job["stocks"], ["2330"])
        self.assertEqual(job["markets"], {"2330": "上市"})

    def test_query_rejects_code_outside_official_markets(self):
        with mock.patch.object(stock_app, "get_market_stock_list", return_value=[]):
            response = self.client.post("/api/jobs", json={"stocks": ["9999"]})

        self.assertEqual(response.status_code, 400)
        self.assertIn("不在官方上市、上櫃或興櫃名單中", response.json["error"])


if __name__ == "__main__":
    unittest.main()
