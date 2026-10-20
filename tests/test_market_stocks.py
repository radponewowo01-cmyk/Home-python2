import json
import tempfile
import threading
import unittest
from concurrent.futures import Future
from contextlib import nullcontext
from pathlib import Path
from unittest import mock

import app as stock_app
import tw_stock


class ImmediateExecutor:
    def submit(self, function, *args):
        future = Future()
        try:
            future.set_result(function(*args))
        except Exception as error:
            future.set_exception(error)
        return future


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

    def test_market_search_falls_back_to_official_list(self):
        official_stock = {
            "code": "3090",
            "name": "日電貿",
            "market": "上市",
        }
        with (
            mock.patch.object(stock_app, "search_yahoo_stocks", return_value=[]),
            mock.patch.object(
                stock_app,
                "get_market_stock_list",
                return_value=[official_stock],
            ),
        ):
            response = self.client.get("/api/market-stocks?q=3090")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json["stocks"],
            [{**official_stock, "symbol": "3090.TW"}],
        )

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
                self.target(*self.args)

        with (
            mock.patch.object(stock_app, "search_yahoo_stocks", side_effect=yahoo_matches),
            mock.patch.object(
                stock_app,
                "get_market_stock_list",
                return_value=[{"code": "2330", "name": "台積電", "market": "上市"}],
            ),
            mock.patch.object(
                stock_app,
                "ThreadPoolExecutor",
                return_value=nullcontext(ImmediateExecutor()),
            ),
            mock.patch.object(stock_app.threading, "Thread", CapturedThread),
            mock.patch.object(
                stock_app,
                "fetch_stock_data",
                return_value=({}, {}, {}),
            ),
        ):
            response = self.client.post("/api/jobs", json={"stocks": ["2330"]})

        self.assertEqual(response.status_code, 202)
        job = stock_app.jobs[response.json["job_id"]]
        self.assertEqual(job["stocks"], ["2330"])
        self.assertEqual(job["markets"], {"2330": "上市"})

    def test_query_is_enqueued_before_yahoo_validation(self):
        class CapturedThread:
            def __init__(self, target, args, daemon):
                self.target = target
                self.args = args
                self.daemon = daemon

            def start(self):
                pass

        with (
            mock.patch.object(stock_app, "search_yahoo_stocks", side_effect=AssertionError),
            mock.patch.object(stock_app.threading, "Thread", CapturedThread),
        ):
            response = self.client.post("/api/jobs", json={"stocks": ["2330"]})

        self.assertEqual(response.status_code, 202)
        job = stock_app.jobs[response.json["job_id"]]
        self.assertEqual(job["status"], "queued")
        self.assertEqual(job["stage"], "validating")

    def test_stock_data_fetch_runs_in_parallel_and_reports_progress(self):
        barrier = threading.Barrier(3)

        def fetch_category(stock_list, category):
            barrier.wait(timeout=2)
            return {stock: category for stock in stock_list}

        progress = []
        with (
            mock.patch.object(
                tw_stock,
                "fetch_institution_data",
                side_effect=lambda stocks: fetch_category(stocks, "institution"),
            ),
            mock.patch.object(
                tw_stock,
                "fetch_price_data",
                side_effect=lambda stocks: fetch_category(stocks, "prices"),
            ),
            mock.patch.object(
                tw_stock,
                "fetch_margin_short_data",
                side_effect=lambda stocks: fetch_category(stocks, "margin"),
            ),
        ):
            institution, prices, margin = tw_stock.fetch_stock_data(
                ["2330", "2317", "2308"],
                progress_callback=lambda completed, total: progress.append(
                    (completed, total)
                ),
            )

        self.assertEqual(list(institution), ["2330", "2317", "2308"])
        self.assertEqual(list(prices), ["2330", "2317", "2308"])
        self.assertEqual(list(margin), ["2330", "2317", "2308"])
        self.assertEqual(sorted(progress), [(1, 3), (2, 3), (3, 3)])

    def test_query_rejects_code_outside_official_markets(self):
        class CapturedThread:
            def __init__(self, target, args, daemon):
                self.target = target
                self.args = args
                self.daemon = daemon

            def start(self):
                self.target(*self.args)

        with tempfile.TemporaryDirectory() as temp_directory:
            stock_list_file = Path(temp_directory) / "stock_list.json"
            stock_list_file.write_text(json.dumps(["9999", "2330"]), encoding="utf-8")
            with (
                mock.patch.object(stock_app, "stock_list_file", stock_list_file),
                mock.patch.object(stock_app, "connect_mssql", return_value=None),
                mock.patch.object(stock_app, "search_yahoo_stocks", return_value=[]),
                mock.patch.object(
                    stock_app,
                    "get_market_stock_list",
                    return_value=[
                        {"code": "2330", "name": "台積電", "market": "上市"},
                    ],
                ),
                mock.patch.object(
                    stock_app,
                    "ThreadPoolExecutor",
                    return_value=nullcontext(ImmediateExecutor()),
                ),
                mock.patch.object(stock_app.threading, "Thread", CapturedThread),
            ):
                response = self.client.post("/api/jobs", json={"stocks": ["9999"]})
                saved_stocks = json.loads(stock_list_file.read_text(encoding="utf-8"))

        self.assertEqual(response.status_code, 202)
        job = stock_app.jobs[response.json["job_id"]]
        self.assertEqual(job["status"], "failed")
        self.assertIn("都找不到", job["error"])
        self.assertEqual(job["unknown_stocks"], ["9999"])
        self.assertEqual(job["removed_stocks"], ["9999"])
        self.assertEqual(saved_stocks, ["2330"])

    def test_official_market_listing_keeps_stock_when_yahoo_returns_no_match(self):
        class CapturedThread:
            def __init__(self, target, args, daemon):
                self.target = target
                self.args = args
                self.daemon = daemon

            def start(self):
                self.target(*self.args)

        official_stocks = [
            {"code": "6488", "name": "環球晶", "market": "上櫃"},
        ]
        with (
            mock.patch.object(stock_app, "search_yahoo_stocks", return_value=[]),
            mock.patch.object(
                stock_app,
                "get_market_stock_list",
                return_value=official_stocks,
            ),
            mock.patch.object(
                stock_app,
                "ThreadPoolExecutor",
                return_value=nullcontext(ImmediateExecutor()),
            ),
            mock.patch.object(stock_app.threading, "Thread", CapturedThread),
            mock.patch.object(
                stock_app,
                "fetch_stock_data",
                return_value=({}, {}, {}),
            ),
        ):
            response = self.client.post("/api/jobs", json={"stocks": ["6488"]})

        self.assertEqual(response.status_code, 202)
        job = stock_app.jobs[response.json["job_id"]]
        self.assertEqual(job["status"], "completed")
        self.assertEqual(job["stocks"], ["6488"])
        self.assertEqual(job["markets"], {"6488": "上櫃"})
        self.assertEqual(job["unknown_stocks"], [])
        self.assertEqual(job["removed_stocks"], [])

    def test_query_removes_unknown_stock_and_runs_remaining_codes(self):
        class CapturedThread:
            def __init__(self, target, args, daemon):
                self.target = target
                self.args = args
                self.daemon = daemon

            def start(self):
                self.target(*self.args)

        valid_match = {
            "code": "2330",
            "name": "Taiwan Semiconductor",
            "market": "上市",
            "symbol": "2330.TW",
        }

        with tempfile.TemporaryDirectory() as temp_directory:
            stock_list_file = Path(temp_directory) / "stock_list.json"
            stock_list_file.write_text(json.dumps(["9999", "2330"]), encoding="utf-8")
            with (
                mock.patch.object(stock_app, "stock_list_file", stock_list_file),
                mock.patch.object(stock_app, "connect_mssql", return_value=None),
                mock.patch.object(
                    stock_app,
                    "search_yahoo_stocks",
                    side_effect=lambda code: [] if code == "9999" else [valid_match],
                ),
                mock.patch.object(
                    stock_app,
                    "ThreadPoolExecutor",
                    return_value=nullcontext(ImmediateExecutor()),
                ),
                mock.patch.object(
                    stock_app,
                    "get_market_stock_list",
                    return_value=[{"code": "2330", "name": "台積電", "market": "上市"}],
                ),
                mock.patch.object(stock_app.threading, "Thread", CapturedThread),
                mock.patch.object(
                    stock_app,
                    "fetch_stock_data",
                    return_value=({}, {}, {}),
                ),
            ):
                response = self.client.post(
                    "/api/jobs",
                    json={"stocks": ["9999", "2330"]},
                )
                saved_stocks = json.loads(stock_list_file.read_text(encoding="utf-8"))

        self.assertEqual(response.status_code, 202)
        self.assertEqual(saved_stocks, ["2330"])
        job = stock_app.jobs[response.json["job_id"]]
        self.assertEqual(job["stocks"], ["2330"])
        self.assertEqual(job["unknown_stocks"], ["9999"])
        self.assertEqual(job["removed_stocks"], ["9999"])

    def test_query_uses_saved_list_when_input_is_empty(self):
        yahoo_matches = [[
            {"code": "2330", "name": "Taiwan Semiconductor", "market": "上市", "symbol": "2330.TW"},
            {"code": "2317", "name": "鴻海", "market": "上市", "symbol": "2317.TW"},
        ]]

        class CapturedThread:
            def __init__(self, target, args, daemon):
                self.target = target
                self.args = args
                self.daemon = daemon

            def start(self):
                self.target(*self.args)

        with (
            mock.patch.object(stock_app, "read_stock_list", return_value=["2330", "2317"]),
            mock.patch.object(
                stock_app,
                "search_yahoo_stocks",
                side_effect=lambda code: next(
                    matches for matches in yahoo_matches
                    if any(stock["code"] == code for stock in matches)
                ),
            ),
            mock.patch.object(
                stock_app,
                "ThreadPoolExecutor",
                return_value=nullcontext(ImmediateExecutor()),
            ),
            mock.patch.object(
                stock_app,
                "get_market_stock_list",
                return_value=[{"code": "2330", "name": "台積電", "market": "上市"}, {"code": "2317", "name": "鴻海", "market": "上市"}],
            ),
            mock.patch.object(stock_app.threading, "Thread", CapturedThread),
            mock.patch.object(
                stock_app,
                "fetch_stock_data",
                return_value=({}, {}, {}),
            ),
        ):
            response = self.client.post("/api/jobs", json={"stocks": []})

        self.assertEqual(response.status_code, 202)
        job = stock_app.jobs[response.json["job_id"]]
        self.assertEqual(job["stocks"], ["2330", "2317"])
        self.assertEqual(job["markets"], {"2330": "上市", "2317": "上市"})

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
