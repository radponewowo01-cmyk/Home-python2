import unittest
from concurrent.futures import Future
from contextlib import nullcontext
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

    def test_index_does_not_read_stock_list_from_mssql(self):
        with mock.patch.object(
            stock_app,
            "connect_mssql",
            side_effect=AssertionError("首頁不應連線讀取清單"),
        ):
            response = self.client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"let stockList = [];", response.data)

    def test_stock_list_read_requires_mssql_configuration(self):
        with mock.patch.dict(stock_app.os.environ, {}, clear=True):
            response = self.client.get("/api/stock-list")

        self.assertEqual(response.status_code, 503)
        self.assertIn("MSSQL_CONNECTION_STRING", response.json["error"])

    def test_empty_stock_list_save_requires_explicit_confirmation(self):
        with mock.patch.object(stock_app, "persist_stock_list") as persist:
            response = self.client.post(
                "/api/stock-list",
                json={"stocks": []},
            )

        self.assertEqual(response.status_code, 400)
        persist.assert_not_called()

    def test_confirmed_empty_stock_list_save_is_allowed(self):
        with mock.patch.object(stock_app, "persist_stock_list") as persist:
            response = self.client.post(
                "/api/stock-list",
                json={"stocks": [], "clear_list": True},
            )

        self.assertEqual(response.status_code, 200)
        persist.assert_called_once_with([])

    def test_first_mssql_read_returns_empty_list_without_json_seed(self):
        class FakeCursor:
            def __init__(self):
                self.statements = []

            def execute(self, statement, parameters=None):
                self.statements.append(statement)

            def fetchall(self):
                return []

            def close(self):
                pass

        class FakeConnection:
            def __init__(self, cursor):
                self.fake_cursor = cursor
                self.committed = False

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def close(self):
                pass

            def commit(self):
                self.committed = True

            def cursor(self):
                return self.fake_cursor

        cursor = FakeCursor()
        with mock.patch.object(
            stock_app,
            "connect_mssql",
            return_value=FakeConnection(cursor),
        ) as connect:
            stocks = stock_app.read_stock_list()

        self.assertEqual(stocks, [])
        self.assertTrue(connect.return_value.committed)
        self.assertTrue(any("SELECT stock_code" in sql for sql in cursor.statements))
        self.assertFalse(
            any("INSERT INTO dbo.StockList" in sql for sql in cursor.statements)
        )

    def test_mssql_stock_list_save_commits_and_verifies_written_rows(self):
        class FakeCursor:
            def __init__(self):
                self.statements = []
                self.rows = [("3090",)]

            def execute(self, statement, parameters=None):
                self.statements.append((statement, parameters))

            def fetchall(self):
                return self.rows

            def close(self):
                pass

        class FakeConnection:
            def __init__(self, cursor):
                self.fake_cursor = cursor
                self.committed = False

            def cursor(self):
                return self.fake_cursor

            def commit(self):
                self.committed = True

            def close(self):
                pass

        cursor = FakeCursor()
        connection = FakeConnection(cursor)
        with mock.patch.object(
            stock_app,
            "connect_mssql",
            return_value=connection,
        ):
            stock_app.persist_stock_list(["3090"])

        self.assertTrue(connection.committed)
        self.assertTrue(
            any(
                "INSERT INTO dbo.StockList" in statement
                and parameters == ("3090", 0)
                for statement, parameters in cursor.statements
            )
        )
        self.assertTrue(
            any("SELECT stock_code" in statement for statement, _ in cursor.statements)
        )

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

    def test_stock_data_fetch_caps_parallelism_and_reports_progress(self):
        executor_limits = []
        real_executor = tw_stock.ThreadPoolExecutor

        def fetch_category(stock_list, category):
            return {stock: category for stock in stock_list}

        def create_executor(max_workers):
            executor_limits.append(max_workers)
            return real_executor(max_workers=max_workers)

        progress = []
        with (
            mock.patch.object(
                tw_stock,
                "ThreadPoolExecutor",
                side_effect=create_executor,
            ),
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
                ["2330", "2317", "2308", "2454", "2881"],
                progress_callback=lambda completed, total: progress.append(
                    (completed, total)
                ),
            )

        expected_stocks = ["2330", "2317", "2308", "2454", "2881"]
        self.assertEqual(list(institution), expected_stocks)
        self.assertEqual(list(prices), expected_stocks)
        self.assertEqual(list(margin), expected_stocks)
        self.assertEqual(sorted(progress), [(index, 5) for index in range(1, 6)])
        self.assertEqual(executor_limits, [3])

    def test_query_rejects_code_outside_official_markets(self):
        class CapturedThread:
            def __init__(self, target, args, daemon):
                self.target = target
                self.args = args
                self.daemon = daemon

            def start(self):
                self.target(*self.args)

        persisted_stocks = []
        with (
            mock.patch.object(
                stock_app,
                "read_stock_list",
                return_value=["9999", "2330"],
            ),
            mock.patch.object(
                stock_app,
                "persist_stock_list",
                side_effect=lambda stocks: persisted_stocks.extend(stocks),
            ),
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

        self.assertEqual(response.status_code, 202)
        job = stock_app.jobs[response.json["job_id"]]
        self.assertEqual(job["status"], "failed")
        self.assertIn("都找不到", job["error"])
        self.assertEqual(job["unknown_stocks"], ["9999"])
        self.assertEqual(job["removed_stocks"], ["9999"])
        self.assertEqual(persisted_stocks, ["2330"])

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

        persisted_stocks = []
        with (
            mock.patch.object(
                stock_app,
                "read_stock_list",
                return_value=["9999", "2330"],
            ),
            mock.patch.object(
                stock_app,
                "persist_stock_list",
                side_effect=lambda stocks: persisted_stocks.extend(stocks),
            ),
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

        self.assertEqual(response.status_code, 202)
        self.assertEqual(persisted_stocks, ["2330"])
        job = stock_app.jobs[response.json["job_id"]]
        self.assertEqual(job["stocks"], ["2330"])
        self.assertEqual(job["unknown_stocks"], ["9999"])
        self.assertEqual(job["removed_stocks"], ["9999"])

    def test_query_rejects_empty_input_without_loading_saved_list(self):
        with mock.patch.object(
            stock_app,
            "read_stock_list",
            side_effect=AssertionError("空輸入不應自動讀取清單"),
        ):
            response = self.client.post("/api/jobs", json={"stocks": []})

        self.assertEqual(response.status_code, 400)

    def test_deleted_stock_stays_removed_after_loading_saved_list(self):
        saved_stocks = ["2330", "2317"]

        def persist_stocks(stocks):
            saved_stocks[:] = stocks

        with (
            mock.patch.object(
                stock_app,
                "persist_stock_list",
                side_effect=persist_stocks,
            ),
            mock.patch.object(
                stock_app,
                "read_stock_list",
                side_effect=lambda: list(saved_stocks),
            ),
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
