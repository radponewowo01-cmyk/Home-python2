# Home-python2

## Render 持久化股票清單

`render.yaml` 會為 Web Service 掛載 `/var/data` 持久磁碟，並透過 `STOCK_LIST_FILE` 將股票清單寫入 `/var/data/stock_list.json`。磁碟首次建立且該檔案尚不存在時，應用程式會從部署版本內的 `stock_list.json` 複製初始清單；之後重新部署或重啟時會保留磁碟上的清單。

網頁的「股票清單」區可輸入代號查詢現有清單、新增或刪除股票，也可讀取已儲存的清單；異動後選擇「儲存清單」才會寫入持久化檔案。清單可儲存為空，但執行股票查詢時至少要有一檔股票。

若要保留目前服務中已儲存、但尚未提交至 Git 的清單，請在套用磁碟設定前先將網頁中的清單複製到專案的 `stock_list.json` 並提交。新掛載的磁碟不會自動包含舊執行個體的暫存檔案。

Render Persistent Disk 需要付費 Web Service，且掛載磁碟的服務只能使用單一執行個體。請在 Render Dashboard 確認磁碟與服務方案及費用。

## 使用 MSSQL 儲存股票清單

設定 `MSSQL_CONNECTION_STRING` 環境變數後，網頁的股票清單讀取及儲存會改用 MSSQL；未設定時仍使用 `STOCK_LIST_FILE` 指定的 JSON 檔案。請將連線字串設在服務的環境變數或 Secret 管理中，不要提交到程式碼或版控。連線字串格式範例：

```text
Driver={ODBC Driver 18 for SQL Server};Server=tcp:<host>,1433;Database=<database>;Uid=<user>;Pwd=<password>;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;
```

執行環境需安裝 Microsoft SQL Server ODBC Driver 及 `requirements.txt` 中的 `pyodbc`。資料庫帳號需有建立資料表及讀寫資料的權限。第一次連線時，應用程式會建立 `dbo.StockList`（股票代號與排序）及 `dbo.StockListState`（初始化標記）；初始化時將 `stock_list.json` 匯入一次。之後資料庫是股票清單的主要來源，清單即使儲存為空也不會在重啟時重新匯入 JSON。
