# Home-python2

## Render 持久化股票清單

`render.yaml` 會為 Web Service 掛載 `/var/data` 持久磁碟，並透過 `STOCK_LIST_FILE` 將股票清單寫入 `/var/data/stock_list.json`。磁碟首次建立且該檔案尚不存在時，應用程式會從部署版本內的 `stock_list.json` 複製初始清單；之後重新部署或重啟時會保留磁碟上的清單。

網頁以 Yahoo Finance 搜尋台灣股票代號或名稱，並可直接查詢，不需要先加入「我的清單」。輸入欄可執行單一或多個代號；「執行清單」則查詢目前顯示的清單。數字代號會分別查詢 Yahoo Finance 的 `.TW`、`.TWO` 符號；市場類別以證交所／櫃買中心名單標示（快取 6 小時），官方名單暫時不可用時依 Yahoo Finance 的交易所後綴顯示。若 Yahoo Finance 查無代號，應用程式會自動從已儲存清單移除；批次查詢仍會繼續查詢其餘有效代號，畫面也會同步移除並顯示提示。加入或手動刪除股票只會修改目前清單，按「儲存清單」才會保留變更；「讀取已儲存」會載入已儲存版本，若目前有未儲存變更會先確認。清單可儲存為空。查詢僅支援 Yahoo Finance 有提供台灣股票符號的項目；行情、法人及融資融券資料仍取決於各資料來源對該股票的支援與供應狀況。

若要保留目前服務中已儲存、但尚未提交至 Git 的清單，請在套用磁碟設定前先將網頁中的清單複製到專案的 `stock_list.json` 並提交。新掛載的磁碟不會自動包含舊執行個體的暫存檔案。

Render Persistent Disk 需要付費 Web Service，且掛載磁碟的服務只能使用單一執行個體。請在 Render Dashboard 確認磁碟與服務方案及費用。

## 使用 MSSQL 儲存股票清單

設定 `MSSQL_CONNECTION_STRING` 環境變數後，網頁的股票清單讀取及儲存會改用 MSSQL；未設定時仍使用 `STOCK_LIST_FILE` 指定的 JSON 檔案。請將連線字串設在服務的環境變數或 Secret 管理中，不要提交到程式碼或版控。可直接使用 Somee 提供的 .NET 格式（`workstation id`、`data source`、`user id`、`pwd`、`initial catalog`）；應用程式會使用 `pymssql` 解析欄位並連線，不需 ODBC Driver。

Render 建置時會從 `requirements.txt` 安裝 `pymssql`；其官方 Linux wheel 包含 FreeTDS，因此不需在 Render 安裝 unixODBC 或 ODBC Driver。Somee 的 `data source` 若包含 `,1434` 等連接埠，程式會自動拆分主機與連接埠。資料庫帳號需有建立資料表及讀寫資料的權限。第一次連線時，應用程式會建立 `dbo.StockList`（股票代號與排序）及 `dbo.StockListState`（初始化標記）；初始化時將 `stock_list.json` 匯入一次。之後資料庫是股票清單的主要來源，清單即使儲存為空也不會在重啟時重新匯入 JSON。
