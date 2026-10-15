# Home-python2

## Render 持久化股票清單

`render.yaml` 會為 Web Service 掛載 `/var/data` 持久磁碟，並透過 `STOCK_LIST_FILE` 將股票清單寫入 `/var/data/stock_list.json`。磁碟首次建立且該檔案尚不存在時，應用程式會從部署版本內的 `stock_list.json` 複製初始清單；之後重新部署或重啟時會保留磁碟上的清單。

若要保留目前服務中已儲存、但尚未提交至 Git 的清單，請在套用磁碟設定前先將網頁中的清單複製到專案的 `stock_list.json` 並提交。新掛載的磁碟不會自動包含舊執行個體的暫存檔案。

Render Persistent Disk 需要付費 Web Service，且掛載磁碟的服務只能使用單一執行個體。請在 Render Dashboard 確認磁碟與服務方案及費用。
