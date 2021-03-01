# Data Engineer 相關題目練習

* airflow/python_operator.py<br>
這支程式是airflow workflow執行的程式

* data folder<br>
存放爬蟲抓下來的檔案

* log folder<br>
spider.py以及spark_etl.py程式執行情況的log會記錄在此資料夾底下

* Config.ini<br>
spider.py讀取的相關參數設定檔

* chromedriver.exe<br>
selenium所需用到的chrome driver驅動程式(88版)

* spider.py<br>
這支程式是透過爬蟲到內政部不動產時價登錄網(http://plvr.land.moi.gov.tw/DownloadOpenData) 抓取後續ETL需要用到的資料

* spark_etl.py<br>
這支程式是透過pyspark進行spark相關操作，並透過ETL將資料轉成Json

* result-part1.json、result-part2.json<br>
透過spark處理ETL之後所產生的json檔
