from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, DataFrame
from pyspark.sql.functions import lit, col, udf
from pyspark.sql.types import IntegerType, StringType
from functools import reduce
import glob
import json
import os
import traceback
import logging
from pathlib import Path
import time as t

csv_paths = glob.glob("data/*/*_lvr_land_*.csv") # 取得所有需要的csv路徑
df_lst = [] # 創建一個用來儲存dataframe的list
digit_lst = {'一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9} # 中文對應阿拉伯數字對照表
city_lst = {'A': '台北市', 'B': '台中市', 'E': '高雄市', 'F': '新北市', 'H': '桃園市'} # 縣市對照表

"""初始化spark相關設定"""
conf = SparkConf().setAppName("PySpark App").setMaster("local") # SparkConf包含了Spark集群配置的各種參數，此處用來初始化SparkConf， master為local意旨為單台電腦運算，非併行。
sc = SparkContext(conf=conf) # 透過SparkConf配置SparkContext

""" Converting function to UDF """
chinese2digitUDF = udf(lambda z: chinese2digit(z), IntegerType()) # 將function-chinese2digit轉成udf格式
transcityUDF = udf(lambda z: trans_city(z), StringType()) # 將function-trans_city轉成udf格式
transyearUDF = udf(lambda z: trans_year(z), StringType()) # 將function-trans_year轉成udf格式

"""log紀錄"""
path = str(Path(os.getcwd()))
Info_Log_path = os.path.join(path, "log/pyspark", t.strftime('%Y%m%d', t.localtime()) + ".log")

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)-4s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers = [logging.FileHandler(Info_Log_path, 'a+', 'utf-8'),])

"""錯誤訊息追蹤"""
def exception_to_string(excp):
   stack = traceback.extract_stack()[:-3] + traceback.extract_tb(excp.__traceback__) 
   pretty = traceback.format_list(stack)
   return ''.join(pretty) + '\n  {} {}'.format(excp.__class__,excp)

"""產生json"""
def generaterJson(dataframe):
    city_temp = ''
    date_temp = ''
    data = {}
    events = []
    result_lst = []
    # 迴圈讀取dataframe
    for row in dataframe:
        city = row['df_name']
        date = row['transaction year month and day']
        type = row['building state']
        district = row['The villages and towns urban district']
           
        if city == city_temp: # 如果此筆縣市與前一筆相同
            if date == date_temp: # 如果此筆交易日期與前一筆相同
                pass
            else: # 如果此筆交易日期與前一筆不同
                data['time_slots'].append({
                    'date': date_temp,
                    'events': events
                })
                events = []
            # 每次type跟district就塞入events
            events.append({
                'type': type,
                'district': district
            })                  
        else: # 如果此筆縣市與前一筆不同
            # 如果是不是第一筆資料，則縣市不同時存取一個大Json
            if city_temp != '':
                result_lst.append(data)
            data = {}
            events = []
            events.append({
                'type': type,
                'district': district
            })
            data['city'] = city
            data['time_slots'] = []
            # data['time_slots'].append({
            #     'date': date,
            #     'events': events
            # })
        city_temp = city # 紀錄這一筆的城市名
        date_temp = date # 紀錄這一筆的日期
        # 如果是最後一筆，直接存取json
        if row == dataframe[-1]: 
            result_lst.append(data)

    return result_lst # 回傳Json list

"""將中文數字樓層轉換為阿拉伯數字"""
def chinese2digit(ch_di):
    # 如果此欄位是空的，當作1層
    if ch_di is None:
        return 1
    ch_di = ch_di.replace('層','').replace('零','') # 將層或零去掉
    # 如果已經是阿拉伯數字，則直接回傳
    if ch_di.isdigit():
        return int(ch_di) 

    # 開始將中文數字轉換成阿拉伯數字
    num = 0
    if ch_di: # 若值不是空的
        idx_h, idx_t = ch_di.find('百'), ch_di.find('十')
        if idx_h != -1:
            num += digit_lst[ch_di[idx_h - 1:idx_h]] * 100
        if idx_t != -1:
            # 十前忽略一的處理
            num += digit_lst.get(ch_di[idx_t - 1:idx_t], 1) * 10
        if ch_di[-1] in digit_lst:
            num += digit_lst[ch_di[-1]]
    return num

"""縣市轉換"""
def trans_city(df_name_value):
    city_id = df_name_value.split('_')[2] # 取得城市代號
    return city_lst[city_id] # 回傳對應的城市中文名

"""民國年轉成西元年"""
def trans_year(trade_year):
    trade_year = str(trade_year) # 先轉換成string
    year = int(trade_year[:-4]) + 1911 # 取年的部分 ex. 1010213 -> 101
    return str(year) + '-' + trade_year[-4:-2] + '-' + trade_year[-2:] # 回傳轉換過後的日期ex . 2012-02-13

"""將dataframe結合在一起"""
def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs) 

"""透過資料夾名稱、檔案名稱產生欄位資料內容"""
def col_val_generate(csv_path):
    folder = csv_path.split('\\')[-2] # 取得資料夾路徑
    file_name = csv_path.split('\\')[-1].split('.')[0] # 去除副檔名
    col_val = folder.replace('S','_') + '_' + file_name[0] + '_' + file_name[-1] # ex 103_1_A_B
    return col_val

if __name__ == '__main__':
    # 迴圈讀取所有csv並存取下來
    try:
        for csv_path in csv_paths:
            if os.stat(csv_path).st_size != 0: # 如果檔案大小不是0就讀取
                data = sc.textFile(csv_path, 50) # 先讀取csv文件，取得相關內容
                firstRow = data.first() # 取得csv文件首行
                data = data.filter(lambda row:row != firstRow) # 設定不要首行     
                sqlContext = SQLContext(sc)
                df = sqlContext.read.csv(data, header=True) # 將csv讀取成spark dataframe格式
                df = df.withColumn('df_name', lit(col_val_generate(csv_path)))
                df_lst.append(df) # 儲存至dataframe的list
                logging.info('成功讀取' + csv_path + '，並寫入dataframe')
            else: # 檔案大小如果是0則讀取下一個檔案
                logging.info(csv_path + '檔案大小為0，將不寫入dataframe')
                continue

        logging.info('已完成全部CSV檔案的讀取，將開始進行ETL操作......')

        unioned_df = unionAll(*df_lst) # 將所有dataframe unionAll
        """使用以下條件【主要用途】為【住家用】, 【建物型態】為【住宅大樓】, 【總樓層數】需【大於等於十三層】"""
        result = unioned_df.filter((col("main use") == '住家用') & \
                (col("building state").like('住宅大樓%')) & \
                (chinese2digitUDF(col("total floor number")) >= 13))\
                .select(transcityUDF(col('df_name')).alias('df_name'), \
                    transyearUDF(col('transaction year month and day')).alias('transaction year month and day'), \
                    col("building state"), \
                    col("The villages and towns urban district"))\
                    .distinct()\
                    .orderBy('df_name', 'transaction year month and day', ascending=False)\
                    .collect()
        result_lst = generaterJson(result) # 將dataframe結果轉換成自訂json格式
        with open('result-part1.json', 'w', encoding='utf8') as f:
            json.dump(result_lst[:2], f, ensure_ascii=False)
            f.close()
        with open('result-part2.json', 'w', encoding='utf8') as f:
            json.dump(result_lst[2:], f, ensure_ascii=False)
            f.close()
        logging.info('已完成全部json的產生，程式完成作業......')
        sc.stop() # SparkContext shutdown
    except Exception as err: # 程式若發生異常，將log記錄下來，並將spark關閉
        logging.info('程式發生異常，原因如下 :　' + exception_to_string(err))
        sc.stop() # SparkContext shutdown
