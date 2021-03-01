#coding:utf-8
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import os
import logging
from pathlib import Path
import time as t
import traceback
import configparser

"""config"""
config = configparser.ConfigParser()
config.read('Config.ini', encoding = 'utf-8') # 讀取Config.ini
years = config.get('Parameters', 'Year').split(',') # 取得年份
seasons = config.get('Parameters', 'Season').split(',') # 取得季
real_estate_citys = config.get('Parameters', 'RealEstateCity').split(',') # 取得不動產縣市
presale_house_citys = config.get('Parameters', 'PresaleHouseCity').split(',') # 取得預售屋縣市

"""log紀錄"""
path = str(Path(os.getcwd()))
Info_Log_path = os.path.join(path, "log/spider", t.strftime('%Y%m%d', t.localtime()) + ".log")
logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)-4s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers = [logging.FileHandler(Info_Log_path, 'a+', 'utf-8'),])


"""設定chrome option"""
chrome_options = Options()
chrome_options.add_argument("--window-size=1920,1080") # 指定網頁視窗大小
chrome_options.add_argument('--disable-gpu')
# chrome_options.add_argument('--headless') # 使用無頭模式
prefs = {'profile.default_content_settings.popups': 0, 'download.default_directory': os.path.abspath(os.getcwd()) + '\\data'} # 指定下載檔案路徑
chrome_options.add_experimental_option('prefs', prefs)

"""使用chrome driver"""
driver = webdriver.Chrome('./chromedriver.exe', chrome_options=chrome_options) # 使用selenium中chrome的操作


"""錯誤訊息追蹤"""
def exception_to_string(excp):
   stack = traceback.extract_stack()[:-3] + traceback.extract_tb(excp.__traceback__) 
   pretty = traceback.format_list(stack)
   return ''.join(pretty) + '\n  {} {}'.format(excp.__class__,excp)

"""主流程"""
def main():
    # enable_download(driver)
    logging.info('開始登入內政部不動產時價登錄網......')  
    url = 'http://plvr.land.moi.gov.tw/DownloadOpenData' # 內政部不動產時價登錄網
    driver.get(url)

    logging.info('開始進行網頁自動化腳本操作......')  
    driver.find_element(By.XPATH, '//a[text()="非本期下載"]').click() # 點選非本期下載
    t.sleep(1)
    # 選擇進階搜尋
    driver.find_element_by_xpath('//font/input[@value="1"]').send_keys(Keys.SPACE)
    t.sleep(1)
    driver.find_element_by_xpath('//font/input[@value="2"]').click()
    t.sleep(1)

    # 透過迴圈勾選不動產買賣的縣市
    for real_estate_city in real_estate_citys:
        driver.find_elements_by_xpath("//font[text()='" + real_estate_city + "']/../..//td")[1].click()
        t.sleep(1)
    # 透過迴圈勾選預售屋買賣的縣市
    for presale_house_city in presale_house_citys:
        driver.find_elements_by_xpath("//font[text()='" + presale_house_city + "']/../..//td")[2].click()
        t.sleep(1)

    Select(driver.find_element_by_id('fileFormatId')).select_by_value('csv') # 下載檔案格式選擇CSV格式

    # 使用迴圈將指定年份指定季的檔案下載下來
    logging.info('開始進行檔案下載......')
    for year in years:
        for season in seasons:
            Select(driver.find_element_by_id('historySeason_id')).select_by_value(year + season) # 下載檔案格式選擇CSV格式
            driver.find_element_by_id('downloadBtnId').click() # 點選下載
            t.sleep(7) # 等待壓縮下載時間七秒
    driver.quit() # 關閉driver
    logging.info('已成功下載所有檔案，程式完成作業......')

if __name__ == '__main__':
    try:
        main()
    except Exception as err:
        logging.info('程式發生異常，原因如下 :　' + exception_to_string(err))        