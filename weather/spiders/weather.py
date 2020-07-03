import os
import pandas as pd
import scrapy
import time
from urllib.parse import quote,unquote
from weather.utils.crawler_tool import Date
import logging
import lxml
from bs4 import BeautifulSoup

stname=""
county=""
datepicker=""

class WeatherSpider(scrapy.Spider):
    name="weather"
    #allowed_domains=['https://e-service.cwb.gov.tw/HistoryDataQuery/MonthDataController.do?command=viewMain']
    #start_urls=['']
    #def __init__(self):
        
    def start_requests(self):
        df_station = pd.read_csv(r'./weather/utils/sort_stations.csv')
        station_detail  = []
        STNAMES = df_station.iloc[:,1]
        QUOTE_STNAMES  = [quote(quote(i.split()[0])) for i in STNAMES]
        STATION_IDS = df_station.iloc[:,0].tolist()
        COUNTY_NAMES = df_station.iloc[:,2].tolist()
    
        for i,j in enumerate(STATION_IDS):            
            a = [j, QUOTE_STNAMES[i], COUNTY_NAMES[i]]# [IDS, quoted station name, County name]
            station_detail.append(a)
        
        days = Date('2005-01-01','2020-06-12').str_month_range()
        
        for day in days:
            for station in station_detail:
                print("=====================================================================================")
                print(f"站名ID:  {station[0]}")
                print(f"站名:  {unquote(unquote(station[1]))}")
                print(f"日期:  {day}")
                
                url = f'https://e-service.cwb.gov.tw/HistoryDataQuery/MonthDataController.do?command=viewMain&station={station[0]}&stname={station[1]}&datepicker={day}'
                global stname, county, datepicker
                stname = unquote(unquote(station[1]))
                county = station[2]
                datepicker = day
                yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):            
        soup = BeautifulSoup(response.text,'lxml')
        table_rows = soup.select('table')[1].find_all('tr',attrs={'class':'second_tr'})

        col_name = [] 
        i=0
        # 爬欄位名稱 list 0 為中文 1為英文
        for tr in table_rows:
            th = tr.find_all('th')
            row = [tr.text.strip() for tr in th if tr.text.strip()]
            if row and i >= 0:
                col_name.append(row)
            i += 1
        # 爬內容
        table_rows = soup.select('table')[1].find_all('tr')
        result = []
        i = 0
        for tr in table_rows:
            td = tr.find_all('td')
            row = [tr.text.strip() for tr in td if tr.text.strip()]
            if row and i >= 3:
                result.append(row)
            i += 1
        global stname, county, datepicker
        df1 = pd.DataFrame(result, columns=col_name[0])   
        df1.insert(1,"觀測站名", str(unquote(unquote(stname)))) # 先城市 在觀測站名
        df1.insert(1,"縣市", str(county)) # 先城市 在觀測站名
        df1.insert(0, '日期', datepicker)
        # merge 
        month_list = [str(i) for i in df1['日期'].tolist()]
        day_list = [str(j) for j in  df1['觀測時間(day)'].tolist()]

        #print(month_list, day_list)

        final_list= list()
        for i, j in enumerate(day_list):
            a = f"{month_list[i]}-{j}"
            final_list.append(a)             
        df1 = df1.drop(['觀測時間(day)'], axis=1)
        df1 = df1.drop(['日期'], axis=1)
        #col = df1.columns.tolist()
        #col = [col[-1]] + col[:-1]
        #df1 = df1[col]
        df1.insert(0,'時間', final_list)
        
        csv_path = f'./data/{str(county)}.csv'
        if os.path.isfile(csv_path):
            df1.to_csv(csv_path, index=None, header=False, mode='a', encoding='utf-8-sig')
        else:
            df1.to_csv(csv_path, index=None, encoding='utf-8-sig')

























if __name__ == "__main__":
    a = WeatherSpider()
    a.start_requests()
    