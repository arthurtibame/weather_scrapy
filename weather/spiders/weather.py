import pandas as pd
import scrapy
import time
from urllib.parse import quote,unquote
from weather.utils.crawler_tool import Date

class WeatherSpider(scrapy.Spider):
    name="weather"
    allowed_domains=['']
    start_urls=['']

    def parse(self, response):
        df_station = pd.read_csv(r'./weather/utils/sort_stations.csv')
        result = []
        STNAMES = df_station.iloc[:,1]
        QUOTE_STNAMES  = [quote(quote(i.split()[0])) for i in STNAMES]
        STATION_IDS = df_station.iloc[:,0].tolist()
    
        for i,j in enumerate(STATION_IDS):
            a = [j, QUOTE_STNAMES[i]]
            result.append(a)
        def crawler(county, station, stname, datepicker): #stname
    global headers, counter
    dir_path = r"./data"
    if os.path.isdir(dir_path) is False:
        os.mkdir(dir_path)
    county_path = f'./data/{county}'
    if os.path.isdir(county_path) is False:
        os.mkdir(county_path)
    #print(unquote(unquote("%25E8%2587%25BA%25E5%258C%2597")))
    #print(station,unquote(unquote(stname)), datepicker)
    url = f'https://e-service.cwb.gov.tw/HistoryDataQuery/MonthDataController.do?command=viewMain&station={station}&stname={stname}&datepicker={datepicker}'
            
    res = requests_get(url)
    soup = BeautifulSoup(res.text, 'lxml')

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
    
    df1 = pd.DataFrame(result)      
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
    needed_cols = ["時間", "縣市","觀測站名","氣溫(℃)", "最低氣溫(℃)","相對溼度(%)", "風速(m/s)","降水量(mm)"]
    df1 = df1.loc[:,needed_cols]
    df1= data_cleaning(df1, str(county))
    #print(df1)

    
    file_name = f'{county_path}/{unquote(unquote(stname))}.csv'
    
    if counter == 0 and os.path.isfile(file_name) is False:      
        df1.to_csv(file_name, encoding='utf-8-sig', index=None)
        counter+=1
    else:
        df1.to_csv(file_name , encoding='utf-8-sig', mode='a', header=False, index=None)
        counter+=1
