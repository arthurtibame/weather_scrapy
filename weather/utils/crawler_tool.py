import pandas as pd
from time import sleep


class Date(object):
    ''' 
    傳入日期區間, 可以回傳包含 "日" or 未包含 "日"
        
        -> 包含日: str_day_range
        -> 未包含: str_month_range      
     '''
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date  = end_date
        self.str_day_range = self.__day_range
        self.str_month_range = self.__month_range    

    def __day_range(self):
        '''ex: '2019-01-01' -> 字串型態傳入'''
        
        DAY_RANGE =  pd.date_range(self.start_date, self.end_date).tolist()    
        STR_DAY_RANGE = [i.strftime("%Y-%m-%d") for i in DAY_RANGE]
        
        return STR_DAY_RANGE

    def __month_range(self):
        '''
        ex: '2019-01' -> 字串型態傳入
        dict.fromkeys remove duplicates
        '''
        MONTH_RANGE =  pd.date_range(self.start_date, self.end_date).tolist()    
        STR_MONTH_RANGE = [i.strftime("%Y-%m") for i in MONTH_RANGE]       
        STR_MONTH_RANGE = list(dict.fromkeys(STR_MONTH_RANGE)) 
        return STR_MONTH_RANGE
if __name__ == "__main__":
    a = Date('2019-01-01', '2019-12-31')
    print(a.str_day_range())
            