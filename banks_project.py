import requests
import sqlite3
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime

url = "https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks"

log_file = "code_log.txt"
table_list =["Name","MC_USD_Billion"]
table_names = "Largest_banks"
output_path = "Largest_banks_data.csv"
csv_path = "exchange_rate.csv"
db_names = "Banks.db"
conn = sqlite3.connect(db_names)

def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' 
    now = datetime.now() 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ' : ' + message + '\n') 
        
def extract(url,table_list):
    html_page = requests.get(url).text
    data_web = BeautifulSoup(html_page,'html.parser')
    df = pd.DataFrame(columns=table_list)
    tables = data_web.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            mc_usd_billion = float(col[2].text.strip()[:-1])
            names = col[1].text.strip()[:-1]
            data_dict = {"Name":names,"MC_USD_Billion":mc_usd_billion}
            df1 = pd.DataFrame(data_dict,index = [0])
            df = pd.concat([df,df1],ignore_index=True)
    return df

def transform(df,csv_path):
    file_csv = pd.read_csv(csv_path)
    dict = file_csv.set_index('Currency').to_dict()['Rate']
    df['MC_EUR_Billion'] = [np.round(x*dict['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_GBP_Billion']= [np.round(x*dict['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion']= [np.round(x*dict['INR'],2) for x in df['MC_USD_Billion']]
    return df

def load_to_csv(df,output_path):
    df.to_csv(output_path)
def load_to_db(df,conn,table_names):
    df.to_sql(table_names,conn,if_exists = 'replace',index = False)

def run_queries(sql_query,conn):
    print(sql_query)
    sql_query_output = pd.read_sql(sql_query,conn)
    print(sql_query_output)

log_progress('Start Process')
df = extract(url,table_list)
log_progress('Extract Complete')
df = transform(df,csv_path)
log_progress('Transform Complete')
load_to_csv(df,output_path)
log_progress('Load To File .CSV Complete')
conn = sqlite3.connect(db_names)
log_progress('Connect To DataBase Successful')
load_to_db(df,conn,table_names)
log_progress('Load To DataBase Complete')
sql_query = f"SELECT * FROM Largest_banks"
run_queries(sql_query,conn)
sql_query = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_queries(sql_query,conn)
sql_query = f"SELECT Name from Largest_banks LIMIT 5"
run_queries(sql_query,conn)
log_progress("Process Complete")

conn.close()