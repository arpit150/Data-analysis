import pandas as pd 
import os
import sys
import psycopg2
from datetime import datetime

df = pd.read_csv(r"E:\console.csv")

df

def connection_pg():
    global conn
  
def match(r):
    city_id = r['City_ID']
    city_nme = r['CITY_NME']
    stt_id = r['stt_id']
    dst_id = r['dst_id']
    stt_nme = r['stt_nme']
    stt_code= stt_nme[-2:]
    table_name = 'raw_table_'+city_nme.replace(' ','_')
    admin_table = stt_code+'_ADDR_ADMIN_R'
	t1= datetime.timestamp()
    sqlQuery = "select mmi_master.gstndata_final('{}','{}','{}','{}','{}','{}','{}','{}')".format('py_output',table_name,dst_id,stt_id,city_nme.lower(),city_id,'mmi_master',admin_table.upper())
    print(sqlQuery)
	
	print(datetime.timestamp()-t1)
    try:
        conn = psycopg2.connect(
        user = "postgres",
        password = "postgres",
        host = "10.1.1.119",
        port='5432',
        database ="mmi_gstn")
        cursor = conn.cursor()
        cursor.execute(sqlQuery)
        conn.commit()
        
    except(Exception,psycopg2.DatabaseError) as error :
        print("data error",error)
        
    finally:
        if(conn):
            cursor.close()
            conn.close()
 
df.apply(lambda row:match(row),axis=1)
