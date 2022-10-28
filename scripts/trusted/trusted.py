#!/usr/bin/env python
# coding: utf-8

# In[64]:


import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, PatternMatchingEventHandler
import duckdb
import pathlib
import pandas
from pandas_profiling import ProfileReport
import numpy as np
from scipy import stats
import joblib
import random

sleeping_time = 5


def data_quality(df):
    profile = ProfileReport(df, title="Profiling Report")
    profile.to_widgets()
    
def remove_duplicates(name_no_prefix, df, cursor):
    df_clean = df.drop_duplicates()
    # removing previous table
    cursor.execute(f"DROP TABLE {name_no_prefix}")
    # create new one
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {name_no_prefix} AS SELECT * FROM df_clean")


# In[ ]:


while True:
    try:
        offset = random.randint(0, 30)
        conn_formatted = duckdb.connect(database=str(pathlib.Path().resolve().parent) + '/formatted/my-db.duckdb', read_only=False)
        conn_trusted = duckdb.connect(database='my-db.duckdb', read_only=False)
        conn_logs = duckdb.connect(database='logs.duckdb', read_only=False)

        cursor = conn_formatted.cursor()
        cursor2 = conn_trusted.cursor()
        log_cursor = conn_logs.cursor()

        log_cursor.execute("CREATE TABLE IF NOT EXISTS Logs (ID Varchar(255), name Varchar(255))")


        fetch_query = "SHOW TABLES"
        cursor.execute(fetch_query)
        print(cursor.fetchall())

        cursor.execute(fetch_query)
        tables = cursor.fetchall()
        for table in tables:
            df = cursor.execute(f"SELECT * FROM {table[0]}").df()
            df_hash = joblib.hash(df)
            log_cursor.execute(f"SELECT * FROM LOGS WHERE Logs.ID = \'{df_hash}\'")
            # check that is a new table based on the hashed value
            df_not_exists = log_cursor.fetchall() == []
            if df_not_exists:
                print("New table found!")
                log_cursor.execute(f"INSERT INTO LOGS VALUES (\'{df_hash}\', \'{table[0]}\')")
                name_no_prefix = table[0].split("_")[0]
                cursor2.execute("SHOW TABLES")
                existing_tables = [x[0] for x in cursor2.fetchall()]
                
                if name_no_prefix not in existing_tables:
                    print(f"Creating table {name_no_prefix}")
                    cursor2.execute(f"CREATE TABLE IF NOT EXISTS {name_no_prefix} AS SELECT * FROM df")
                else:
                    print("Inserting into table")
                    cursor2.execute(f"INSERT INTO {name_no_prefix} SELECT * FROM df")
                
                # Reporting and deduplicating
                print(f"Report for table {name_no_prefix}")
                df = cursor2.execute(f"SELECT * FROM {name_no_prefix}").df()
                data_quality(df)
                remove_duplicates(name_no_prefix, df, cursor2)
            
        
    except BaseException as error:
        print('An exception occurred: {}'.format(error))
        print("Retrying shortly... Don't worry :D")
        time.sleep(1)
        
    finally:
        conn_formatted.close()
        conn_trusted.close()
        conn_logs.close()
    time.sleep(sleeping_time + offset)


# In[56]:


conn_logs = duckdb.connect(database='logs.duckdb', read_only=False)
df = conn_logs.execute("SELECT * FROM LOGS").df()
conn_logs.close()
df

