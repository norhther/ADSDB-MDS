#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system('pip install duckdb==0.5.0')
get_ipython().system('pip install watchdog')


# In[11]:


import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, PatternMatchingEventHandler
import duckdb
import pathlib

parent_path = str(pathlib.Path().resolve().parent)
path = parent_path + "/landing/persistent/"
sep = "@"

class EventHandler(PatternMatchingEventHandler):
    def __init__(self, query):
        PatternMatchingEventHandler.__init__(
            self,
            patterns=["*.csv"],
            ignore_directories=True,
            case_sensitive=False,
        )
        self.query = query
    
    
    def on_created(self, event):
        filename = event.src_path.split("/")[-1]
        filename_no_sep = filename.split(sep)[1]
        filename_trunc = filename_no_sep.split(".")[0]
        done = False
        while not done:
            try:
                con = duckdb.connect(database = parent_path + '/formatted/my-db.duckdb', read_only=False)
                cursor = con.cursor()
                cursor.execute(query.format(filename_trunc), 
                               [event.src_path])
                done = True
                cursor.execute("show tables")
                print(cursor.fetchall())
            except Exception as e:
                print(e)
                sleep(2)
            finally:
                cursor.close()
                con.close()

query = "CREATE TABLE {} AS SELECT * FROM read_csv_auto(?, HEADER=TRUE);"
event_handler = EventHandler(query)
observer = Observer()
observer.schedule(event_handler, path, recursive=True)
observer.start()

try:
    while True:
        time.sleep(1)
        
except KeyboardInterrupt:
    observer.stop()
    con.close()
observer.join()


# In[2]:


con = duckdb.connect(database=parent_path + '/formatted/my-db.duckdb', read_only=False)
cursor = con.cursor()
cursor.execute("show tables")
print(cursor.fetchall())
con.close()


# In[ ]:


con = duckdb.connect(database=parent_path + '/formatted/my-db.duckdb', read_only=False)
cursor = con.cursor()
cursor.execute("show tables")
tables = [x[0] for x in cursor.fetchall()]

for table in tables:
    cursor.execute(f"Drop table {table}")

