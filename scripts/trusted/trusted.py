import time
import duckdb
import pathlib
import pandas
from pandas_profiling import ProfileReport
import numpy as np
from scipy import stats
import joblib
import random
from datetime import datetime
import logging

logger = logging.getLogger("Trusted logger")
c_handler = logging.StreamHandler()
c_handler.setFormatter(logging.Formatter(fmt=' %(name)s :: %(levelname)-8s :: %(message)s'))
logger.addHandler(c_handler)
logger.setLevel(logging.DEBUG)

def data_quality(df, name):
    logger.info("Data quality report for " + name)
    profile = ProfileReport(df, title="Profiling Report")
    now = datetime.now().strftime("%m-%d-%Y-%H-%M-%S") 
    profile.to_file(str(pathlib.Path().resolve().parent) + "/scripts/formatted/" + str(now) + "@" + name + ".html")
    logger.info("Data quality report saved in " + str(pathlib.Path().resolve().parent) + "/scripts/formatted/" + str(now) + "@" + name + ".html")
    
def remove_duplicates(name_no_prefix, df, cursor):
    logger.info("Removing duplicates from " + name_no_prefix + " database")
    df_clean = df.drop_duplicates()
    # removing previous table
    cursor.execute(f"DROP TABLE {name_no_prefix}")
    # create new one
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {name_no_prefix} AS SELECT * FROM df_clean")
    logger.info("Done removing duplicates from " + name_no_prefix)


def run():
    sleeping_time = 5
    while True:
        try:
            offset = random.randint(0, 5)
            logger.info("Trying to adquire db lock...")
            conn_formatted = duckdb.connect(database=str(pathlib.Path().resolve().parent) + '/scripts/formatted/my-db.duckdb', read_only=False)
            conn_trusted = duckdb.connect(database=str(pathlib.Path().resolve().parent) + '/scripts/trusted/my-db.duckdb', read_only=False)
            conn_logs = duckdb.connect(database=str(pathlib.Path().resolve().parent) +'/scripts/trusted/logs.duckdb', read_only=False)

            cursor = conn_formatted.cursor()
            cursor2 = conn_trusted.cursor()
            log_cursor = conn_logs.cursor()
            logger.info("Lock adquired!")

            log_cursor.execute("CREATE TABLE IF NOT EXISTS Logs (ID Varchar(255), name Varchar(255))")

            fetch_query = "SHOW TABLES"
            cursor.execute(fetch_query)

            cursor.execute(fetch_query)
            tables = cursor.fetchall()
            for table in tables:
                df = cursor.execute(f"SELECT * FROM {table[0]}").df()
                df_hash = joblib.hash(df)
                log_cursor.execute(f"SELECT * FROM LOGS WHERE Logs.ID = \'{df_hash}\'")
                # check that is a new table based on the hashed value
                df_not_exists = log_cursor.fetchall() == []
                if df_not_exists:
                    logger.info(f"Found a new table with no hash registered: {table[0]}")
                    log_cursor.execute(f"INSERT INTO LOGS VALUES (\'{df_hash}\', \'{table[0]}\')")
                    name_no_prefix = table[0].split("_")[0]
                    cursor2.execute("SHOW TABLES")
                    existing_tables = [x[0] for x in cursor2.fetchall()]
                    
                    if name_no_prefix not in existing_tables:
                        logger.info("Name without prefix not found in db, creating table...")
                        cursor2.execute(f"CREATE TABLE IF NOT EXISTS {name_no_prefix} AS SELECT * FROM df")
                    else:
                        logger.info(f"Inserting values into {name_no_prefix}")
                        cursor2.execute(f"INSERT INTO {name_no_prefix} SELECT * FROM df")
                    
                    # Reporting and deduplicating
                    df = cursor2.execute(f"SELECT * FROM {name_no_prefix}").df()
                    data_quality(df, name_no_prefix)
                    remove_duplicates(name_no_prefix, df, cursor2)
                
            
        except BaseException as error:
            logger.warning("Can't lock the db. Retrying shortly...")
            time.sleep(1)
            
        finally:
            conn_formatted.close()
            conn_trusted.close()
            conn_logs.close()
            logger.info("Lock released")
        time.sleep(sleeping_time + offset)