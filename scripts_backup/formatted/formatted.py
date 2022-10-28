import time
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
import duckdb
import pathlib


class FormattedHandler(PatternMatchingEventHandler):
    def __init__(self, query):
        PatternMatchingEventHandler.__init__(
            self,
            patterns=["*.csv"],
            ignore_directories=True,
            case_sensitive=False,
        )
        self.query = query
    
    
    def on_created(self, event):
        print("holi")
        parent_path = str(pathlib.Path().resolve().parent)
        sep = "@"
        filename = event.src_path.split("/")[-1]
        filename_no_sep = filename.split(sep)[1]
        filename_trunc = filename_no_sep.split(".")[0]
        done = False
        while not done:
            try:
                con = duckdb.connect(database = parent_path + '/formatted/my-db.duckdb', read_only=False)
                cursor = con.cursor()
                cursor.execute(self.query.format(filename_trunc), 
                               [event.src_path])
                done = True
                cursor.execute("show tables")
                print(cursor.fetchall())
            except Exception as e:
                print(e)
                time.sleep(2)
            finally:
                cursor.close()
                con.close()


def run():
    parent_path = str(pathlib.Path().resolve().parent)
    path = parent_path + "/landing/persistent/"
    query = "CREATE TABLE {} AS SELECT * FROM read_csv_auto(?, HEADER=TRUE);"

    event_handler = FormattedHandler(query)
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        observer.stop()
    observer.join()