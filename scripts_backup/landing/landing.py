import time
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
import shutil as sh
from datetime import datetime

class EventHandler(PatternMatchingEventHandler):
    def __init__(self):
        PatternMatchingEventHandler.__init__(
            self,
            patterns=["*.csv"],
            ignore_directories=True,
            case_sensitive=False,
        )    
    
    def on_created(self, event):
        orig_folder = "temporal/"
        dest_folder = "persistent/"
        filename = event.src_path.split("/")[-1]
        filename_trunc = filename.split(".")[0]
        now = datetime.now().strftime("%m-%d-%Y-%H.%M.%S") # dots to be able to create files in windows os
        filename_metadata = str(now) + "@" + filename_trunc + ".csv"
        try:
            sh.move(orig_folder + filename, dest_folder + filename_metadata)
        except BaseException as error:
            print('An exception occurred: {}'.format(error))


def run():      
    orig_folder = "temporal/"
    dest_folder = "persistent/"
    event_handler = EventHandler()
    observer = Observer()
    observer.schedule(event_handler, orig_folder, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        observer.stop()
    observer.join()