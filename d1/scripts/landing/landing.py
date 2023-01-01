import time
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
import shutil as sh
from datetime import datetime
import logging

logger = logging.getLogger("Landing logger")
c_handler = logging.StreamHandler()
c_handler.setFormatter(logging.Formatter(fmt=' %(name)s :: %(levelname)-8s :: %(message)s'))
logger.addHandler(c_handler)
logger.setLevel(logging.DEBUG)

class EventHandler(PatternMatchingEventHandler):
    def __init__(self):
        PatternMatchingEventHandler.__init__(
            self,
            patterns=["*.csv"],
            ignore_directories=True,
            case_sensitive=False,
        )    
    
    def on_created(self, event):
        logger.info("New element landed in temporal")
        orig_folder = "landing/temporal/"
        dest_folder = "landing/persistent/"
        filename = event.src_path.split("/")[-1]
        filename_trunc = filename.split(".")[0]
        now = datetime.now().strftime("%m-%d-%Y-%H.%M.%S") # dots to be able to create files in windows os
        filename_metadata = str(now) + "@" + filename_trunc + ".csv"
        logger.info("Choosen filename is " + filename_metadata)
        try:
            logger.info("Trying to move it to persistent")
            sh.move(orig_folder + filename, dest_folder + filename_metadata)
        except BaseException as error:
            logger.error("Error moving the file :-(")
            print('An exception occurred: {}'.format(error))
        logger.info("Moved " + filename_metadata + " to persistent")


def run():      
    orig_folder = "landing/temporal/"
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