from landing.landing import run as runLanding
from formatted.formatted import run as runFormatted
from trusted.trusted import run as runTrusted
from exploitation.exploitation import run as runExploitaition
import threading
import logging

logger = logging.getLogger("Launch logger")
c_handler = logging.StreamHandler()
c_handler.setFormatter(logging.Formatter(fmt=' %(name)s :: %(levelname)-8s :: %(message)s'))
logger.addHandler(c_handler)
logger.setLevel(logging.DEBUG)
logger.info("Starting tasks")

t1 = threading.Thread(target = runLanding)
t2 = threading.Thread(target = runFormatted)
t3 = threading.Thread(target = runTrusted)
t4 = threading.Thread(target = runExploitaition)

t1.start()
t2.start()
t3.start()
t4.start()

t1.join()
t2.join()
t3.join()
t4.join()


