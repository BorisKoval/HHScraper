#import apscheduler
from apscheduler.schedulers.background import BackgroundScheduler
import logging

import HHScraperCore

logging.basicConfig()
logging.getLogger('apscheduler').setLevel(logging.DEBUG)

scheduler = BackgroundScheduler()
scheduler.add_job(HHScraperCore.GetStats, 'interval', seconds = 2)
scheduler.start()

scheduler.print_jobs()

#HHScraperCore.GetStats()

