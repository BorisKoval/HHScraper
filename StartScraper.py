from HHScraperCore import ParseAllData
import schedule
import time

schedule.every(1).minutes.do(ParseAllData)

while True:
    schedule.run_pending()
    time.sleep(1)
