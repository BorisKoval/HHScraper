from HHScraperCore import ParseAllData
import schedule
import time

schedule.every(30).minutes.do(ParseAllData)

while True:
    schedule.run_pending()
    time.sleep(1)
