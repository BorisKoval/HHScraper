from HHScraperCore import AddStatsToDB
from HHScraperCore import AddVacanciesToDB
import schedule
import time

schedule.every(3).minutes.do(AddStatsToDB)
schedule.every(3).minutes.do(AddVacanciesToDB)

while True:
    schedule.run_pending()
    time.sleep(1)
