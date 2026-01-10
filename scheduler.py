import schedule
import time
from datetime import datetime
import pytz
import sync_service

def sync_job():
    print(f"Running sync at {datetime.now()}")
    result = sync_service.sync_data()
    print(result)

def run_scheduler():
    uae_tz = pytz.timezone('Asia/Dubai')
    schedule.every().day.at("00:00").do(sync_job)
    
    print("Scheduler started. Sync will run daily at 12:00 AM UAE time")
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == '__main__':
    run_scheduler()
