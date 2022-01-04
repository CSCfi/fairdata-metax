from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
import create_statistic_summary

def start():
    scheduler = BackgroundScheduler()
    scheduler.add_job(create_statistic_summary.create_statistic_summary, 'interval', minutes=1)
    
    scheduler.start()