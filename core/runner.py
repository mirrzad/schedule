import time
import schedule
from schedule import repeat, every, run_pending


def job1(name, age=0):
    print("Hi Amir!")


schedule.every().day.at("02:29:10").do(job1, 'amir', age=30)


while True:
    run_pending()
