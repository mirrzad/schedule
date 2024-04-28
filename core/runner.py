import schedule
from schedule import repeat, every


@repeat(every(10).seconds.tag('force'))
def job():
    print("Hi Amir!")


while True:
    schedule.run_pending()
