import time

import schedule
from schedule import repeat, every


def job1(name, age=0):
    print("Hi Amir!")


schedule.every(2).seconds.do(job1, 'amir', age=30)

f = schedule.get_jobs()

print(repr(f[0]))
print(str(f[0]))

