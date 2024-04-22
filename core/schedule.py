import logging
import datetime
from functools import partial, update_wrapper


logger = logging.getLogger('schedule')


class ScheduleError(Exception):
    """Basic Schedule Errors"""


class ScheduleValueError(ScheduleError):
    """Improper Value is set for scheduler"""


class IntervalError(ScheduleError):
    """Improper Interval was used by the user"""


class Scheduler:
    def __init__(self):
        self.jobs = []

    def every(self, interval):
        job = Job(interval, self)
        return job

    def run_pending(self):
        runnable_jobs = (job for job in self.jobs if job.should_run)
        for job in sorted(runnable_jobs):
            self._run_job(job)

    def _run_job(self, job):
        job.run()


class Job:
    def __init__(self, interval, scheduler=None):
        self.interval = interval
        self.unit = None
        self.job_func = None
        self.period = None
        self.next_run = None
        self.last_run = None
        self.scheduler = scheduler

    def __lt__(self, other):
        return self.next_run < other.next_run

    @property
    def second(self):
        if self.interval != 1:
            raise IntervalError('Use seconds instead of second')
        return self.seconds

    @property
    def seconds(self):
        self.unit = 'seconds'
        return self

    @property
    def minute(self):
        if self.interval != 1:
            raise IntervalError('Use minutes instead of second')
        return self.minutes

    @property
    def minutes(self):
        self.unit = 'minutes'
        return self

    @property
    def hour(self):
        if self.interval != 1:
            raise IntervalError('Use hours instead of second')
        return self.hours

    @property
    def hours(self):
        self.unit = 'hours'
        return self

    @property
    def day(self):
        if self.interval != 1:
            raise IntervalError('Use days instead of second')
        return self.days

    @property
    def days(self):
        self.unit = 'days'
        return self

    @property
    def week(self):
        if self.interval != 1:
            raise IntervalError('Use weeks instead of second')
        return self.seconds

    @property
    def weeks(self):
        self.unit = 'weeks'
        return self

    def do(self, func, *args, **kwargs):
        self.job_func = partial(func, *args, **kwargs)
        update_wrapper(self.job_func, func)
        self._schedule_next_run()
        self.scheduler.jobs.append(self)
        return self

    def _schedule_next_run(self):
        if self.unit not in ('seconds', 'minutes', 'hours', 'days', 'weeks'):
            raise ScheduleError('Invalid unit for the job')
        self.period = datetime.timedelta(**{self.unit: self.interval})
        self.next_run = datetime.datetime.now() + self.period

    @property
    def should_run(self):
        assert self.next_run is not None, 'must set the next_run value'
        return self.next_run <= datetime.datetime.now()

    def run(self):
        logger.debug(f'job {self} is running')
        output = self.job_func()
        self.last_run = datetime.datetime.now()
        self._schedule_next_run()
        return output


default_scheduler = Scheduler()


def every(interval=1):
    return default_scheduler.every(interval)


def run_pending():
    return default_scheduler.run_pending()
