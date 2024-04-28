import logging
import datetime
import time
from collections.abc import Hashable
from functools import partial, update_wrapper


logger = logging.getLogger('schedule')


class ScheduleError(Exception):
    """Basic Schedule Errors"""


class ScheduleValueError(ScheduleError):
    """Improper Value is set for scheduler"""


class IntervalError(ScheduleError):
    """Improper Interval was used by the user"""


class CancelJob:
    """The job unscheduled itself"""


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

    def get_jobs(self, tag=None):
        if tag is None:
            return self.jobs[:]
        else:
            return [job for job in self.jobs if tag in job.tags]

    def get_next_run(self, tag=None):
        if not self.jobs:
            return None
        filtered_jobs = self.get_jobs(tag)
        if not filtered_jobs:
            return None
        return min(filtered_jobs).next_run

    def idle_seconds(self, tag=None):
        if self.get_next_run() is None:
            return None
        return (self.get_next_run(tag) - datetime.datetime.now()).total_seconds()

    def clear(self, tag=None):
        if tag is None:
            logger.debug('Deleting all jobs')
            del self.jobs[:]
        else:
            logger.debug(f'Deleting jobs with tag: {tag}')
            self.jobs[:] = [job for job in self.jobs if tag not in job.tags]

    def run_all(self, delay_seconds):
        logger.debug(f'Running {len(self.jobs)} jobs with {delay_seconds} delay seconds')
        for job in self.jobs[:]:
            self._run_job(job)
            time.sleep(delay_seconds)

    def cancel_job(self, job):
        try:
            logger.debug(f'Canceling job: {str(job)}')
            self.jobs.remove(job)
        except ValueError:
            logger.debug(f'Cancelling not scheduled job: {str(job)}')


class Job:
    def __init__(self, interval, scheduler=None):
        self.interval = interval
        self.unit = None
        self.job_func = None
        self.period = None
        self.next_run = None
        self.last_run = None
        self.cancel_after = None
        self.tags = set()
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

    def tag(self, *tags):
        for tag in tags:
            if not isinstance(tag, Hashable):
                raise TypeError('Tags must be hashable')
        self.tags.update(tags)
        return self

    def until(self, until_time):
        if isinstance(until_time, datetime.datetime):
            self.cancel_after = until_time
        elif isinstance(until_time, datetime.timedelta):
            self.cancel_after = datetime.datetime.now() + until_time
        elif isinstance(until_time, datetime.time):
            self.cancel_after = datetime.datetime.combine(datetime.datetime.now(), until_time)
        elif isinstance(until_time, str):
            cancel_after = self._decode_datetime_str(
                until_time,
                [
                    '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%d %H:%M',
                    '%Y-%m-%d',
                    '%H:%M:%S',
                    '%H:%M',
                ]
            )

            if cancel_after is None:
                raise ScheduleValueError("Improper string format has been entered")
            if '-' not in until_time:
                now = datetime.datetime.now()
                cancel_after = cancel_after.replace(year=now.year, month=now.month, day=now.day)
            self.cancel_after = cancel_after
        else:
            raise TypeError('Improper type has been entered for until()')
        if self.cancel_after < datetime.datetime.now():
            raise ScheduleValueError('Time is already past!')
        return self

    def _decode_datetime_str(self, datetime_str, formats):
        for f in formats:
            try:
                return datetime.datetime.strptime(datetime_str, f)
            except ValueError:
                pass
        return None

    def _is_overdue(self, when):
        if self.cancel_after < when and self.cancel_after is not None:
            return True
        else:
            return False

    def do(self, func, *args, **kwargs):
        if self._is_overdue(datetime.datetime.now()):
            logger.debug(f'Cancelling job {self}')
            return CancelJob

        self.job_func = partial(func, *args, **kwargs)
        update_wrapper(self.job_func, func)
        self._schedule_next_run()
        self.scheduler.jobs.append(self)
        if self._is_overdue(self.next_run):
            logger.debug(f'Cancelling job {self}')
            return CancelJob
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


def get_jobs(tag=None):
    return default_scheduler.get_jobs(tag)


def get_next_run(tag=None):
    return default_scheduler.get_next_run(tag)


def idle_seconds(tag=None):
    return default_scheduler.idle_seconds(tag)


def run_all(delay_seconds=0):
    return default_scheduler.run_all(delay_seconds)


def clear(tag=None):
    return default_scheduler.clear(tag)


def cancel_job(job):
    return default_scheduler.cancel_job(job)


def repeat(job):
    def _wrapper(func, *args, **kwargs):
        job.do(func, *args, **kwargs)
        return func

    return _wrapper
