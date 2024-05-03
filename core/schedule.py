import logging
import datetime
import time
import random
import pytz
import re
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
        ret = job.run()
        if isinstance(ret, CancelJob) or ret is CancelJob:
            self.cancel_job(job)

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
        self.latest = None
        self.at_time = None
        self.at_time_zone = None
        self.start_day = None
        self.cancel_after = None
        self.tags = set()
        self.scheduler = scheduler

    def __lt__(self, other):
        return self.next_run < other.next_run

    def __str__(self):
        if hasattr(self.job_func, '__name__'):
            job_func_name = self.job_func.__name__
        else:
            job_func_name = repr(self.job_func)
        return 'job (interval={}, unit={}, do={}, args={}, kwargs={})'.format(
            self.interval,
            self.unit,
            job_func_name,
            '()' if self.job_func is None else self.job_func.args,
            '{}' if self.job_func is None else self.job_func.keywords,
        )

    def __repr__(self):

        time_stats = f'last_run: {self.last_run}, next_run: {self.next_run}'

        if hasattr(self.job_func, '__name__'):
            job_func_name = self.job_func.__name__
        else:
            job_func_name = repr(self.job_func)

        if self.job_func is not None:
            args = [x for x in self.job_func.args]
            kwargs = ['%s=%s' % (k, v) for k, v in self.job_func.keywords.items()]
            call_repr = job_func_name + '(' + ', '.join(args + kwargs) + ')'
        else:
            call_repr = [None]

        if self.at_time is not None:
            return 'Every %s %s at %s do %s %s' % (
                self.interval,
                self.unit[:-1] if self.interval == 1 else self.unit,
                self.at_time,
                call_repr,
                time_stats
            )

        else:
            fmt = (
                'Every %(interval)s' + ('to %(latest)s' if self.latest is not None else '')
                + ' %(unit)s do %(call_repr)s %(time_stats)s'
            )
            return fmt % dict(
                interval=self.interval,
                latest=self.latest,
                unit=(self.unit[:-1] if self.interval == 1 else self.unit),
                call_repr=call_repr,
                time_stats=time_stats
            )

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

    @property
    def monday(self):
        if self.interval != 1:
            raise IntervalError(
                'monday() is only for weekly jobs. Using monday() for every '
                '2 or more weeks is not supported'
            )
        self.start_day = 'monday'
        return self.weeks

    @property
    def tuesday(self):
        if self.interval != 1:
            raise IntervalError(
                'tuesday() is only for weekly jobs. Using tuesday() for every '
                '2 or more weeks is not supported'
            )
        self.start_day = 'tuesday'
        return self.weeks

    @property
    def wednesday(self):
        if self.interval != 1:
            raise IntervalError(
                'wednesday() is only for weekly jobs. Using wednesday() for every '
                '2 or more weeks is not supported'
            )
        self.start_day = 'wednesday'
        return self.weeks

    @property
    def thursday(self):
        if self.interval != 1:
            raise IntervalError(
                'thursday() is only for weekly jobs. Using thursday() for every '
                '2 or more weeks is not supported'
            )
        self.start_day = 'thursday'
        return self.weeks

    @property
    def friday(self):
        if self.interval != 1:
            raise IntervalError(
                'friday() is only for weekly jobs. Using friday() for every '
                '2 or more weeks is not supported'
            )
        self.start_day = 'friday'
        return self.weeks

    @property
    def saturday(self):
        if self.interval != 1:
            raise IntervalError(
                'saturday() is only for weekly jobs. Using saturday() for every '
                '2 or more weeks is not supported'
            )
        self.start_day = 'saturday'
        return self.weeks

    @property
    def sunday(self):
        if self.interval != 1:
            raise IntervalError(
                'sunday() is only for weekly jobs. Using sunday() for every '
                '2 or more weeks is not supported'
            )
        self.start_day = 'sunday'
        return self.weeks

    def to(self, latest):
        self.latest = latest
        return self

    def at(self, time_str, tz=None):
        if self.unit not in ('days', 'hours', 'minutes') and not self.start_day:
            raise ScheduleValueError('Invalid unit for the job')
        if tz is not None:
            if isinstance(tz, str):
                self.at_time_zone = pytz.timezone(tz)
            elif isinstance(tz, pytz.BaseTzInfo):
                self.at_time_zone = tz
            else:
                raise ScheduleValueError('Timezone must be string or pytz.timezone object')
        if not isinstance(time_str, str):
            raise TypeError('at() parameter must be string')
        if self.unit == 'days' or self.start_day:
            if not re.match(r'^[0-2]\d:[0-5]\d(:[0-5]\d)?$', time_str):
                raise ScheduleValueError('Invalid time format')
        if self.unit == 'hours':
            if not re.match(r'^([0-5]\d)?:[0-5]\d$', time_str):
                raise ScheduleValueError('Invalid time format')
        if self.unit == 'minutes':
            if not re.match(r'^[0-5]\d$', time_str):
                raise ScheduleValueError('Invalid time format')

        time_values = time_str.split(':')

        if len(time_values) == 3:
            hour, minute, second = time_values
        elif len(time_values) == 2 and self.unit == 'minutes':
            hour, minute = 0, 0
            _, second = time_values
        elif len(time_values) == 2 and self.unit == 'hours' and len(time_values[0]):
            hour = 0
            minute, second = time_values
        else:
            hour, minute = time_values
            second = 0

        if self.unit == 'days' or self.start_day:
            hour = int(hour)
            if not (0 <= hour <= 23):
                raise ScheduleValueError('Invalid number of hours')
        elif self.unit == 'hours':
            hour = 0
            minute = int(minute)
            if not (0 <= minute <= 59):
                raise ScheduleValueError('Invalid number of minutes')
        elif self.unit == 'minutes':
            hour = 0
            minute = 0
            second = int(second)
            if not (0 <= second <= 59):
                raise ScheduleValueError('Invalid number of seconds')

        self.at_time = datetime.time(hour=int(hour), minute=int(minute), second=int(second))
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
        if self.cancel_after is not None and self.cancel_after < when:
            return True
        else:
            return False

    def do(self, func, *args, **kwargs):
        self.job_func = partial(func, *args, **kwargs)
        update_wrapper(self.job_func, func)
        self._schedule_next_run()
        self.scheduler.jobs.append(self)
        return self

    def _schedule_next_run(self):
        if self.unit not in ('seconds', 'minutes', 'hours', 'days', 'weeks'):
            raise ScheduleError('Invalid unit for the job')

        if self.latest is not None:
            if self.latest <= self.interval:
                raise ScheduleValueError('Latest must be greater than interval')
            else:
                interval = random.randint(self.interval, self.latest)
        else:
            interval = self.interval
        self.period = datetime.timedelta(**{self.unit: interval})
        self.next_run = datetime.datetime.now() + self.period

        if self.start_day is not None:
            if self.unit != 'weeks':
                raise ScheduleValueError('Unit must be weeks')
            weekdays = ('monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday')
            if self.start_day not in weekdays:
                raise ScheduleValueError('Invalid day of the week. valid days are: {}'.format(weekdays))

            weekday = weekdays.index(self.start_day)
            days_ahead = weekday - self.next_run.weekday()

            if days_ahead <= 0:
                days_ahead += 7

            self.next_run += datetime.timedelta(days_ahead) - self.period

        if self.at_time is not None:
            if self.unit not in ('days', 'hours', 'minutes') and self.start_day is None:
                raise ScheduleValueError('Invalid unit for the time')

            kwargs = {'second': self.at_time.second,
                      'minute': self.at_time.minute,
                      'hour': self.at_time.hour,
                      'microsecond': 0
                      }
            self.next_run = self.next_run.replace(**kwargs)

        if self.at_time_zone is not None:
            self.next_run = self.at_time_zone.localize(self.next_run).astimezone().replace(tzinfo=None)

        if self.start_day is not None and self.at_time is not None:
            if (self.next_run - datetime.datetime.now()).days >= 7:
                self.next_run -= self.period

    @property
    def should_run(self):
        assert self.next_run is not None, 'must set the next_run value'
        return self.next_run <= datetime.datetime.now()

    def run(self):
        if self._is_overdue(datetime.datetime.now()):
            logger.debug(f'Cancelling job {self}')
            return CancelJob
        logger.debug(f'job {self} is running')
        output = self.job_func()
        self.last_run = datetime.datetime.now()
        self._schedule_next_run()
        if self._is_overdue(self.next_run):
            logger.debug(f'Cancelling job {self}')
            return CancelJob
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
