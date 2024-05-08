import unittest
import datetime
import schedule
from unittest.mock import Mock
from schedule import every, ScheduleError, ScheduleValueError, IntervalError


def make_mock_job(name=None):
    job = Mock()
    job.__name__ = name or 'job'
    return job


class MockDateTime:
    def __init__(self, year, month, day, hour, minute, second=0):
        self.year = year
        self.month = month
        self.day = day
        self.hour = hour
        self.minute = minute
        self.second = second
        self.original_datetime = None

    def __enter__(self):
        class MockDate(datetime.datetime):
            @classmethod
            def today(cls):
                return cls(self.year, self.month, self.day)

            @classmethod
            def now(cls, tz=None):
                return cls(self.year, self.month, self.day, self.hour, self.minute, self.second)

        self.original_datetime = datetime.datetime
        datetime.datetime = MockDate

    def __exit__(self, exc_type, exc_val, exc_tb):
        datetime.datetime = self.original_datetime


class ScheduleTest(unittest.TestCase):
    def setUp(self):
        schedule.clear()

    def test_time_units(self):
        assert every().seconds.unit == 'seconds'
        assert every().minutes.unit == 'minutes'
        assert every().hours.unit == 'hours'
        assert every().days.unit == 'days'
        assert every().weeks.unit == 'weeks'

        job_instance = schedule.Job(interval=2)

        with self.assertRaises(IntervalError):
            job_instance.second

        with self.assertRaises(IntervalError):
            job_instance.minute

        with self.assertRaises(IntervalError):
            job_instance.hour

        with self.assertRaises(IntervalError):
            job_instance.day

        with self.assertRaises(IntervalError):
            job_instance.week

        with self.assertRaisesRegex(
                IntervalError,
                (
                    r"monday\(\) is for weekly jobs\. Using monday\(\) for every 2 or more weeks is not supported"
                ),
        ):
            job_instance.monday

        with self.assertRaisesRegex(
                IntervalError,
                (
                    r"tuesday\(\) is for weekly jobs\. Using tuesday\(\) for every 2 or more weeks is not supported"
                ),
        ):
            job_instance.tuesday

        with self.assertRaisesRegex(
                IntervalError,
                (
                    r"wednesday\(\) is for weekly jobs\. Using wednesday\(\) for every 2 or more weeks is not supported"
                ),
        ):
            job_instance.wednesday

        with self.assertRaisesRegex(
                IntervalError,
                (
                    r"thursday\(\) is for weekly jobs\. Using thursday\(\) for every 2 or more weeks is not supported"
                ),
        ):
            job_instance.thursday

        with self.assertRaisesRegex(
                IntervalError,
                (
                    r"friday\(\) is for weekly jobs\. Using friday\(\) for every 2 or more weeks is not supported"
                ),
        ):
            job_instance.friday

        with self.assertRaisesRegex(
                IntervalError,
                (
                    r"saturday\(\) is for weekly jobs\. Using saturday\(\) for every 2 or more weeks is not supported"
                ),
        ):
            job_instance.saturday

        with self.assertRaisesRegex(
                IntervalError,
                (
                    r"sunday\(\) is for weekly jobs\. Using sunday\(\) for every 2 or more weeks is not supported"
                ),
        ):
            job_instance.sunday

        job_instance.unit = 'foo'

        self.assertRaises(ScheduleValueError, job_instance.at, "10:00:00")
        self.assertRaises(ScheduleError, job_instance._schedule_next_run)

        job_instance.unit = 'days'
        job_instance.start_day = 2

        self.assertRaises(ScheduleValueError, job_instance._schedule_next_run)

        job_instance.unit = "weeks"
        job_instance.start_day = "foo"
        self.assertRaises(ScheduleValueError, job_instance._schedule_next_run)

        job_instance.unit = 'days'

        self.assertRaises(ScheduleValueError, job_instance.at, "26:00:00")
        self.assertRaises(ScheduleValueError, job_instance.at, "05:65:00")
        self.assertRaises(ScheduleValueError, job_instance.at, "05:05:62")

        self.assertRaises(ScheduleValueError, job_instance.at, "01-26:03")
        self.assertRaises(ScheduleValueError, job_instance.at, "01:66:03")
        self.assertRaises(ScheduleValueError, job_instance.at, "01:08:7")

        job_instance.unit = 'seconds'
        job_instance.at_time = datetime.datetime.now()
        job_instance.start_day = None

        self.assertRaises(ScheduleValueError, job_instance._schedule_next_run)

        job_instance.latest = 1
        job_instance.unit = 'hours'
        job_instance.start_day = None
        self.assertRaises(ScheduleError, job_instance._schedule_next_run)

    def test_next_run_with_tag(self):
        with MockDateTime(2020, 7, 21, 18, 5):
            job1 = every(10).seconds.do(make_mock_job('job1')).tag('tag1')
            job2 = every(2).hours.do(make_mock_job('job2')).tag('tag1', 'tag2')
            job3 = every(10).minutes.do(make_mock_job('job3')).tag('tag1', 'tag3')
            assert schedule.get_next_run('tag1') == job1.next_run
            assert schedule.get_next_run('tag2') == job2.next_run
            assert schedule.get_next_run('tag3') == job3.next_run
            assert schedule.default_scheduler.get_next_run('tag2') == job2.next_run
            assert schedule.get_next_run('tag4') is None


    def test_singular_plural_unit_match(self):
        assert every().seconds.do(make_mock_job('job1')).unit == every().second.do(make_mock_job('job2')).unit
        assert every().minutes.do(make_mock_job('job1')).unit == every().minute.do(make_mock_job('job2')).unit
        assert every().hours.do(make_mock_job('job1')).unit == every().hour.do(make_mock_job('job2')).unit
        assert every().days.do(make_mock_job('job1')).unit == every().day.do(make_mock_job('job2')).unit
        assert every().weeks.do(make_mock_job('job1')).unit == every().week.do(make_mock_job('job2')).unit
