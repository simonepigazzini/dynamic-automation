from dynauto.credentials import dbhost, dbusr, dbpwd, dbssl, dbport
from dynauto.influxdb_utils import reformat_last, sanity_check
import sys
import enum
from typing import Optional, List, Dict
from copy import deepcopy as dcopy
from datetime import datetime

from influxdb import InfluxDBClient
import urllib3

urllib3.disable_warnings()


class JobStatus(enum.StrEnum):
    """Simple class to encode job statuses."""

    IDLE = (enum.auto(),)
    RUNNING = (enum.auto(),)
    FAILED = (enum.auto(),)
    DONE = (enum.auto(),)

    @classmethod
    def statuses(cls) -> List[str]:
        """Returns the list of available statuses."""
        return cls.__members__.keys()


class JobCtrl:
    """
    Update job status and information to the influxdb. Each combination of
    campaign+block+task-name is called a task.

    The job status is represented by the following table::

       {
           'measurement' : 'job',
           'tags' : {
               'task'     : 'Task name',
               'campaign' : 'Campaign to which the job belongs to',
               'block'    : 'Unique data block identifier',
               'id'       : 'Progressive number starting from 0 that identify the current job within the task'
           },
           'time' : timestamp,
           'fields' : {
               'IDLE' : boolean,
               'RUNNING' : boolean,
               'FAILED' : boolean,
               'DONE' : boolean
           }
       }

    User defined fields can also be appended to the default list by each job.

    :param task: name of the task.
    :param campaign: name of the processing campaign.
    :param block: unique block id.
    :param dbname: database name.
    """

    def __init__(
        self,
        task: str,
        campaign: str,
        block: int,
        dbname: str,
    ):
        """
        Create a new JobCtrl task
        """
        # allow only pre-determined tasks
        if task is None:
            sys.exit("[JobCtrl::init] The task field is mandatory")

        # create point data template 
        self.global_data = {
            "measurement": None,
            "tags": {
                "task": str(task),
                "campaign": str(campaign),
                "block": int(block)
            },
            "time": None,
            # (set all available statuses to 0)
            "fields": {sts: 0 for sts in JobStatus.statuses()},
        }

        self.db = InfluxDBClient(
            host=dbhost,
            port=dbport,
            username=dbusr,
            password=dbpwd,
            ssl=dbssl,
            database=dbname,
            timeout=30_000
        )

        # common tools
        self.match_tags = ""
        for t, v in self.global_data["tags"].items():
            if self.match_tags != "":
                self.match_tags += " AND "
            self.match_tags += "\"{}\" = '{}'".format(t, v)

    def task_exist(self):
        """
        Check if specified task exist already in the db.

        :rtype: bool
        """
        exist = len(
            self.db.query(
                f'SELECT * FROM "job" WHERE {self.match_tags}'
            )
        )

        return exist > 0

    def task_completed(self):
        """
        Check if specified task is complete (i.e. all jobs are marked as done).

        :rtype: bool
        """
        task_sts = reformat_last(
            self.db.query(f'SELECT sum(*) FROM (SELECT last(*) FROM "job" WHERE {self.match_tags} GROUP BY id)')
        )

        # check if the number of done jobs equals the total number of jobs (sum over all fields)
        return task_sts[0]['sum_done'] == sum([n for k,n in task_sts[0].items() if 'sum' in k]) if task_sts else False

    def task_end_time(self):
        """
        Return the task completion time.

        :rtype: datetime, None if task is not completed.
        """
        if not self.task_completed():
            return None

        task_sts = reformat_last(
            self.db.query(f'SELECT last("done") FROM "job" WHERE {self.match_tags}')
        )

        return datetime.strptime(task_sts[0]['time'], "%Y-%m-%dT%H:%M:%SZ")

    def create_task(self,
                    jids: List = None,
                    fields: Optional[List[Dict]] = None,
                    recreate: bool = False) -> bool:
        """
        Set newly submitted jobs to idle state. This method should be called by the submit script.

        :param jids: submitted job IDs, defaults to [].
        :param fields: user specified fields (a list matching one to one the jids).
        :param recreate: ignore existing task and reset all jobs to idle (possibly adding new jobs to the task).
        """
        # check ids
        if not jids:
            sys.exit("[JobCtrl::createTask] Job ids list is empty")

        # check that a task does not already exist for this task+campaign combination
        if self.task_exist() and not recreate:
            sys.exit(
                '[JobCtrl::createTask] Task self.global_data["tags"]["task"] \
                already exist for self.global_data["tags"]["campaign"] campaign'
            )
        # delete previous iteration
        if recreate:
            self.db.query(f'DELETE FROM "job" WHERE {self.match_tags}')

        # insert jobs with status set to idle
        data = [dcopy(self.global_data) for _ in range(len(jids))]
        subtime = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        for i, jid in enumerate(jids):
            data[i]["measurement"] = "job"
            data[i]["time"] = subtime
            data[i]["tags"]["id"] = str(jid)
            data[i]["fields"][JobStatus.IDLE.name] = 1
            if fields:
                for f, v in fields[i].items():
                    data[i]["fields"][f] = v

        # data.append(task_data)
        if not sanity_check(data):
            return False
        return self.db.write_points(data)

    def get_job(self, jid: int, last: bool = False) -> Dict:
        """
        Retrive from the influx db the information of a single job in the current task.

        :param jid: the job id.
        :return: a dictionary containing the job information.
        """
        if last:
            job_query = reformat_last(
                self.db.query(f'SELECT last(*) FROM "job" WHERE {self.match_tags} AND "id" = \'{str(jid)}\''))
        else:
            job_query = self.db.query(
                'SELECT * FROM "job" WHERE %s AND "id"=\'%s\'' % (self.match_tags, str(jid)))

        return job_query

    def get_jobs(self) -> Dict:
        """
        Retrive from the influx db all jobs belonging to the current task.

        :return: a dictionary containing the list of job ids in a given status.
        """
        # query for all declared status
        query_str = ', '.join([f'last("{sts}") AS "{sts}"' for sts in JobStatus.statuses()])
        jobs_query = self.db.query(
            'SELECT %s FROM "job" WHERE %s GROUP BY "id"' % (query_str, self.match_tags)
            )

        jobs = {sts: [] for sts in JobStatus.statuses()}
        for jid, job in jobs_query.items():
            data = next(job)
            for status, v in jobs.items():
                if data[status] > 0:
                    v.append(jid[1]["id"])

        return jobs

    def get_nretries(self, jid: int = None) -> int:
        """
        Return the number of times a job has been resubmitted.

        :param jid: job id within the task
        """
        n = 0
        # the same could be achieved by querying for sum("failed")
        for point in self.get_job(jid=jid).get_points():
            if point['failed']:
                n += 1

        return n

    def set_status(self,
                   jid: int = None,
                   status: JobStatus = JobStatus.IDLE,
                   fields: Optional[Dict] = None) -> bool:
        """
        Set status of job #jid. This methods (or its shortcuts) should be called by the job itself.

        :param jid: job id within the submission, defaults to None.
        :param status: job new status, defaults to JobStatus.idle.
        :param fields: job updated fields. Fields from previous status are
                       preserved if not specified here.
        """
        # check id
        if jid is None:
            sys.exit("[JobCtrl::set_status] Please specify a vaild job id")

        # get last status for this job
        prev_data = reformat_last(
            self.db.query(f'SELECT last(*) FROM "job" WHERE {self.match_tags} AND "id" = \'{str(jid)}\''))

        # check if job already exist in db (should have been injected by createTask)
        if not len(prev_data):
            sys.exit(
                "[JobCtrl::set_status] Job %s not found in %s+%s task. Please submit the task first using JobCtrl::createTask"
                % (
                    str(jid),
                    self.global_data["tags"]["task"],
                    self.global_data["tags"]["campaign"],
                )
            )

        # check status
        if status.name not in JobStatus.statuses():
            sys.exit(
                "[JobCtrl::set_status] Specified status %s is not valid. Valid statuses are: \n\t%s"
                % (status, "\n\t".join(JobStatus.statuses()))
            )

        data = dcopy(self.global_data)
        data["measurement"] = "job"
        data["time"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        data["tags"]["id"] = str(jid)
        data["fields"][status.name] = 1
        if fields:
            prev_data[-1].update(fields)
        for f, v in prev_data[-1].items():
            if f not in data["fields"].keys() and f != "time":
                data["fields"][f] = v

        # check data consistency
        if sum([data["fields"][sts] for sts in JobStatus.statuses()]) != 1:
            sys.exit(f"[JobCtrl::set_status] More then one status is being set for the same job (id={jid})")

        if not sanity_check([data]):
            return False
        return self.db.write_points([data])
        
    def get_idle(self) -> List:
        """
        Get all jobs in the current task in idle state.

        :return: the list of job ids in idle state.
        """
        return self.get_jobs()["IDLE"]

    def get_running(self) -> List:
        """
        Get all jobs in the current task in running state.

        :return: the list of job ids in running state.
        """
        return self.get_jobs()["running"]

    def get_failed(self) -> List:
        """
        Get all jobs in the current task in failed state.

        :return: the list of job ids in failed state.
        """
        return self.get_jobs()["failed"]

    def get_done(self) -> List:
        """
        Get all jobs in the current task in done state.

        :return: the list of job ids in done state.
        """
        return self.get_jobs()["done"]

    def idle(self, jid: int = None, fields: Optional[Dict] = None):
        """
        Set job #jid status to idle.

        :param jid: job id within the submission, defaults to None.
        :param fields: job updated fields. Fields from previous status are
                       preserved if not specified here.
        """
        self.set_status(jid=jid, status=JobStatus.IDLE, fields=fields)

    def running(self, jid: int = None, fields: Optional[Dict] = None):
        """
        Set job #jid status to running.

        :param jid: job id within the submission, defaults to None.
        :param fields: job updated fields. Fields from previous status are
                       preserved if not specified here.
        """
        self.set_status(jid=jid, status=JobStatus.RUNNING, fields=fields)

    def failed(self, jid: int = None, fields: Optional[Dict] = None):
        """
        Set job #jid status to failed.

        :param jid: job id within the submission, defaults to None.
        :param fields: job updated fields. Fields from previous status are
                       preserved if not specified here.
        """
        self.set_status(jid=jid, status=JobStatus.FAILED, fields=fields)

    def done(self, jid: int = None, fields: Optional[Dict] = None):
        """
        Set job #jid status to done.

        :param jid: job id within the submission, defaults to None.
        :param fields: job updated fields. Fields from previous status are
                       preserved if not specified here.
        """
        self.set_status(jid=jid, status=JobStatus.DONE, fields=fields)
