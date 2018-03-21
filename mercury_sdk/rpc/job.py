import logging
import time

log = logging.getLogger(__name__)


class RPCException(Exception):
    pass


class Job(object):
    def __init__(self, rpc_client, query, instruction):
        """

        :param rpc_client:
        :param query:
        :param instruction:

        """
        self.rpc_client = rpc_client
        self.query = query
        self.instruction = instruction

        self.job_id = None

    @property
    def started(self):
        """ If a job_id is present, the job has been started"""
        return bool(self.job_id)

    def start(self):
        """ Start the job """
        r = self.rpc_client.post(data={
            'query': self.query,
            'instruction': self.instruction
        })

        if r.get('error'):
            raise RPCException(r['data']['message'])

        self.job_id = r['job_id']

    @property
    def raw(self):
        """ Fetch the job from the server """
        if self.started:
            return self.rpc_client.get(self.job_id)

    @property
    def status(self):
        """ Get the jobs status endpoint """
        if self.started:
            return self.rpc_client.status(self.job_id)

    @property
    def tasks(self):
        """ Get the jobs tasks endpoint """
        if self.started:
            return self.rpc_client.tasks(self.job_id)

    @property
    def is_running(self):
        """ Check if the job is running or not """
        return self.started and not self.status['time_completed']

    def join(self, timeout=None, poll_interval=2):
        """

        :param timeout:
        :param poll_interval:
        :return: Tasks structure
        """
        if self.started:
            started = time.time()
            while True:
                if not self.is_running:
                    return self.status
                if timeout and time.time() - started > timeout:
                    break
                time.sleep(poll_interval)


class SimpleJob(Job):
    """ Creates an instruction in the standard form """
    def __init__(self, rpc_client, query, method, job_args=None,
                 job_kwargs=None):
        """

        :param rpc_client:
        :param query:
        :param method:
        :param job_args:
        :param job_kwargs:
        """
        self.method = method
        self.job_args = job_args or ()
        self.job_kw = job_kwargs or {}

        super(SimpleJob, self).__init__(rpc_client, query, instruction={
            'method': self.method,
            'args': self.job_args,
            'kwargs': self.job_kw
        })


class Preprocessor(Job):
    def __init__(self, rpc_client, query, preprocessor, payload):
        """

        :param rpc_client:
        :param query:
        :param preprocessor:
        :param payload:
        """
        self.preprocessor = preprocessor

        instruction = {'preprocessor': self.preprocessor}
        instruction.update(payload)
        super(Preprocessor, self).__init__(rpc_client, query, instruction)
