import logging
import time

log = logging.getLogger(__name__)


class RPCException(Exception):
    pass


class Job(object):
    def __init__(self, rpc_client, query, method, job_args, job_kwargs):
        """

        :param rpc_client:
        :param query:
        :param method:
        :param job_args:
        :param job_kwargs:
        """
        self.rpc_client = rpc_client
        self.query = query
        self.method = method
        self.job_args = job_args
        self.job_kw = job_kwargs

        self.job_id = None

    @property
    def started(self):
        """ If a job_id is present, the job has been started"""
        return bool(self.job_id)

    def start(self):
        """ Start the job """
        r = self.rpc_client.post(data={
            'query': self.query,
            'instruction': {
                'method': self.method,
                'args': self.job_args,
                'kwargs': self.job_kw
            }
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


if __name__ == '__main__':
    from mercury_sdk.http.rpc import JobInterfaceBase

    interface = JobInterfaceBase(
        'http://mercury.dcx.rackspace.com:9005')
    j = Job(interface,
            {},
            'echo',
            ('Hello World!',),
            {})
    print(j.started)
    print(j.status)
    j.start()
    from pprint import pprint
    pprint(j.join())
