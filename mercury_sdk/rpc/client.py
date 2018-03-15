import logging

from mercury_sdk.rpc.job import Job

log = logging.getLogger(__name__)


class RPCClient(object):
    def __init__(self, rpc_interface):
        self.client = rpc_interface

    def create_job(self, query, method, *args, **kwargs):
        return Job(self.client, query, method, args, kwargs)

    def preprocessor(self):
        """ Use a preprocessor """
        raise NotImplementedError


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    from mercury_sdk.http.rpc import JobInterfaceBase
    interface = JobInterfaceBase('http://localhost:9005')
    c = RPCClient(interface)
    j = c.create_job({}, 'echo', 'Hello World!')

    print(j)


