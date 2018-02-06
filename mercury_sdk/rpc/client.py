import logging


log = logging.getLogger(__name__)


class RPCException(Exception):
    pass


class RPCClient(object):
    def __init__(self, rpc_interface):
        self.client = rpc_interface

    def create_job(self, query, method, *args, **kwargs):
        r = self.client.post(data={
            'query': query,
            'instruction': {
                'method': method,
                'args': args,
                'kwargs': kwargs
            }
        })

        if r.get('error'):
            raise RPCException(r['data']['message'])

        # Return Job object
        return r['job_id']

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


