class Task(object):
    keys = [
        '_id',
        'action',
        'args',
        'backend',
        'host',
        'job_id',
        'kwargs',
        'mercury_id',
        'message',
        'method',
        'port',
        'progress',
        'status',
        'task_id',
        'time_completed',
        'time_started',
        'time_updated',
        'timeout',
        'traceback',
        'ttl_time_completed'
    ]

    def _create_attributes(self, kwargs):
        """
        Doing it this way means I can just pass **task into Task. Live long
        and prosper. (Downside is you lose tab complete in ipython/pycharm)
        :param kwargs:
        :return:
        """
        missing = []
        for key in self.keys:
            if key not in kwargs:
                missing.append(key)

        if missing:
            # This shouldn't ever happen
            raise ValueError('{} are missing. Shame.'.format(
                ','.join(missing)
            ))

        for key in self.keys:
            self.__setattr__(key, kwargs[key])

    def __init__(self, **kwargs):
        """

        :param kwargs:
        """
        self._create_attributes(kwargs)
