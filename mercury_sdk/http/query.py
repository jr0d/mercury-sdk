from mercury_sdk.http.base import check_error, InterfaceBase


class QueryInterfaceBase(InterfaceBase):
    """ Used for endpoints that support /query"""
    @check_error
    def query(self, query, item='/query', projection=None, params=None,
              extra_headers=None):
        """

        :param query:
        :param item:
        :param projection:
        :param params:
        :param extra_headers:
        :return:
        """
        params = params or {}
        if projection:
            params['projection'] = ','.join(projection)

        return self.post(item, data={'query': query}, params=params,
                         extra_headers=extra_headers)
