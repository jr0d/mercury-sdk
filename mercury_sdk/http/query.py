from mercury_sdk.http.base import InterfaceBase


class QueryInterfaceBase(InterfaceBase):
    """ Used for endpoints that support /query"""

    @staticmethod
    def set_projection(params, projection):
        """

        :param params:
        :param projection:
        :return:
        """
        params.update({'projection': ','.join(projection)})

    def get(self, mercury_id=None, projection=None, params=None,
            extra_headers=None):
        """
        Override for get that add projection argument
        :param mercury_id:
        :param projection:
        :param params:
        :param extra_headers:
        :return:
        """

        params = params or {}

        if projection:
            self.set_projection(params, projection)

        return super(QueryInterfaceBase, self).get(item=mercury_id,
                                                   params=params,
                                                   extra_headers=extra_headers)

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
            self.set_projection(params, projection)

        return self.post(item, data={'query': query}, params=params,
                         extra_headers=extra_headers)
