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

    @staticmethod
    def strip_empty(items):
        for item in items:
            for key in item:
                if isinstance(item[key], list):
                    item[key] = [el for el in item[key] if el]

    # TODO: Add pagenation support
    def query(self, query, item='/query', projection=None, params=None,
              extra_headers=None, strip_empty_elements=False):
        """

        :param query:
        :param item:
        :param projection:
        :param params:
        :param extra_headers:
        :param strip_empty_elements:
        :return:
        """

        params = params or {}

        if projection:
            self.set_projection(params, projection)

        data = self.post(item, data={'query': query}, params=params,
                         extra_headers=extra_headers)

        if strip_empty_elements:
            self.strip_empty(data['items'])

        return data
