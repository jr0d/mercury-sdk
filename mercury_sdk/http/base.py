import json
import requests


def check_error(f):
    """Decorator provides consistent formatting for client http errors."""

    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except requests.exceptions.HTTPError as http_error:
            try:
                data = http_error.response.json()
            except ValueError:
                data = http_error.response.text
            return {'error': True, 'code': http_error.response.status_code,
                    'data': data}

    return wrapper


class InterfaceBase(object):
    """ Base HTTP Interface class """
    SERVICE_URI = ''

    def __init__(self, target, max_items=100, additional_headers=None):
        """ Base Constructor

        :param target: URL and base URI of the target service
        :param max_items: The default maximum items to request for endpoints
        :param additional_headers: Additional headers to pass on, such as
        auth tokens.
        .. note:

            These can also be added for each request
        that support pagination.
        """
        self.target = target
        if self.SERVICE_URI:
            self.base_url = '{0}/{1}'.format(self.target, self.SERVICE_URI)
        else:
            self.base_url = self.target

        self.headers = {'Content-type': 'application/json'}

        if additional_headers:
            self.headers.update(additional_headers)

        self.max_items = max_items

    def join_endpoint(self, endpoint):
        """
        :param endpoint:
        :return:
        """
        endpoint = endpoint or ''
        endpoint = endpoint.strip('/')
        if not endpoint:
            # endpoint is base_url
            return self.base_url
        return self.base_url + '/' + endpoint.lstrip('/')

    def get_per_request_headers(self, extra_request_headers):
        """

        :param extra_request_headers:
        :return:
        """
        if not extra_request_headers:
            return self.headers

        per_request_headers = self.headers.copy()
        per_request_headers.update(extra_request_headers)
        return per_request_headers

    @check_error
    def _request(self,
                 method,
                 item='',
                 data=None,
                 params=None,
                 extra_headers=None):
        """

        :param method:
        :param item:
        :param data:
        :param params:
        :param extra_headers:
        :return:
        """

        if data is not None:
            data = json.dumps(data)

        r = requests.request(method,
                             self.join_endpoint(item),
                             params=params,
                             data=data,
                             headers=self.get_per_request_headers(
                                 extra_headers))
        r.raise_for_status()
        return r.json()

    def get(self, item='', params=None, extra_headers=None):
        """

        :param item:
        :param params:
        :param extra_headers:
        :return:
        """
        return self._request('get', item, params=params,
                             extra_headers=extra_headers)

    def post(self, item='', data=None, params=None, extra_headers=None):
        """

        :param item:
        :param data:
        :param params:
        :param extra_headers:
        :return:
        """
        return self._request('post', item, data=data, params=params,
                             extra_headers=extra_headers)

