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

    def __init__(self, target, max_items=100):
        """ Base Constructor

        :param target: URL and base URI of the target service
        :param max_items: The default maximum items to request for endpoints
        that support pagination.
        """
        self.target = target
        if self.SERVICE_URI:
            self.base_url = '{0}/{1}'.format(self.target, self.SERVICE_URI)
        else:
            self.base_url = self.target

        self.headers = {'Content-type': 'application/json'}

        self.max_items = max_items

    def join_endpoint(self, endpoint):
        """
        :param endpoint:
        :return:
        """
        endpoint = endpoint.strip('/')
        if not endpoint:
            # endpoint is base_url
            return self.base_url
        return self.base_url + '/' + endpoint.lstrip('/')

    def append_header(self, extra_headers):
        """

        :param extra_headers:
        :return:
        """
        if not extra_headers:
            return self.headers
        return extra_headers.update(self.headers)

    @check_error
    def get(self, item='', params=None, extra_headers=None):
        """

        :param item:
        :param params:
        :param extra_headers:
        :return:
        """
        r = requests.get(self.join_endpoint(item), params=params,
                         headers=self.append_header(extra_headers))
        r.raise_for_status()
        return r.json()

    @check_error
    def post(self, item='', data=None, params=None, extra_headers=None):
        """

        :param item:
        :param data:
        :param params:
        :param extra_headers:
        :return:
        """
        r = requests.post(self.join_endpoint(item), params=params,
                          data=json.dumps(data),
                          headers=self.append_header(extra_headers))
        r.raise_for_status()
        return r.json()

