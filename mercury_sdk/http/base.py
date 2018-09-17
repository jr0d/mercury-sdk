import logging
import json
import requests

log = logging.getLogger(__name__)


def check_error(f):
    """Decorator provides consistent formatting for client http errors."""

    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except requests.exceptions.HTTPError as http_error:
            try:
                log.exception('Encountered an HTTP exception')
                data = http_error.response.json()
            except ValueError:
                data = http_error.response.text
            _response = {'error': True,
                         'code': http_error.response.status_code,
                         'data': data}
            log.debug('Response: %s', _response)
            return _response

    return wrapper


class InterfaceBase(object):
    """ Base HTTP Interface class """
    SERVICE_URI = ''

    def __init__(self, target, max_items=100, auth_token=None,
                 additional_headers=None, verify_ssl=True):
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

        if auth_token:
            self.headers.update({'X-Auth-Token': auth_token})
        if additional_headers:
            self.headers.update(additional_headers)

        self.max_items = max_items
        self.verify_ssl = verify_ssl

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

        headers = self.get_per_request_headers(extra_headers)
        log.debug('Request: method=%s url=%s endpoint=%s params=%s data=%s '
                  'headers=%s', method, self.base_url, item, params, data,
                  headers)
        r = requests.request(method,
                             self.join_endpoint(item),
                             params=params,
                             data=data,
                             headers=headers,
                             verify=self.verify_ssl)
        r.raise_for_status()
        _response = r.json()
        log.debug('Response: %s', _response)
        return _response

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
