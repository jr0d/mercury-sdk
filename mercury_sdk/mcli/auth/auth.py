import logging
import importlib
import os
import yaml

from datetime import datetime, timezone
import dateutil.parser

LOG = logging.getLogger(__name__)
# TODO: Support multiple auth handlers
__auth_handler = None


def write_token(token_data, path):
    with open(path, 'w') as token_file:
        yaml.dump(token_data, stream=token_file)


def read_token(path):
    with open(path) as fp:
        return yaml.load(fp)


# TODO: learn handler names but searching the auth directory
def get_auth_handler(handler_name):
    global __auth_handler
    if not __auth_handler:
        __auth_handler = importlib.import_module(
            '.mcli.auth.{}'.format(handler_name), package='mercury_sdk')
    return __auth_handler


# Since the configuration file is an argument, and handler_name is learned
# from the configuration, this method cannot be used. Once the change is in
# to discover handlers via directory scan, the handler can then add it's own
# argparse arguments. As of now, this isn't used.
def update_parser(handler_name, parser):
    _h = get_auth_handler(handler_name)
    if hasattr(_h, 'add_arguments'):
        _h.add_arguments(parser)


# TODO: index tokens by mercury url
def get_token(configuration, token_path):
    if os.path.exists(token_path):
        LOG.debug('Token exists, checking...')
        token_data = read_token(token_path)
        if datetime.now(timezone.utc) < dateutil.parser.parse(
                token_data['expires_at']):
            return token_data
        else:
            LOG.info('Token expired at: {}'.format(token_data['expired_at']))
            os.remove(token_path)

    LOG.info('Auth handler: {}'.format(configuration['auth_handler']))
    auth_handler = get_auth_handler(configuration['auth_handler'])

    token_data = auth_handler.authenticate(configuration)
    if not configuration.get('no_store'):
        LOG.debug('Writing token file: {}'.format(token_path))
        write_token(token_data, token_path)
    return token_data


def invalidate_token(configuration, token_path):
    if os.path.exists(token_path):
        token_data = read_token(token_path)
        auth_handler = get_auth_handler(configuration['auth_handler'])
        auth_handler.invalidate(configuration, token_data)
        os.remove(token_path)
