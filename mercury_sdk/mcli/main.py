import argparse
import json
import logging
import os
import pkg_resources

import colorama

from mercury_sdk.mcli.configuration import configuration_from_yaml
from mercury_sdk.mcli.auth import auth as mcli_auth
from mercury_sdk.mcli import output
from mercury_sdk.mcli import operations
from mercury_sdk.mcli import shell

LOG = logging.getLogger(__name__)

COMMANDS = [
    'inventory',
    'rpc',
    'shell'
]

DEFAULT_PROGRAM_DIR = os.path.expanduser('~/.mercury-sdk')
DEFAULT_CONFIG = os.path.join(DEFAULT_PROGRAM_DIR, 'mcli.yml')
TOKEN_CACHE = os.path.join(DEFAULT_PROGRAM_DIR, '.token.yml')

MERCURY_API_URL_ENV = 'MERCURY_API_URL'
MERCURY_API_USERNAME_ENV = 'MERCURY_API_USERNAME'
MERCURY_API_QUERY_ENV = 'MERCURY_API_QUERY'

program_version = pkg_resources.get_distribution('mercury-sdk').version


def options():
    parser = argparse.ArgumentParser(
        description='The Mercury Command Line Interface',
        epilog='SDK version {}'.format(program_version))
    parser.add_argument('--version',
                        action='store_true', help='Display the program version')
    parser.add_argument('-c', '--config-file',
                        default=DEFAULT_CONFIG,
                        help='SDK configuration file')
    parser.add_argument('--program-directory', default=DEFAULT_PROGRAM_DIR,
                        help='Alternative location for program data')
    parser.add_argument('--token-cache', default=TOKEN_CACHE,
                        help='alternative location of the token cache')
    parser.add_argument('-m', '--mercury-url',
                        default=os.environ.get(MERCURY_API_URL_ENV),
                        help='The mercury url to use')
    parser.add_argument('-v', dest='verbosity',
                        action='count',
                        default=0,
                        help='Verbosity level -v, somewhat verbose, -vvv '
                             'really verbose')

    subparsers = parser.add_subparsers(dest='command', help='<command> --help')

    # login
    login_parser = subparsers.add_parser(
        'login',
        help='Login to the authentication service and store the token in the '
             'local environment')

    login_parser.add_argument('--no-store', default=False, action='store_true',
                              help='Do not store the token')
    login_parser.add_argument('-q', '--quiet', action='store_true',
                              help='Do not print the token')

    # logout
    subparsers.add_parser(
        'logout',
        help='Logout of the authentication service')

    # set-token
    set_token_parser = subparsers.add_parser(
        'set-token',
        help='Bypass auth handlers and set a token directly')
    set_token_parser.add_argument('token', help='The token to set')
    set_token_parser.add_argument(
        '--expire-at', default=8,
        help='The number of hours to consider the token valid')

    # inventory
    inv_parser = subparsers.add_parser('inventory',
                                       help='Inventory query operations')
    inv_parser.add_argument('-q', '--query', default='{}',
                            help='A mercury query to execute in valid JSON. '
                                 'Use "-" and the value will be read from '
                                 'stdout use "@filename" and the query will be '
                                 'read from this file')
    inv_parser.add_argument('-p', '--projection', default='',
                            help='Specify the key projection to produce the '
                                 'desired output')
    inv_parser.add_argument('-n', '--max-items', default=100)
    inv_parser.add_argument('-a', '--active',
                            help='Only search for active devices',
                            action='store_true')
    inv_parser.add_argument('mercury_id', default=None, nargs='?',
                            help='Get a device record by mercury_id')

    # rpc
    rpc_parser = subparsers.add_parser(
        'rpc',
        help='RPC commands'
    )
    rpc_subparsers = rpc_parser.add_subparsers(dest='rpc_command')
    rpc_submit_parser = rpc_subparsers.add_parser('submit',
                                                  help='submit a job')
    rpc_submit_parser.add_argument('-q', '--query',
                                   help='A mercury query to execute in valid JSON. '
                                        'Use "-" and the value will be read from '
                                        'stdout use "@filename" and the query will be '
                                        'read from this file'
                                   )
    rpc_submit_parser.add_argument('-t', '--target',
                                   help='A single mercury id to run the command on')
    rpc_submit_parser.add_argument('-m', '--method', help='the RPC to run')
    rpc_submit_parser.add_argument('-a', '--args', nargs='+')
    rpc_submit_parser.add_argument('-k', '--kwargs', default='{}')

    # shell
    shell_parser = subparsers.add_parser(
        'shell',
        help='Enter a shell for interactive inventory management')
    shell_parser.add_argument('-q', '--query', default=None,
                              help='Set the initial target query for the shell')
    shell_parser.add_argument('-t', '--target', default=None,
                              help='The mercury_id of a single target')
    shell_parser.add_argument('-r', '--run', default=None,
                              help='Instead of entering the shell,'
                                   'run the command, print the result, and '
                                   'exit')
    shell_parser.add_argument('--quiet', default=False, action='store_true',
                              help='Suppress command output')
    shell_parser.add_argument('--raw', default=False, action='store_true',
                              help='Do not print agent info, only the raw command output')

    namespace = parser.parse_args()
    if namespace.version:
        output.print_and_exit(output.format_version(), 0)
    if not namespace.command:
        parser.print_help()
        output.print_and_exit('\nPlease specify a command...')
    return namespace


def merge_configuration(namespace, configuration):
    _program_configuration = namespace.__dict__
    if not namespace.mercury_url:
        _m = configuration.get('mercury_api', {}).get('url')
        if not _m:
            output.print_and_exit('Mercury Service URL is undefined', 1)
        _program_configuration['mercury_url'] = _m
    _program_configuration['auth'] = configuration.get('auth')
    _program_configuration['auth_handler'] = configuration.get('auth_handler')
    return _program_configuration


def router(command, configuration):
    output.print_basic_info(configuration)
    if command == 'login':

        token_data = mcli_auth.get_token(configuration, TOKEN_CACHE)
        if not configuration['quiet']:
            print('Expires: {expires_at}, Token: {token}'.format(**token_data))
        return
    elif command == 'logout':
        mcli_auth.invalidate_token(configuration, TOKEN_CACHE)
        return

    if configuration['auth'] and configuration['auth_handler']:
        token = mcli_auth.get_token(configuration, TOKEN_CACHE)['token']
    else:
        token = None

    if command == 'inventory':
        inv_client = operations.get_inventory_client(configuration, token)
        if configuration.get('mercury_id'):
            print(operations.get_inventory(inv_client, configuration))
        else:
            print(operations.query_inventory(inv_client, configuration))

    def _prepare_rpc():
        _rpc_client = operations.get_rpc_client(configuration, token)
        query = configuration.get('query')
        target = configuration.get('target')
        if query:
            try:
                _target_query = json.loads(configuration['query'])
            except ValueError:
                output.print_and_exit('query is not valid JSON')
                return
        elif target:
            _target_query = {'mercury_id': target.strip()}
        else:
            output.print_and_exit('Must provide a query or target')
            return

        return _rpc_client, _target_query

    if command == 'rpc':
        rpc_client, target_query = _prepare_rpc()
        kwargs = json.loads(configuration['kwargs'])
        print(operations.make_rpc_call(rpc_client,
                                       target_query,
                                       configuration['method'],
                                       configuration['args'],
                                       kwargs))

    if command == 'shell':
        rpc_client, target_query = _prepare_rpc()

        mshell = shell.MercuryShell(rpc_client, initial_query=target_query,
                                    quiet=configuration['quiet'], raw=configuration['raw'])

        if configuration.get('run'):
            mshell.run_job(configuration.get('run').strip())
        else:
            mshell.input_loop()


def main():
    colorama.init(autoreset=True)
    namespace = options()

    if not os.path.exists(namespace.program_directory):
        os.makedirs(namespace.program_directory, 0o700)
    configuration = configuration_from_yaml(namespace.config_file) or {}
    output.setup_logging(namespace.verbosity)

    program_configuration = merge_configuration(namespace, configuration)

    router(namespace.command, program_configuration)
