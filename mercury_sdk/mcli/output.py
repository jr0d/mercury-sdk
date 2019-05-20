import logging
import sys

import colorama
import pkg_resources

FORMAT = '%(message)s'
program_version = pkg_resources.get_distribution('mercury-sdk').version
LOG = logging.getLogger(__name__)


def setup_logging(verbosity):
    verbosity_map = {
        0: logging.ERROR,
        1: logging.WARNING,
        2: logging.INFO,
        3: logging.DEBUG
    }

    logging.basicConfig(format=FORMAT,
                        level=verbosity_map.get(verbosity, logging.DEBUG))


# Remove fstrings for python < 3.6 compatibility
# def print_basic_info(configuration):
#     LOG.warning(f'{format_version()}\n'
#                 f'Configuration file: {colorama.Fore.MAGENTA}'
#                 f'{configuration.get("config_file")}'
#                 f'{colorama.Style.RESET_ALL}\n'
#                 f'Mercury URL: '
#                 f'{configuration.get("mercury_url")}')
#
#
# def format_version():
#      return f'SDK Version: {colorama.Fore.GREEN}{program_version}' \
#             f'{colorama.Style.RESET_ALL}'


def print_basic_info(configuration):
    LOG.warning('{}\n'
                'Configuration file: {}'
                '{}'
                '{}\n'
                'Mercury URL: '
                '{}'.format(
                            format_version(),
                            colorama.Fore.MAGENTA,
                            configuration.get('config_file'),
                            colorama.Style.RESET_ALL,
                            configuration.get('mercury_url')))


def format_version():
    return 'SDK Version: {}{} {}'.format(
        colorama.Fore.GREEN, program_version, colorama.Style.RESET_ALL)


def print_and_exit(message, code=0):
    print(message)
    sys.exit(code)


def print_rpc_capabilities(active_data):
    for name, capability in active_data['capabilities'].items():
        print('{}{}'.format(colorama.Fore.MAGENTA, name))
        print(colorama.Style.RESET_ALL)
        print('{}'.format(capability['description']))
        print('{}'.format(capability['doc']))

