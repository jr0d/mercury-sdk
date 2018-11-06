import logging
import pkg_resources
import sys

import colorama

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


def print_basic_info(configuration):
    LOG.warning(f'{format_version()}\n'
                f'Configuration file: {colorama.Fore.MAGENTA}'
                f'{configuration.get("config_file")}'
                f'{colorama.Style.RESET_ALL}\n'
                f'Mercury URL: '
                f'{configuration.get("mercury_url")}')


def format_version():
    return f'SDK Version: {colorama.Fore.GREEN}{program_version}' \
           f'{colorama.Style.RESET_ALL}'


def print_and_exit(message, code=0):
    print(message)
    sys.exit(code)
