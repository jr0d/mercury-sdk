import argparse
from mercury.common.configuration import MercuryConfiguration

COMMANDS = [
    'inventory',
    'active',
    'rpc'
]


def options():
    mc = MercuryConfiguration('mcli',
                              'mcli.conf',
                              description='The Mercury Command Line Interface',
                              enable_logging_options=False)
    mc.add_positional_argument('command', 'COMMANDS: {}'.format(
        '\n'.join(COMMANDS)))

    mc.scan_options()
    return mc


if __name__ == '__main__':
    c = options()
    print(c.extra_options)
