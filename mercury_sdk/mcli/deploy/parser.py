import logging
import os
import yaml

from mercury_sdk.mcli import configuration

log = logging.getLogger(__name__)


def get_keys_from_file(path):
    with open(os.path.expanduser(path)) as fp:
        return [line.strip() for line in fp.readlines()]


def ssh_authorized_keys(segment, config):
    """

    :param segment:
    :param config:
    :return:
    """
    if 'ssh_authorized_keys' not in segment:
        return

    keys = []
    if 'keys' not in config['modules']:
        config['modules']['keys'] = keys

    key_defs = segment['ssh_authorized_keys']
    for key_def in key_defs:
        if 'key.filename' in key_def:
            log.info('Reading keys from %s', key_def['key.filename'])
            keys += get_keys_from_file(key_def['key.filename'])
        elif 'key.github' in key_def:
            log.info('This is really cool but not implemented yet')
        else:
            log.error("Garbage in ssh_authorized_keys")


def update_vars(segment, config):
    """

    :param segment:
    :param config:
    :return:
    """
    if 'vars' not in segment:
        return
    # vars are overridden with each call
    config['vars'].update(segment['vars'])


def update_segment(segment, config):
    ssh_authorized_keys(segment, config)
    update_vars(segment, config)


def parse_deploy_configuration(path):
    data = configuration.read_config(path)

    deploy_config = {
        'vars': {},
        'modules': {},
        'secrets': {}
    }

    if 'global' in data:
        update_segment(data['global'], deploy_config)

    # make this update_groups
    if 'groups' in data:
        for group in data['groups']:
            # perform query to get targets
            pass






