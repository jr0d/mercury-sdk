import json

import yaml

from mercury_sdk.mcli import operations
from mercury_sdk.mcli import output
from mercury_sdk.rpc import job


def configuration_from_yaml(filename):
    """Loads a YAML configuration file.

    :param filename: The filename of the file to load.
    :returns: dict -- A dictionary representing the YAML configuration file
        loaded. If the file can't be loaded, then the empty dict is returned.
    """
    with open(filename) as infile:
        return yaml.load(infile.read())


def press_server(client, target_query, configuration, wait=False):
    try:
        _job = job.SimpleJob(client, target_query, 'press',
                             job_kwargs={
                                 'configuration': configuration_from_yaml(
                                     configuration)})
    except (IOError, OSError) as e:
        output.print_and_exit(
            'Could not load configuration file: {}'.format(e), 1)
        return
    _job.start()

    if not wait:
        return json.dumps({
            'job_id': _job.job_id,
            'targets': _job.targets
        }, indent=2)

    _job.join(poll_interval=1)

    return json.dumps(_job.tasks, indent=2)


def build_press_asset_db_from_inv(client, query, hostname_template):
    """ Simply adds the current ip to the asset store """

    projection = ["mercury_id", "interfaces"]
    assets = {}
    q = operations.de(query)
    q.update({'active': {'$ne': None}})
    data = operations.make_query(client, q, projection, limit=250, strip_empty=True)

    if not data["items"]:
        operations.output.print_and_exit("Query did not match any active targets")

    counter = 0
    for device in data['items']:
        for interface in device['interfaces']:
            if interface['address_info']:
                counter += 1
                assets[device['mercury_id']] = {
                    'snet_ip_address': interface['address_info'][0]['addr'],
                    'hostname': hostname_template.format(**dict(cnt=counter)),
                    'predictable_names': interface['predictable_names']
                }
                break
    return assets
