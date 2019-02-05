import json

import pystache

from mercury_sdk.mcli import operations
from mercury_sdk.mcli import output
from mercury_sdk.rpc import job

# TODO: Implement guards once pagenation support is added
MAX_ITEMS = 100


def render_configuration(asset_data, configuration):
    return pystache.render(configuration, **asset_data)


def press_multiple(client, active_client, query, configuration,
                   assets=None, asset_backend=None, wait=False):
    if assets or asset_backend:
        # pre-query for matching mercury_ids
        matches = active_client.query(query, projection=['mercury_id'])['items']
        if not matches:
            output.print_and_exit('Query matches no active computers')
    pass


def press_single_server(client, target, configuration, assets=None,
                        asset_backend=None, wait=False):
    target_query = {'mercury_id': target}

    if assets or asset_backend:
        if assets:
            configuration = render_configuration(assets, configuration)
        print(configuration)
        output.print_and_exit('')
    try:
        _job = job.SimpleJob(client, target_query, 'press',
                             job_kwargs={
                                 'configuration':
                                     operations.read_data_from_file_or_stdin(
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

    return json.dumps(_job.tasks)
