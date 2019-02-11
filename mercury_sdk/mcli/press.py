import json
import pystache
import subprocess

from mercury_sdk.http import active
from mercury_sdk.mcli import operations
from mercury_sdk.mcli import output
from mercury_sdk.rpc import job

# TODO: Implement guards once pagenation support is added
MAX_ITEMS = 100


def render_configuration(asset_data, configuration):
    return pystache.render(configuration, **asset_data)


def get_active_matches(rpc_client, query):
    # Assemble an active client from the rpc client we already have
    active_client = active.ActiveComputers(rpc_client.target)
    active_client.headers.update(rpc_client.headers)

    return active_client.query(query, projection=['mercury_id'])['items']

def call_asset_backend(matches, path):

def press(rpc_client,
          query_or_target,
          press_configuration,
          asset_file_path=None,
          asset_backend_path=None,
          extra_assets=None,
          wait=False):

    if isinstance(query_or_target, str):
        query = {'mercury_id': query_or_target}
        single_target = True
    else:
        query = query_or_target
        single_target = False

    if asset_file_path or asset_backend_path or extra_assets:
        assets = {}
        if asset_file_path:
            assets.update(operations.read_data_from_file_or_stdin(asset_file_path))

        if not single_target:
            matches = get_active_matches(rpc_client, query)
            if not matches:
                output.print_and_exit('Query did not match any targets')




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
