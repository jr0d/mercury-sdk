import os
import json
import sys
import yaml

from mercury_sdk.http import inventory, rpc
from mercury_sdk.rpc import job
from mercury_sdk.mcli import output

MAX_INPUT_SIZE = 20*2**20  # 20MiB


def get_inventory_client(configuration, token=None):
    return inventory.InventoryComputers(
        configuration['mercury_url'],
        max_items=configuration['max_items'],
        auth_token=token
    )


def read_data_from_file_or_stdin(path=None):
    if not os.path.expanduser(path):
        fp = sys.stdin
    else:
        fp = open(path)

    raw = fp.read(MAX_INPUT_SIZE)

    if fp.read(1):
        raise RuntimeError('Input is too large')

    fp.close()

    parsed = None

    try:
        parsed = json.loads(raw)
    except ValueError:
        pass

    if not parsed:
        try:
            parsed = yaml.safe_load(raw)
        except ValueError:
            raise RuntimeError('Input data is not valid JSON/YAML')

    return parsed


def query_inventory(client, configuration):
    raw = configuration['query']
    if raw == '-':
        query = read_data_from_file_or_stdin()
    elif raw and raw[0] == '@':
        query = read_data_from_file_or_stdin(raw[1:])
    else:
        try:
            query = json.loads(configuration['query'])
        except ValueError:
            output.print_and_exit('Query is not valid json', 1)
            return

    if configuration.get('active'):
        query.update({'active': {'$ne': None}})

    data = client.query(
        query,
        projection=configuration['projection'].split(','),
        params={'limit': configuration['max_items']},
        strip_empty_elements=True
    )

    return json.dumps(data, indent=2)


def get_inventory(client, configuration):
    data = client.get(configuration['mercury_id'],
                      projection=configuration['projection'].split(','))
    return json.dumps(data, indent=2)


def get_rpc_client(configuration, token=None):
    return rpc.JobInterfaceBase(
        configuration['mercury_url'],
        auth_token=token)


def get_instruction_from_file(path):
    data = read_data_from_file_or_stdin(path)
    if 'instruction' in data:
        # If format is {instruction: data}
        data = data['instruction']
    try:
        method = data['method']
    except KeyError:
        output.print_and_exit('Instruction is malformed', 1)
        return

    args = data.get('args', [])
    kwargs = data.get('kwargs', {})
    return method, args, kwargs


def make_rpc(client, target_query, method, job_args, job_kwargs, wait=False):
    _job = job.SimpleJob(client, target_query, method, job_args, job_kwargs)
    _job.start()

    if not wait:
        return json.dumps({
            'job_id': _job.job_id,
            'targets': _job.targets
        }, indent=2)

    # Blocks and waits for RPC task to complete
    _job.join(poll_interval=1)

    return json.dumps(_job.tasks, indent=2)


def get_job(client, job_id):
    return json.dumps(client.get(job_id), indent=2)


def get_status(client, job_id):
    return json.dumps(client.status(job_id), indent=2)


def get_tasks(client, job_id):
    return json.dumps(client.tasks(job_id), indent=2)
