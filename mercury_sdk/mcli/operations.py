import json
from mercury_sdk.http import inventory, rpc
from mercury_sdk.rpc import job
from mercury_sdk.mcli import output


def get_inventory_client(configuration, token=None):
    return inventory.InventoryComputers(
        configuration['mercury_url'],
        max_items=configuration['max_items'],
        auth_token=token
    )


def query_inventory(client, configuration):
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
