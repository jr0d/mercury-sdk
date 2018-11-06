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
