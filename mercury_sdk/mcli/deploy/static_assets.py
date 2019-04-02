from mercury_sdk.rpc import job

from mercury_sdk.mcli import operations
from mercury_sdk.mcli import press


def press_static_preprocessor(rpc_client, inv_client, template, query, hostname):
    assets = press.build_press_asset_db_from_inv(inv_client, query, hostname)

    with open(template) as fp:
        template_data = fp.read()

    instruction = {
        'assets': assets,
        'template': template_data.splitlines()
    }

    preprocessor = job.Preprocessor(
        rpc_client, operations.de(query), 'press_static_assets', instruction)

    preprocessor.start()
    print(operations.json_format(preprocessor.tasks))
    preprocessor.join()
    print(operations.json_format(preprocessor.status))
