from mercury_sdk.mcli import operations


def build_ansible_inventory(client, query, user='root', hostname=''):
    projection = ["mercury_id", "interfaces"]
    ansible_inventory = []
    q = operations.de(query)
    data = operations.make_query(client, q, projection, limit=250, strip_empty=True)

    if not data["items"]:
        operations.output.print_and_exit("Query did not match any active targets")

    counter = 0
    for device in data['items']:
        for interface in device['interfaces']:
            if interface['address_info']:
                counter += 1
                ansible_inventory.append(
                    "{} ansible_ssh_user={}{}".format(
                        interface['address_info'][0]['addr'],
                        user,
                        ' unique_hostname={}'.format(hostname.format(
                            **dict(cnt=counter)) if hostname else '')
                    ))
                break
    return "\n".join(ansible_inventory)

