import yaml

from mercury_sdk.mcli import output


def read_config(path):
    try:
        with open(path) as fp:
            return yaml.safe_load(fp)
    except OSError as os_error:
        output.print_and_exit("Error opening or reading file: {}".format(os_error))
    except ValueError as v_error:
        output.print_and_exit("Error deserializing YAML file: {}".format(v_error))
