import yaml


def configuration_from_yaml(filename):
    """Loads a YAML configuration file.

    :param filename: The filename of the file to load.
    :returns: dict -- A dictionary representing the YAML configuration file
        loaded. If the file can't be loaded, then the empty dict is returned.
    """
    try:
        with open(filename) as infile:
            return yaml.safe_load(infile.read())
    except IOError:
        return {}

