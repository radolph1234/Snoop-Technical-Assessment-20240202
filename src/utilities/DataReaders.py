import json


def read_json(path: str) -> dict:
    """
    Read a JSON file and return the content as a dictionary.

    :param path: path to JSON file
    :type path: str
    :return: Dictionary containing the data
    :rtype: dict
    """
    
    with open(path, 'r') as f:
        return json.load(f)
