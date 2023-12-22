import logging
from typing import Dict, List


def sanity_check(data: List[Dict]) -> bool:
    """
    Check for inconsistencies in the data points.

    :param data: list of data points.
    :returns: True if all data points are valid, False otherwise.
    """

    # check for fields with same key as tags and
    for point in data:
        if any([f in point['tags'] for f in point['fields']]):
            logging.error(f"Tags key found in fields: \n {point['tags']}, {point['fields']}")
            return False

    return True


def reformat_last(data: Dict) -> Dict:
    """
    Remove the automatically prepended 'last_' to results from queries involving last()

    :param data: data returned by InfluxDBClient.query
    :returns: reformatted data points.
    """
    # copy keys to avoid 'dictionary changed during loop' error
    rdata = []
    if len(data):
        for d in data.get_points():
            keys = list(d.keys())
            for key in keys:
                d[key.replace('last_', '')] = d.pop(key)
            rdata.append(d)

    return rdata
