def explode_json(json_struct: dict, parent: str, leaves: list):
    """
        Recursively explore a dict and return the leave names to create multiple columns from complex objects

        Populates the leaves data structure with all the elements
    """
    for key, values in json_struct.items():
        if isinstance(values, dict):
            explode_json(values, f"{key}_", leaves)
        else:
            leaf = f"{parent}{key}"
            leaves.append(leaf)


def safe_dict_get(dct: dict, keys: list):
    """
        Get the value from the last key of a nested dict based on a list of keys
    """
    for key in keys:
        try:
            dct = dct[key]
        except KeyError:
            return None
    return dct
