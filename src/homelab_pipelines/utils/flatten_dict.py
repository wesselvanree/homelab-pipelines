from typing import Any, Dict


def flatten_dict(d: Dict[str, Any], parent_key="", sep="__") -> Dict[str, Any]:
    """Flatten a dictionary.

    Each child will be transformed in such a way that the key becomes `[parent_key][sep][key]`.

    Parameters
    ----------
    d : _type_
        _description_
    parent_key : str, optional
        _description_, by default ""
    sep : str, optional
        _description_, by default "__"

    Returns
    -------
    _type_
        _description_
    """
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_dict(v, new_key, sep=sep))
        else:
            items[new_key] = v
    return items
