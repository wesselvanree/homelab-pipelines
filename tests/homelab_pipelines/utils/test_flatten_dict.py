from inline_snapshot import snapshot

from homelab_pipelines.utils.flatten_dict import flatten_dict


def test_flatten_dict():
    value = {"a": 0, "b": {"1": 1, "2": 2}}
    assert flatten_dict(value) == snapshot({"a": 0, "b__1": 1, "b__2": 2})

    value = {"parent1": 0, "parent2": {"child": 1, "subparent": {"child": 2}}}
    assert flatten_dict(value) == snapshot(
        {
            "parent1": 0,
            "parent2__child": 1,
            "parent2__subparent__child": 2,
        }
    )
