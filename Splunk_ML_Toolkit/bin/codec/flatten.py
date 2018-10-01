#!/usr/bin/env python

import copy


class Ref(object):
    def __init__(self, obj):
        self._id = self.id(obj)

    def get_id(self):
        return self._id

    @staticmethod
    def id(obj):
        return str(id(obj))


def flatten(obj):
    """Flatten an object hierarchy.

    Walks an object hierarchy while keeping a dictionary of
    non-Ref objects.  Replaces all non-Ref objects with Ref
    objects.

    Parameters
    ----------
    obj : object (new-style)
        Root of the object hierarchy to flatten.

    Returns
    -------
    (flat_obj, refs) : tuple
        flat_obj is a deep-copy of obj with all non-Ref objects
        replaced with Ref objects. refs is a dictionary of non-Ref
        objects.
    """
    refs = {}
    cpy = copy.deepcopy(obj)
    todo = [cpy]

    while len(todo) > 0:
        current = todo.pop(0)

        if isinstance(current, object) and hasattr(current, "__dict__"):
            todo.append(current.__dict__)

        elif isinstance(current, dict):
            for k, v in current.items():
                if isinstance(v, object) and hasattr(v, "__dict__") and not isinstance(v, Ref):
                    todo.append(v)
                    refs.setdefault(Ref.id(v), v)
                    current[k] = Ref(v)

        elif isinstance(current, list):
            for i, v in enumerate(current):
                if isinstance(v, object) and hasattr(v, "__dict__") and not isinstance(v, Ref):
                    todo.append(v)
                    refs.setdefault(Ref.id(v), v)
                    current[i] = Ref(v)

        elif (isinstance(current, set) or
                  isinstance(current, frozenset) or
                  isinstance(current, tuple)):
            raise ValueError("unsupported: %s", current)

    return (cpy, refs)


def expand(obj, refs):
    """Expand a flattened object hierarchy.

    Walks a flattened object hierarchy and replaces Ref objects
    with the actual objects referred to from refs.

    Parameters
    ----------
    obj : object (new-style)
        Root of the object hierarchy flattened by flatten(). All
        objects are modified in place.
    refs : dict
        Dictionary of object references used by Ref objects in obj.

    Returns
    -------
    obj : object (new-style)
        For convenience, the expanded obj is returned.

    """
    todo = [obj]

    while len(todo) > 0:
        current = todo.pop(0)

        if isinstance(current, object) and hasattr(current, "__dict__"):
            assert not isinstance(current, Ref)
            todo.append(current.__dict__)

        elif isinstance(current, dict):
            for k, v in current.items():
                if isinstance(v, Ref):
                    current[k] = refs[v.get_id()]
                    todo.append(current[k])

        elif isinstance(current, list):
            for i, v in enumerate(current):
                if isinstance(v, Ref):
                    current[i] = refs[v.get_id()]
                    todo.append(current[i])

        elif (isinstance(current, set) or
                  isinstance(current, frozenset) or
                  isinstance(current, tuple)):
            raise ValueError("unsupported: %s", current)

    return obj
