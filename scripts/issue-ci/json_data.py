"""One strict JSON decoder for policy, metadata and readiness evidence."""
import json


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate JSON object key')
        result[key] = value
    return result


def loads(value):
    # object_pairs_hook runs at every depth, including objects inside arrays,
    # after JSON escapes in keys have been decoded. Equal duplicates also refuse.
    return json.loads(value, object_pairs_hook=unique_object)
