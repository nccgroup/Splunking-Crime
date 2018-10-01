import json
import os
from urllib import pathname2url

from vendor.jsonschema import Draft4Validator, RefResolver, SchemaError, ValidationError
from vendor.jsonschema import validate


def validate_json(json_data, schema_file):
    with open(schema_file) as f:
        schema = json.load(f)

    schema_dir = os.path.dirname(schema_file)
    schema_path = 'file://{0}/'.format(pathname2url(schema_dir))

    resolver = RefResolver(schema_path, schema)

    try:
        Draft4Validator.check_schema(schema)
        validate(json_data, schema, resolver=resolver)
    except SchemaError as e:
        raise Exception("Failed to check JSON schema in {}: {}".format(schema_file, e.message))
    except ValidationError as e:
        raise Exception("Unable to validate data against json schema in {}: {}, {}, {}, {}, {}".format(schema_file, e.message, e.context, e.path, e.schema_path, e.cause))
