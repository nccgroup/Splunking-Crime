import os
import json

from util.validation_util import validate_json
from util.experiment_util import convert_form_args_to_dict

json_keys = ['dataSource', 'searchStages']


def validate_experiment_form_args(form_args, experiment_schema_file=None):
    if experiment_schema_file is None:
        experiment_schema_file = os.path.join(os.path.dirname(__file__), "experiment_schema.json")
    with open(experiment_schema_file) as schema:
        valid_keys = json.loads(schema.read())['properties'].keys()

    form_dict = convert_form_args_to_dict(form_args, valid_keys, json_keys)

    validate_json(form_dict, schema_file=experiment_schema_file)


def validate_experiment_history_json(json_data):
    experiment_history_schema_file = os.path.join(os.path.dirname(__file__), "experiment_history_schema.json")
    validate_json(json_data, schema_file=experiment_history_schema_file)
