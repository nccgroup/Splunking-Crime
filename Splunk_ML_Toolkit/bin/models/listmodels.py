"""
This module contains methods used only by the "listmodels" command and /list_models REST handler
and has been split from models.base in order to avoid dependencies on Anaconda Python
"""

import re
import traceback

import cexc
import util.models_util as models_util
from util.constants import MODEL_FILE_REGEX
from util.lookups_util import get_lookups_from_splunk, parse_model_reply

logger = cexc.get_logger(__name__)
messages = cexc.get_messages_logger()

_model_re = re.compile(MODEL_FILE_REGEX)

def add_model_info_to_lookup_info(lookup_info):
    """
    Adds model-specific information to one of the entries from /lookup-table-files

    Args:
        lookup_info (set): An entry containing information about a lookup file

    Returns:
        lookup_info (set): The input, augmented with model-specific information
    """

    # define some defalt values here if something goes wrong loading the model
    algo_name = 'Unknown'
    model_name = 'Unknown'

    try:
        match = _model_re.match(lookup_info['id'])
        model_name = match.group('model_name')

        algo_name, _, options = models_util.load_algo_options_from_disk(file_path=lookup_info['content']['eai:data'])
    except Exception:
        # if we fail to load the model, we should still populate model info as best we can
        options = {}

        logger.warn(traceback.format_exc())
        messages.warn('listmodels: Failed to load model "%s"', model_name)

    options['algo_name'] = algo_name # can't use the "algo_name" inside options because it may not be present in pre 2.3 models
    options['model_name'] = model_name # can't use the "model_name" inside options because it may be inconsistent with the model file name
    lookup_info['content']['mlspl:model_info'] = options

    return lookup_info


def list_models(searchinfo, query_params=None):
    """
    Gets a list of models from Splunk's /lookup-table-files endpoint

    Args:
        searchinfo (set): a seachinfo object
        query_params (list): a list of tuples representing URL params, ie. [(count, -1)]

    Returns:
        output (list): a list of lookup files
    """

    if query_params is None:
        query_params = []
    name_query = ('search', 'name=*__mlspl_*.csv')
    query_params.append(name_query)

    lookup_files = get_lookups_from_splunk(searchinfo, '-', parse_model_reply, query_params=query_params)
    lookup_files['entry'] = map(add_model_info_to_lookup_info, lookup_files['entry'])

    return lookup_files
