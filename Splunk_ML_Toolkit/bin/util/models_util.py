"""
This module contains utility methods needed by both models.base, models.listmodels and models.deletemodels
"""

import json
import os
import shutil

import cexc

from util.base_util import is_valid_identifier, get_staging_area_path
from util.lookups_util import load_lookup_file_from_disk
from util import rest_url_util
from util.rest_proxy import rest_proxy_from_searchinfo
from util.lookups_util import get_lookup_file_from_searchinfo, get_file_path_from_content
from rest.proxy import SplunkRestException

logger = cexc.get_logger(__name__)
messages = cexc.get_messages_logger()


def get_model_list_by_experiment(rest_proxy, namespace, experiment_id):
    url_params = [
        ('search', '__mlspl__exp_*{}*.csv'.format(experiment_id)),
        ('count', '0')
    ]
    url = rest_url_util.make_get_lookup_url(rest_proxy, namespace=namespace, url_params=url_params)
    reply = rest_proxy.make_rest_call('GET', url)
    content = json.loads(reply.get('content'))
    entries = content.get('entry')
    model_list = []
    for entry in entries:
        model_list.append(entry.get('name'))
    return model_list


def load_algo_options_from_disk(file_path):
    model_data = load_lookup_file_from_disk(file_path)
    algo_name = model_data['algo']
    model_options = json.loads(model_data['options'])

    return algo_name, model_data, model_options


def model_name_to_filename(name, tmp=False):
    assert isinstance(name, basestring)
    assert is_valid_identifier(name), "Invalid model name"
    suffix = '.tmp' if tmp else ''
    return '__mlspl_' + name + '.csv' + suffix


def update_model_file_from_rest(model_filename, searchinfo, namespace, model_filepath):
    """
    update the model file by replacing it with a file from the upload staging area.

    Args:
        model_filename (str): target model file name
        searchinfo (dict): searchinfo
        namespace (str): file namespace. 'user', 'app' or 'global'
        model_filepath (str): the file path of the source file, it has to be in STAGING AREA.

    Returns:
        reply (dict): reply from POST request.
    """

    rest_proxy = rest_proxy_from_searchinfo(searchinfo)
    url = rest_url_util.make_lookup_url(rest_proxy, namespace=namespace, lookup_file=model_filename)
    payload = {
        'eai:data': model_filepath,
        'output_mode': 'json'
    }

    return rest_proxy.make_rest_call('POST', url, postargs=payload)


def create_model_file_from_rest(model_filename, searchinfo, namespace, model_filepath):
    """
    Create a ml-spl model file by moving the file from the upload staging area into $SPLUNK_HOME.

    Args:
        model_filename (str): the target model name
        searchinfo (dict) :
        namespace (str) : file namespace
        model_filepath (str) : the file path of the source file, it has to be in STAGING AREA.

    Returns:
        reply (dict): reply from POST request.
    """

    rest_proxy = rest_proxy_from_searchinfo(searchinfo)
    url = rest_url_util.make_lookup_url(rest_proxy, namespace=namespace)
    payload = {
        'eai:data': model_filepath,
        'name': model_filename,
        'output_mode': 'json'
    }

    return rest_proxy.make_rest_call('POST', url, postargs=payload)


def move_model_file_from_staging(model_filename, searchinfo, namespace, model_filepath):
    """
    Try to update the model file to the target `searchinfo` and `namespace` from `model_filepath`, if it is not there,
    create the model file.
    Args:
        model_filename:  the target model name
        searchinfo (dict): search info
        namespace: (str): file namespace. 'user', 'app' or 'global'
        model_filepath (str): the file path of the source file, it has to be in STAGING AREA.

    Returns:
        reply (dict): the reply from the POST request.

    """
    # try to update the model
    reply = update_model_file_from_rest(model_filename, searchinfo, namespace, model_filepath)
    # if we fail to update the model because it doesn't exist, try to create it instead
    if not reply['success']:
        if reply['error_type'] == 'ResourceNotFound':
            reply = create_model_file_from_rest(model_filename, searchinfo, namespace, model_filepath)

        # the redundant-looking check is actually necessary because it prevents this logic from triggering if the update fails but the create succceeds
        if not reply['success']:
            try:
                # if the model save fails, clean up the temp model file
                os.unlink(model_filepath)
            # if we somehow fail to clean up the temp model, don't expose the error to the user
            except Exception as e:
                logger.debug(str(e))

    return reply


def parse_reply_for_rest(reply):
    """
    simplified version of lookups_parse_reply - instead of throwing custom Exceptions for non success case, it only
    throws one exception which is a wrapper of splunk reply.
    Args:
        reply:

    Returns:

    """
    try:
        if not reply['success']:
            raise SplunkRestException(reply)
        return json.loads(reply['content'])
    except SplunkRestException as e:
        cexc.log_traceback()
        raise SplunkRestException(reply)
    except Exception as e:
        cexc.log_traceback()
        raise Exception("Invalid JSON response from REST API, Please check mlspl.log for more details.")


def copy_model_to_staging(src_model_name, searchinfo, dest_dir_path=None):
    """
    given a model name and space info, disk copy the model file to a destined directory with a new model name

    Args:
        src_model_name (str): source model name
        searchinfo (dict): searchinfo of the model owner
        dest_dir_path (str): destination path 

    Returns:
        filepath (str): the file path in staging directory
    """
    if dest_dir_path is None:
        dest_dir_path = get_staging_area_path()
    src_model_filename = model_name_to_filename(src_model_name)
    reply = get_lookup_file_from_searchinfo(src_model_filename, searchinfo, namespace='user')

    content = parse_reply_for_rest(reply)
    src_model_filepath = get_file_path_from_content(content)
    dest_model_filepath = os.path.join(dest_dir_path, src_model_filename)
    try:
        shutil.copy2(src_model_filepath, dest_model_filepath)
    except Exception as e:
        logger.debug(str(e))
        raise Exception('Failed to disk copy model %s to the staging area.' % src_model_filepath)

    return dest_model_filepath
