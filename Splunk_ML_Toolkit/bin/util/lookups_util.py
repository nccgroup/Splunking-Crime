import copy
import csv
import json
import os

import cexc
from util import btool_util, rest_url_util
from util.constants import DEFAULT_LOOKUPS_DIR, CSV_FILESIZE_LIMIT
from util.file_util import file_exists
from util.lookup_exceptions import (
    ExperimentNotAuthorizedError,
    ExperimentNotFoundError,
    LookupNotAuthorizedException,
    LookupAlreadyExists,
    LookupNotFoundException,
    ModelNotFoundException,
    ModelNotAuthorizedException,
    )
from util.param_util import missing_keys_in_dict
from util.rest_proxy import rest_proxy_from_searchinfo
from util.rest_url_util import make_get_lookup_url
from util.searchinfo_util import should_use_btool, validate_searchinfo_for_btool

logger = cexc.get_logger(__name__)
messages = cexc.get_messages_logger()


def lookups_parse_reply(reply=None):
    if reply:
        try:
            content = reply.get('content')
            if not content:
                raise RuntimeError("No content in reply")
            json_content = json.loads(content)
            if reply['success']:
                return json_content
            else:
                error_type = reply.get('error_type')
                error_text = json_content.get('messages')[0].get('text')
                if error_type is None:
                    # clunky but there is no other way
                    if "already exists" in error_text:
                        raise LookupAlreadyExists()
                    else:
                        raise RuntimeError(error_text)
                elif error_type == 'ResourceNotFound':
                    raise LookupNotFoundException()
                elif error_type == 'AuthorizationFailed':
                    raise LookupNotAuthorizedException()
                else:
                    raise RuntimeError(error_type + ', ' + error_text)
        except Exception as e:
            # TODO: If this is a general util function now,
            # better to delegate error handling to the user of the util function.
            logger.debug(e.message)
            logger.debug(reply)
            raise e
    else:
        raise RuntimeError("No reply received")


def parse_experiment_reply(reply=None):
    try:
        return lookups_parse_reply(reply)
    except LookupNotFoundException as e:
        raise ExperimentNotFoundError(exception=e)
    except LookupNotAuthorizedException as e:
        raise ExperimentNotAuthorizedError(exception=e)


def parse_model_reply(reply=None):
    try:
        return lookups_parse_reply(reply)
    except LookupNotFoundException:
        raise ModelNotFoundException()
    except LookupNotAuthorizedException:
        raise ModelNotAuthorizedException()


def build_lookups_query_params(query_params, username):
    """
    Adds additional filtering to the query received from REST to show only those the current user has access to

    Args:
        query_params (list): a list of tuples representing URL params, ie. [(count, -1)]
        username (string): the current user

    Returns:
        query_params_copy (list): a copy of query_params augmented with additional filtering
    """
    query_params_copy = copy.deepcopy(query_params)

    # based on availableWithUserWildCardSearchString() from SplunkWebCore's SplunkDsBase.js
    escaped_username = json.dumps(username)
    user_filter = '((eai:acl.sharing="user" AND eai:acl.owner=%s) OR (eai:acl.sharing!="user"))' % escaped_username
    query_params_copy.append(('search', user_filter))

    return query_params_copy


def get_lookups_from_splunk(searchinfo, namespace, cb_reply_parser, query_params):
    """
    Gets a list of models from Splunk's /lookup-table-files endpoint

    Args:
        searchinfo (dict): a seachinfo object
        namespace (string): which namespace to get lookups from
        cb_reply_parser(function): a callback to process the reply from splunk
        query_params (list): a list of tuples representing URL params, ie. [(count, -1)]

    Returns:
        lookup_files (dict): a map from a lookup file's location on disk to info about it
    """

    rest_proxy = rest_proxy_from_searchinfo(searchinfo)

    # searchinfo can be null, in which case we should fall back to the safe 'nobody' username because we can't get the user
    try:
        username = rest_proxy.splunk_user
    except AttributeError:
        username = 'nobody'

    query_params_copy = build_lookups_query_params(query_params, username)
    url = make_get_lookup_url(rest_proxy, namespace=namespace, lookup_file=None, url_params=query_params_copy)
    reply = rest_proxy.make_rest_call('GET', url)
    lookup_files = cb_reply_parser(reply)

    return lookup_files


def file_name_to_path(file_name, lookups_dir=DEFAULT_LOOKUPS_DIR):
    if file_name != os.path.basename(file_name):
        raise ValueError("Invalid filename {}".format(file_name))
    file_path = os.path.join(lookups_dir, file_name)
    return file_path


def load_lookup_file_from_disk(file_path):
    """
    parse the lookup file from the given path and return the result

    Args:
        file_path (string): the path to the lookup file

    Returns:
        lookup_data (dict): result from the csv parser
    """
    if not file_exists(file_path):
        raise RuntimeError('Not valid filepath: {}'.format(file_path))

    try:
        with open(file_path, mode='r') as f:
            reader = csv.DictReader(f)
            csv.field_size_limit(CSV_FILESIZE_LIMIT)
            lookup_data = reader.next()
    except Exception as e:
        raise RuntimeError('Error reading model file: %s, %s' % (file_path, str(e)))

    return lookup_data


def lookup_name_to_path_from_splunk(lookup_name, file_name, searchinfo, namespace=None, lookup_type=None):
    if lookup_type is None:
        raise Exception("lookup_type must be specified (`model` or `experiment`)")

    if should_use_btool(searchinfo):
        is_valid, err = validate_searchinfo_for_btool(searchinfo)
        if is_valid:
            logger.debug('Using btool to load lookup path from Splunk')
            file_path = lookup_name_to_path_distributed(lookup_name=lookup_name,
                                                        file_name=file_name,
                                                        searchinfo=searchinfo,
                                                        namespace=namespace,
                                                        lookup_type=lookup_type)
        else:
            raise RuntimeError('Failed to get lookup path: %s' % err)
    else:
        file_path = lookup_name_to_path_from_splunk_rest(file_name, searchinfo, namespace, lookup_type)

    return file_path


def get_lookup_file_from_searchinfo(file_name, searchinfo, namespace):
    """
    file a GET request to /lookup-table-files/filename endpoint, return the reply

    Args:
        file_name (str) : file name of the existing lookup file
        searchinfo (dict) : searchinfo
        namespace (str) : lookup file namespace

    Returns:
        reply (dict) : the response from GET

    """
    rest_proxy = rest_proxy_from_searchinfo(searchinfo)
    url = rest_url_util.make_get_lookup_url(rest_proxy, namespace=namespace, lookup_file=file_name)
    return rest_proxy.make_rest_call('GET', url)


def lookup_name_to_path_from_splunk_rest(file_name, searchinfo, namespace=None, lookup_type=None):
    reply = get_lookup_file_from_searchinfo(file_name, searchinfo, namespace=namespace)
    if lookup_type == 'model':
        json_content = parse_model_reply(reply)
    elif lookup_type == "experiment":
        json_content = parse_experiment_reply(reply)
    else:
        json_content = lookups_parse_reply(reply)

    return get_file_path_from_content(json_content)


def lookup_name_to_path_distributed(lookup_name, file_name, searchinfo, namespace, lookup_type):
    if namespace is None:
        namespace = 'user'

    # For distributed search, these searchinfo fields must be present.
    required_searchinfo_fields = (
        'app',
        'username',
    )
    missing_keys = missing_keys_in_dict(required_searchinfo_fields, searchinfo)
    if missing_keys:
        logger.debug('searchinfo in getinfo missing the following keys: %s', ', '.join(missing_keys))
        raise Exception("Please check mlspl.log for more details.")

    app = searchinfo['app']
    user = searchinfo['username']
    bundle = searchinfo.get('bundle_path')
    roles = searchinfo.get('roles', [])

    results = {}
    # If "app:" is used, skip the user namespace
    if namespace is 'user':
        results[user] = btool_util.get_lookups_btool(user=user, app=app, target_dir=bundle, lookup_type=lookup_type)
    for role in roles:
        results[role] = btool_util.get_lookups_btool(user=role, app=app, target_dir=bundle, lookup_type=lookup_type)

    lookup_path = get_lookup_from_btool_result(
        btool_dict=results, lookup_name=lookup_name, file_name=file_name, user=user, app=app, roles=roles,
        namespace=namespace)
    return lookup_path


def get_lookup_from_btool_result(btool_dict, lookup_name, file_name, user, app, roles, namespace):
    try:
        if namespace == 'user' and user in btool_dict and lookup_name in btool_dict[user]:
            result = btool_dict[user][lookup_name]
            user_match_str = os.path.join('users', user, app, 'lookups', file_name)
            # Here only lookups in the user namespace is checked, because there is a issue/bug with btool
            # if username is also a role name in Splunk (e.g. username=power and there is the "power" role),
            # btool might return objects that the user have no permission on but role does.
            if result.endswith(user_match_str):
                return result

        app_match_str = os.path.join('apps', app, 'lookups', file_name)
        merged_result = None
        for role in roles:
            try:
                path = btool_dict[role].pop(lookup_name)
                if path.endswith(app_match_str):
                    return path
                else:
                    # If "app:" is not used, check global namespace
                    if namespace != 'app' and (merged_result is None or merged_result < path):
                        merged_result = path
            except KeyError:
                continue  # Do Nothing, go to next item
    except Exception:
        cexc.log_traceback()
        raise Exception("Please check mlspl.log for more details.")
    return merged_result


def get_file_path_from_content(content):
    try:
        file_path = content['entry'][0]['content']['eai:data']
    except Exception as e:
        cexc.log_traceback()
        raise Exception("Invalid JSON response from REST API, Please check mlspl.log for more details.")

    return file_path
