import copy
import urllib


def make_splunk_url(splunk_proxy, namespace, extra_url_parts=None, url_params=None):
    """
    Compose a generic Splunk REST URL.

    Args:
        splunk_proxy (SplunkRestProxy): splunk rest bouncer wrapper
        namespace (str): namespace, either 'user', 'app' or 'global'
        extra_url_parts (array): an array of strings for extra url path parts, ie. ['users', '1'] becomes 'users/1'
        url_params (array): an array of tuples representing URL params, ie. [(count, -1)]

    Returns:
        base_url (str): The base Splunk REST, namespaced to the user/app
    """
    # note that we also accept global in this function.
    # This is because in listmodels, the returned models need to be loaded
    # using load_model function with namespace specified. Since we are getting
    # namespace info from Splunk for each model, it can be global (set from UI).
    # Therefore, we need to support global in make_lookup_url so that we won't
    # throw at load_model time when Splunk tags a certain model as global.

    if extra_url_parts is None:
        extra_url_parts = []

    if url_params is None:
        url_params = []

    if namespace in ['app', 'global']:
        user = 'nobody'
    elif namespace == 'user':
        user = splunk_proxy.splunk_user
    elif namespace == '-':
        user = namespace
    else:
        raise RuntimeError('You may only specify namespace "app"')

    base_url_parts = [
        splunk_proxy.splunkd_uri,
        splunk_proxy.name_space_str,
        user,
        splunk_proxy.splunk_app
    ]

    base_url = '/'.join(base_url_parts + extra_url_parts)

    url_param_string = urllib.urlencode(url_params)

    if url_param_string:
        base_url += '?' + url_param_string

    return base_url


def make_lookup_url(splunk_proxy, namespace, lookup_file=None, url_params=None):
    """
    Compose url for data/lookup_table_files endpoint.

    Args:
        splunk_proxy (SplunkRestProxy): splunk rest bouncer wrapper
        namespace (str): namespace, either 'user', 'app' or 'global'
        lookup_file (str): Optional. Target lookup_file for the endpoint.
        url_params (array): an array of tuples representing URL params, ie. [(count, -1)]


    Returns:
        base_url (str): the base Splunk /lookup-table-files URL for the current namespace
    """

    url_parts = ['data', 'lookup-table-files']
    if lookup_file is not None:
        url_parts.append(lookup_file)

    url = make_splunk_url(splunk_proxy, namespace, extra_url_parts=url_parts, url_params=url_params)

    return url


def make_get_lookup_url(splunk_proxy, namespace, lookup_file=None, url_params=None):
    """
    Compose url for GET operation to data/lookup_table_files endpoint.

    Args:
        splunk_proxy (SplunkRestProxy): splunk rest bouncer wrapper
        namespace (str): namespace, either 'user', 'app' or 'global'
        lookup_file (str): Optional. Target lookup_file for the endpoint.
        url_params (array): an array of tuples representing URL params, ie. [(count, -1)]

    Returns:
        url (str): the Splunk /lookup-table-files URL for GET commands specifically
    """

    if url_params is None:
        url_params = []

    # copy url_params because we need to modify it and we want to leave the passed-in argument untouched
    url_params_copy = copy.deepcopy(url_params)
    url_params_copy.append(('output_mode', 'json'))

    url = make_lookup_url(splunk_proxy, namespace, lookup_file, url_params=url_params_copy)

    return url


def make_kvstore_url(splunk_proxy, namespace, collection, extra_url_params=None):
    """
    Compose a URL for querying the KVStore collection

    Args:
        splunk_proxy (SplunkRestProxy): splunk rest bouncer wrapper
        namespace (str): namespace, either 'user' or 'app'
        collection (str): the name of the KVStore collection to query
        extra_url_params (list of tuples): extra parameters to be added in the url
            after ?output_mode=json

    Returns:
        kvstore_url (str): The URL of the KVStore collection
    """

    if extra_url_params is None:
        extra_url_params = []
    url_parts = ['storage', 'collections', 'data', collection]

    return make_splunk_url(splunk_proxy, namespace, extra_url_parts=url_parts, url_params=extra_url_params)
