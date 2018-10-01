
from util.rest_url_util import make_kvstore_url

def load_collection_from_rest(namespace, collection_name, rest_proxy):
    """
    Load the contents of a single collection under a single namespace from REST

    Args:
        namespace (str): "user" or "app"
        collection_name (str): the name of the KVStore collection to load
        rest_proxy (SplunkRestProxy): splunk rest bouncer wrapper

    Returns:
        reply (str): a reply from the rest proxy
    """

    url = make_kvstore_url(rest_proxy, namespace, collection_name)
    reply = rest_proxy.make_rest_call('GET', url, postargs={})

    return reply
