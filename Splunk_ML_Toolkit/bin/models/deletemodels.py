import os

import cexc

from util import models_util, rest_url_util
from util.constants import DEFAULT_LOOKUPS_DIR
from util.lookups_util import file_name_to_path, parse_model_reply
from util.rest_proxy import rest_proxy_from_searchinfo


logger = cexc.get_logger(__name__)


def delete_model_with_splunk_rest(model_name, searchinfo=None, namespace=None):
    file_name = models_util.model_name_to_filename(model_name)
    logger.debug('Deleting model: %s' % file_name)
    rest_proxy = rest_proxy_from_searchinfo(searchinfo)
    url = rest_url_util.make_get_lookup_url(rest_proxy, namespace=namespace, lookup_file=file_name)
    reply = rest_proxy.make_rest_call('DELETE', url)
    parse_model_reply(reply)


def delete_model(model_name, searchinfo=None, namespace=None,
                 model_dir=DEFAULT_LOOKUPS_DIR, tmp=False):
    if not tmp:
        delete_model_with_splunk_rest(model_name, searchinfo, namespace)
    else:
        delete_model_from_disk(model_name, model_dir, tmp)


def delete_model_from_disk(model_name, model_dir=DEFAULT_LOOKUPS_DIR, tmp=False):
    path = file_name_to_path(models_util.model_name_to_filename(model_name, tmp), model_dir)
    os.unlink(path)