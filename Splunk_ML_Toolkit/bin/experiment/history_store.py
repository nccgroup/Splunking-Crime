import httplib
import json
import time
import logging

from experiment_validation import validate_experiment_history_json
from util.searchinfo_util import searchinfo_from_request
from rest.proxy import SplunkKVStoreProxy, SplunkRestException, SplunkRestProxyException

import cexc
logger = cexc.get_logger(__name__)


class ExperimentHistoryStore(SplunkKVStoreProxy):
    """
    API for experiment history's KVstore storage
    """
    EXPERIMENT_HISTORY_COLLECTION_NAME = 'experiment_history'

    _with_admin_token = False
    _with_raw_result = False

    def __init__(self, with_admin_token):
        self._with_admin_token = with_admin_token

    @property
    def with_admin_token(self):
        return self._with_admin_token

    @property
    def with_raw_result(self):
        return self._with_raw_result

    def _get_kv_store_collection_name(self):
        """
        - Mandatory overridden
        - see SplunkKVStoreProxy._get_kv_store_collection_name()
        """
        return self.EXPERIMENT_HISTORY_COLLECTION_NAME

    def _transform_request_options(self, rest_options, url_parts, request):
        """
        - Overridden from SplunkRestEndpointProxy
        - Handling experiment specific modification/handling of the request before
        sending the request to kvstore
        - See RestProxy.make_rest_call() for a list of available items for `rest_options`

        Args:
            rest_options (dict): default rest_options constructed by the request method (get, post, delete)
            url_parts (list): a list of url parts without /mltk/experiments
            request (dict): the original request from the rest call to /mltk/experiments/*

        Raises:
            SplunkRestProxyException: some error occurred during this process

        Returns:
            options (tuple) : a two element tuple.  the first element is a dictionary that stores parameters needed by
            RestProxy.make_rest_call(), and the second element stores parameters for _handle_reply, if any.
        """

        if url_parts > 0:
            experiment_id = url_parts[0]
        else:
            raise SplunkRestProxyException('No experiment id specified', logging.ERROR, httplib.BAD_REQUEST)

        if rest_options['method'] == 'GET' or rest_options['method'] == 'DELETE':
            rest_options['getargs'] = dict(rest_options.get('getargs', []) + [("query", json.dumps({'experimentId': experiment_id}))])

        if rest_options['method'] == 'POST':
            experiment_history = json.loads(request.get('payload', '{}'))

            experiment_history["experimentId"] = experiment_id

            try:
                validate_experiment_history_json(experiment_history)

                # these are intentionally added after validation, since we don't want to allow the user to submit them
                searchinfo = searchinfo_from_request(request)
                experiment_history["_time"] = time.time()
                experiment_history["user"] = searchinfo["username"]
                experiment_history["app"] = searchinfo["app"]
            except Exception as e:
                logger.error(str(e))
                raise SplunkRestProxyException('Can not validate experiment history', logging.ERROR, httplib.BAD_REQUEST)
            rest_options['jsonargs'] = json.dumps(experiment_history)

        return rest_options, {}

    def _handle_reply(self, reply, reply_options, request, url_parts, method):
        """
        - Overridden from SplunkRestEndpointProxy

        Args:
            reply (dict): the reply we got from kvstore
            reply_options (dict): the reply options from '_transform_request_options'
            request (dict): the request from rest endpoint
            url_parts (list): a list of url parts without /mltk/experiments
            method (string): original request's method

        Returns:
            reply: reply from input after the filtering
        """

        return {
            'status': reply.get('status', httplib.OK),
            'payload': reply.get('content', '')
        }
