import json
import httplib
import uuid
import logging
import copy

from util import rest_url_util
from util.base_util import is_valid_identifier
from util.constants import EXPERIMENT_MODEL_PREFIX
from util.searchinfo_util import searchinfo_from_request
from util.models_util import (
    get_model_list_by_experiment,
)
from util.lookup_exceptions import ModelNotFoundException

from util.rest_proxy import rest_proxy_from_searchinfo
from util.experiment_util import get_experiment_draft_model_name
from models.copymodels import copy_model
from experiment.experiment_validation import validate_experiment_form_args
from rest.proxy import SplunkRestEndpointProxy, SplunkRestProxyException, SplunkRestException

import cexc
logger = cexc.get_logger(__name__)

MODEL_NAME_ATTR = 'mltk_model_name'


class ExperimentStore(SplunkRestEndpointProxy):
    """
    API for experiment's conf storage backend
    """

    URL_PARTS_PREFIX = ['configs', 'conf-experiments']
    JSON_OUTPUT_FLAG = ('output_mode', 'json')

    _with_admin_token = False
    _with_raw_result = True

    @property
    def with_admin_token(self):
        return self._with_admin_token

    @property
    def with_raw_result(self):
        return self._with_raw_result

    def _convert_url_parts(self, url_parts):
        """
        - Mandatory overridden
        - see SplunkRestEndpointProxy._convert_url_parts()
        """

        return self.URL_PARTS_PREFIX + url_parts

    def _transform_request_options(self, rest_options, url_parts, request):
        """
        - Overridden from SplunkRestEndpointProxy
        - Handling experiment specific modification/handling of the request before
        sending the request to conf endpoint
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

        reply_options = {}
        # for GET/DELETE request, we just want to append output=json to the existing getargs
        if rest_options['method'] == 'GET' or rest_options['method'] == 'DELETE':
            rest_options['getargs'] = dict(rest_options.get('getargs', []) + [self.JSON_OUTPUT_FLAG])

        if rest_options['method'] == 'DELETE':
            self._delete_models(request, url_parts)
        # for POST request, we do validation before proxying the request
        if rest_options['method'] == 'POST':
            postargs, blockedargs = self._split_tuple_list(request.get('form', []), ["promoteModel"])
            try:
                validate_experiment_form_args(postargs)
            except Exception as e:
                logger.error(str(e))
                raise SplunkRestProxyException('Can not validate experiment', logging.ERROR, httplib.BAD_REQUEST)

            postargs['output_mode'] = 'json'

            if blockedargs.get('promoteModel', None):
                reply_options['promote_model'] = True

            if len(url_parts) == 0:
                # this is a create POST
                experiment_uuid = str(uuid.uuid4()).replace('-', '') # removing '-' due to model name constraints
                postargs['name'] = experiment_uuid

            rest_options['postargs'] = postargs

        return rest_options, reply_options

    def clone_experiment_models(self, experiment_fetch_reply, request, url_parts):
        """
        the function performs the "clone models" operation for an experiment, the experiment info is from 'experiment_fetch_reply'
        Args:
            experiment_fetch_reply (dict) : the reply from a mltk/experiments/<guid> POST request
            request (dict) : the request object
            url_parts (list) : a subset of the url, here is a list of length 1 which contains experiment id.

        Returns:
            (dict) a dictionary of `status` and `payload`
        """
        target_info = json.loads(request.get('payload', {}))
        if target_info.viewkeys() == {'app', 'name'}:
            target_model_name = target_info.get('name')
            if not is_valid_identifier(target_model_name):
                raise SplunkRestProxyException('Invalid model name "%s"' % target_model_name, logging.ERROR, httplib.BAD_REQUEST)

            source_searchinfo = searchinfo_from_request(request)
            target_searchinfo = copy.deepcopy(source_searchinfo)
            target_searchinfo['app'] = target_info.get('app')

            clone_experiment_model_callback = self._clone_experiment_model_callback(source_searchinfo, target_searchinfo,
                                                                                    target_model_name, url_parts[0],
                                                                                    reply_handler=self._add_model_name_to_reply)

            reply_list = self._handle_all_experiment_models(experiment_fetch_reply, clone_experiment_model_callback)

            formatted_reply = self._handle_clone_reply(reply_list)

            return self._handle_reply(formatted_reply, {}, request, url_parts, 'POST')

        else:
            raise SplunkRestProxyException('This handler only supports "app" and "name" as arguments',
                                           logging.ERROR, httplib.BAD_REQUEST)

    @staticmethod
    def _handle_all_experiment_models(reply, callback_handler):
        """
        pass the callback_handler to each model for all search stages of an experiment, exit if handler returns failure.
        Args:
            reply (dict) : the reply object of an experiment GET request.
            callback_handler (func) : a callback handler for each model, it should return the reply of a REST request.

        Returns:
            (list): a list of replies from each handlers
        """

        try:
            content = json.loads(reply['content'])
            entries = content['entry']

            # a cache that stores the reply from the callback of each model
            reply_list = []

            for entry in entries:
                ss_json = entry['content']['searchStages']
                search_stages = json.loads(ss_json)
                for search_stage in search_stages:
                    model_name = search_stage.get('modelName')
                    if model_name is not None:
                        reply = callback_handler(model_name)
                        reply_list.append(reply)
                        # if any of the reply is not successful, stop the process and return the current reply list
                        if not reply.get('success'):
                            return reply_list

            return reply_list

        except Exception:
            cexc.log_traceback()
            raise Exception("Invalid JSON response from REST API, Please check mlspl.log for more details.")

    @staticmethod
    def _clone_experiment_model_callback(source_searchinfo, target_searchinfo, target_base_model_name, experiment_id, reply_handler=None):
        """
        a closure function that take the necessary info to clone a model inside an experiment to designated namespace.
        Args:
            source_searchinfo (dict) : the searchinfo from the source experiment request.
            target_searchinfo (dict) : the searchinfo for the target space for the clone destination
            target_base_model_name (str) : new name of the destination model(s)
            experiment_id (str) : id of the source experiment
            reply_handler (func): handler for each reply, optional.

        Returns:
            (func) a callback to copy_model that takes model_name <string> as an argument
        """

        def callback(source_model_name):
            # only replace <guid> with the new model name but keeps the suffixes like "_StandardScaler_0"
            target_model_name = source_model_name.replace(EXPERIMENT_MODEL_PREFIX + experiment_id,
                                                          target_base_model_name, 1)

            try:
                raw_reply = copy_model(source_searchinfo, source_model_name, target_searchinfo, target_model_name)
            except SplunkRestException as e:
                raw_reply = e.get_raw_reply()
            return reply_handler(raw_reply, target_model_name) if reply_handler else raw_reply

        return callback

    @staticmethod
    def _add_model_name_to_reply(raw_reply, model_name):
        """
        a util function for customize the reply from Splunk lookup-table-file REST endpoint.
            1. if it's a success REST reply, insert type='INFO' and add custom attribute `mltk_model_name` to the
            `messages` parts.
            2. if it's not a success REST reply, only add the custom attribute.
        Args:
            raw_reply (dict) : a dict of raw reply from Splunk lookup-table-file request
            model_name: the model name which needs to be inserted.

        Returns:
            (dict) modified reply.
        """

        reply = copy.deepcopy(raw_reply)
        try:
            content = json.loads(raw_reply['content'])
            messages = content['messages']

            if len(messages) > 0:
                messages[0][MODEL_NAME_ATTR] = model_name
            else:
                message_success = {
                    'type': "INFO",
                    'text': '',
                    MODEL_NAME_ATTR: model_name,
                }
                messages.append(message_success)

            reply['content'] = json.dumps(content)

        except Exception as e:
            cexc.log_traceback()
            raise Exception("Invalid JSON response from REST API, Please check mlspl.log for more details.")

        return reply

    @staticmethod
    def _promote_draft_model_callback(searchinfo):
        """
        a closure function that take the necessary info to clone a model within the same namespace.
        Args:
            searchinfo:

        Returns:
            (func) a callback to copy_model that takes model_name <string> as an argument
        """

        def callback(model_name):
            draft_model_name = get_experiment_draft_model_name(model_name)

            try:
                return copy_model(searchinfo, draft_model_name, searchinfo, model_name)
            except ModelNotFoundException as e:
                cexc.log_traceback()
                logger.error(e)
                raise SplunkRestProxyException("%s: %s" % (str(e), draft_model_name), logging.ERROR, httplib.NOT_FOUND)

        return callback

    @staticmethod
    def _delete_models(request, url_parts):
        if len(url_parts) == 1:
            try:
                searchinfo = searchinfo_from_request(request)
                rest_proxy = rest_proxy_from_searchinfo(searchinfo)
                model_list = get_model_list_by_experiment(rest_proxy, namespace='user', experiment_id=url_parts[0])
                for model_name in model_list:
                    url = rest_url_util.make_get_lookup_url(rest_proxy, namespace='user', lookup_file=model_name)
                    reply = rest_proxy.make_rest_call('DELETE', url)
            except Exception as e:
                cexc.log_traceback()
                pass

    @staticmethod
    def _handle_clone_reply(replies):
        """
        merge the 'messages' part of all replies into the last reply.
        Args:
            replies (list) : the replies from all splunk REST requests, with ['content']['messages'] modified by _clone_experiment_model_callback()

        Returns:
            (dict) a modified version of mltk clone reply, trimming all attributes in `content` except 'messages'.
        """

        messages = []
        merged_reply = None  # set None to throw exception if replies is empty
        try:
            for reply in replies:
                messages.append(json.loads(reply['content'])['messages'][0])
                merged_reply = reply
                if not reply['success']:
                    break

            merged_reply['content'] = json.dumps({'messages': messages})
        except Exception as e:
            cexc.log_traceback()
            raise Exception("Invalid JSON response from REST API, Please check mlspl.log for more details.")

        return merged_reply

    def _handle_reply(self, reply, reply_options, request, url_parts, method):
        """
        - Overridden from SplunkRestEndpointProxy
        - Replace '/configs/conf-experiments' in the reply with '/mltk/experiments'

        Args:
            reply (dict): the reply we got from '/configs/conf-experiments'
            reply_options (dict): the reply options from '_transform_request_options'
            url_parts (list): a list of url parts without /mltk/experiments
            method (string): original request's method

        Returns:
            reply: reply from input after the filtering
        """

        def deproxy(string):
            # replace '/configs/conf-experiments' with '/mltk/experiments'
            return string.replace('/%s' % '/'.join(self.URL_PARTS_PREFIX), '/mltk/experiments')

        content = json.loads(reply.get('content'))

        if content.get('origin'):
            content['origin'] = deproxy(content['origin'])

        if content.get('links'):
            for key, value in content['links'].iteritems():
                content['links'][key] = deproxy(value)

        if content.get('entry'):
            entry = content['entry']
            for item in entry:
                item['id'] = deproxy(item['id'])
                for key, value in item['links'].iteritems():
                    item['links'][key] = deproxy(value)

        # promote the draft model to production.
        if reply_options.get('promote_model') and method == 'POST' and reply.get('status') == httplib.OK:
            searchinfo = searchinfo_from_request(request)

            promote_draft_model_callback = self._promote_draft_model_callback(searchinfo)
            self._handle_all_experiment_models(reply, promote_draft_model_callback)

        return {
            'status': reply.get('status', httplib.OK),
            'payload': json.dumps(content)
        }
