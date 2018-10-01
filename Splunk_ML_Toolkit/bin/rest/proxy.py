import logging
import json
import httplib
import abc
from util.rest_proxy import SplunkRestProxy
from util.rest_url_util import make_splunk_url, make_kvstore_url

import cexc
logger = cexc.get_logger(__name__)

class SplunkRestException(Exception):
    """
    Takes an error reply from rest bouncer and serialize to a http response
    """

    def __init__(self, reply):
        super(SplunkRestException, self).__init__(reply.get('content', ''))
        self.reply = reply

    def get_raw_reply(self):
        return self.reply

    def to_http_response(self):
        return {
            'payload': self.reply.get('content', ''),
            'status': self.reply.get('status', httplib.INTERNAL_SERVER_ERROR)
        }

class SplunkRestProxyException(Exception):
    """
    Custom exception that can be serialized to a http response
    """

    def __init__(self, message, level, status_code=500):
        super(SplunkRestProxyException, self).__init__(message)
        self.status_code = status_code
        self.level = self.levelNames[level]

    levelNames = {
        logging.ERROR : 'ERROR',
        logging.WARNING : 'WARN',
        logging.INFO : 'INFO'
    }

    def to_json(self):
        return {"messages": [{"type": self.level, "text": self.message}]}

    def to_http_response(self):
        return {
            'payload': json.dumps(self.to_json()),
            'status': self.status_code,
        }

class SplunkRestEndpointProxy(object):
    """
    Abstracted API for proxying request from a custom endpoint to a splunk endpoint
    """

    __metaclass__ = abc.ABCMeta

    @property
    @abc.abstractproperty
    def with_admin_token(self):
        return False

    @property
    @abc.abstractproperty
    def with_raw_result(self):
        return True

    def _split_tuple_list(self, array_list, blocked_keys=None):
        if not blocked_keys:
            blocked_keys = []
        passthrough_args = {}
        blocked_args = {}
        for r in array_list:
            key, value = tuple(r)
            if key in blocked_keys:
                blocked_args[key] = value
            else:
                passthrough_args[key] = value
        return passthrough_args, blocked_args

    def get(self, request, url_parts, with_raw_reply=False):
        """
        handles GET request from the rest handler

        Args:
            request (dict): the request passed from the rest handler
            url_parts (list): the list of url parts of the INCOMING request
            with_raw_reply (bool, optional): Defaults to False.

        Returns:
            dict: a dictionary of `status` and `payload`
        """

        return self._make_request(request, url_parts, {'method': 'GET'}, with_raw_reply)

    def post(self, request, url_parts, with_raw_reply=False):
        """
        handles POST request from the rest handler

        Args:
            request (dict): the request passed from the rest handler
            url_parts (list): the list of url parts of the INCOMING request
            with_raw_reply (bool, optional): Defaults to False.

        Returns:
            dict: a dictionary of `status` and `payload`
        """

        return self._make_request(request, url_parts, {'method': 'POST'}, with_raw_reply)

    def delete(self, request, url_parts, with_raw_reply=False):
        """
        handles DELETE request from the rest handler

        Args:
            request (dict): the request passed from the rest handler
            url_parts (list): the list of url parts of the INCOMING request
            with_raw_reply (bool, optional): Defaults to False.

        Returns:
            dict: a dictionary of `status` and `payload`
        """

        return self._make_request(request, url_parts, {'method': 'DELETE'}, with_raw_reply)

    @abc.abstractmethod
    def _convert_url_parts(self, url_parts):
        """
        Coverts the URL parts of the incoming request to something else in splunk rest api

        Mandatory override, must implement

        Args:
            url_parts (list): the list of url parts of the INCOMING request

        Raises:
            NotImplementedError: raises NotImplementedError if method is not implemented

        Return:
            list: converted URL in a list of strings
        """

        raise NotImplementedError('_convert_url_parts() is not implemented')

    def _make_url(self, rest_proxy, namespace, url_parts):
        """
        Optional override, making the proxy able to use different kind user maker

        Args:
            rest_proxy (RestProxy): the rest proxy
            namespace (string): the namespace parameter required by most of the url consturction functions
            url_parts (list): the url split as list of string from the rest call

        Returns:
            string: the full url used by rest_proxy
        """

        return make_splunk_url(rest_proxy, namespace, url_parts, [])

    def _make_request(self, request, url_parts, options, with_raw_reply):
        """
        make the request using rest proxy

        Args:
            request (dict): the original request from the rest call
            url_parts (list): the url split as list of string from the rest call
            options (dict): the default options constructed from (get, post, delete)
            with_raw_reply (bool): skip reply transformation

        Returns:
            dict: a dictionary containing `status` code and `payload` as string
        """

        rest_proxy = SplunkRestProxy.from_rest_request(request, self.with_admin_token)

        transformed_rest_options, reply_options = self._transform_request_options(options, url_parts, request)

        # re-construct the url with _convert_url_parts, and make a full url for rest_proxy
        transformed_rest_options['url'] = self._make_url(
            rest_proxy, 'user',  # TODO: make this configurable, after refactoring all the url making functions
            self._convert_url_parts(url_parts)
        )

        # if there is any existing url parameters passed in, retain those
        # and merge them with any other url parameters produced by request transformation
        getargs_from_request = dict(request.get('query', []))
        if 'getargs' in transformed_rest_options:
            transformed_rest_options['getargs'].update(getargs_from_request)
        else:
            transformed_rest_options['getargs'] = getargs_from_request

        if self.with_raw_result:
            transformed_rest_options['rawResult'] = True

        # TODO: make this safer by validating option entries
        reply = rest_proxy.make_rest_call(**transformed_rest_options)

        # skiping the reply transformation when we need to
        if with_raw_reply:
            return reply

        return self._handle_reply(reply, reply_options, request, url_parts, options['method'])

    @abc.abstractmethod
    def _transform_request_options(self, rest_options, url_parts, request):
        """
        Mutate the `request` object, in case we need some custom modification

        Optional override, class extending SplunkRestProxy can use this method to modify request before sending

        Args:
            rest_options (dict): HTTP request config options
            url_parts (list): the list of url parts of the INCOMING request
            request (object): request object from the INCOMING http request

        Returns:
            tuple: the modified request options stored in a dictionary, and a reply options if any
        """

        return rest_options, {}

    @abc.abstractmethod
    def _handle_reply(self, reply, reply_options, request, url_parts, method):
        """
        Mutate the `reply` object returned from `rest_proxy.make_rest_call()`

        Optional override, class extending SplunkRestProxy can use this method to
        modify the reply before sending back to the client

        Args:
            reply (object): the reply from the splunk rest endpoint
            reply_options (dict): the reply options from '_transform_request_options'
            request (dict): the request from the client side.
            url_parts (list): the list of url parts of the INCOMING http request
            method (string): HTTP method in string

        Returns:
            dict: the modified reply from splunk rest endpoint
        """

        return reply

class SplunkKVStoreProxy(SplunkRestEndpointProxy):
    """
    Abstracted API for proxying request to KVStore, based on SplunkRestEndpointProxy
    """

    @abc.abstractmethod
    def _get_kv_store_collection_name(self):
        """
        Instead of overriding the whole URL, KVStore's url is predictable, we only need the name of the collection

        Raises:
            NotImplementedError: this method must be implemented
        """

        raise NotImplementedError('_get_kv_store_collection_name() is not implemented')

    def _make_url(self, rest_proxy, namespace, url_parts):
        """
        Make the kvstore url

        API: see SplunkRestEndpointProxy._make_url()
        """

        return make_kvstore_url(rest_proxy, 'app', self._get_kv_store_collection_name(), url_parts)

    def _convert_url_parts(self, url_parts):
        return []
