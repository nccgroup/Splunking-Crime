import json

from rest_proxy import rest_proxy_from_searchinfo
import cexc

logger = cexc.get_logger(__name__)


class RestLoadingStrategy(object):
    """
    Load conf file using the Splunk REST API.

    """
    def __init__(self, conf_name, searchinfo):
        """ Initializer.

        Args:
            conf_name (str): the name of the conf file to load, which will be used in the url:
                splunkd_uri/user/app/configs/conf-{conf_name}?output_mode=json&count=-1
            searchinfo (dict): this is the search info returned from the search
        """

        self.conf_name = conf_name
        self.proxy = rest_proxy_from_searchinfo(searchinfo)

    def get_response(self):
        """
        Get the raw response from the Splunk REST URL.

        Returns:
            (dict): e.g.
                {'content':
                    "{
                        'entry': [
                            {
                                'name': 'ACF',
                                'acl': {
                                    'app': 'Splunk_ML_Toolkit',
                                    ...
                                },
                                'content': {
                                    'disabled': false,
                                    'package': 'algos'
                                },
                                ...
                            },
                            ...
                        ]
                    }",
                    ...
                }
        """
        url = '{uri}/{namespace}/nobody/{app}/configs/conf-{conf_name}?output_mode=json&count=-1'.format(
            uri=self.proxy.splunkd_uri,
            namespace=self.proxy.name_space_str,
            app=self.proxy.splunk_app,
            conf_name=self.conf_name,
        )
        resp = self.proxy.make_rest_call(
            method='GET',
            url=url
        )
        return resp

    def get_defaults(self):
        """This method returns the values of the default stanza.

        Due to the fact that the configs/conf-something endpoint will not return values from the
        default stanza, this method will first get a list of the available keys, then loop through
        the properties endpoints to get the values and return them.

        Returns:
            defaults (dict): the dictionary mapping of key to value for the default stanza
        """

        base_url_format = '{uri}/{namespace}/nobody/{app}/properties/{conf_name}/{stanza}'
        ending = '?output_mode=json&count=-1'

        base_url = base_url_format.format(
            uri=self.proxy.splunkd_uri,
            namespace=self.proxy.name_space_str,
            app=self.proxy.splunk_app,
            conf_name=self.conf_name,
            stanza='default'
        ) + ending

        defaults_resp = self.proxy.make_rest_call(
            method='GET',
            url=base_url
        )

        defaults_resp_content = json.loads(defaults_resp['content'])

        if 'entry' not in defaults_resp_content:
            logger.debug('Invalid JSON response from REST API.')
            return {}

        defaults = {item['name']: item['content']  for item in defaults_resp_content['entry']}

        return defaults



    def load_conf(self):
        """
        Load the conf file from the rest endpoint.

        Returns:
            resp (dict): response from endpoint
        """
        resp = self.get_response()
        if not resp:
            logger.debug('Failed to load {conf} using REST API'.format(conf=self.conf_name))
            return {}

        return resp
