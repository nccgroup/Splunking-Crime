import rest_bouncer
from util.searchinfo_util import searchinfo_from_request


class SplunkRestProxy(object):
    def __init__(self, uri, key, app, user):
        """Initialize the Splunk rest API proxy object.

        Args: (all the args come from search info, this proxy object is created in the chunked controller
                and passed to the processors. From the processors use self.rest_proxy to access this util)
            uri (string): Splunk URI
            key (string): Sessionkey of the current logged in user
            app (string): App namespace
            user (string): Username
        """
        # Attributes
        self.splunkd_uri = uri
        self.session_key = key
        self.splunk_app = app
        self.splunk_user = user
        self.name_space_str = 'servicesNS'

    def make_rest_call(self, method, url, postargs=None, jsonargs=None, getargs=None, rawResult=False):
        """Make rest call to Splunk rest endpoint using the bouncer.

        Args: (all the args come from search info, this proxy object is created in the chunked controller
                and passed to the processors. From the processors use self.rest_proxy to access this util)
            method (string): REST method - GET, POST and etc.
            url (string): The complete URL for making the rest call, usually is 
                            {splunk_uri}/{namespaces}/{rest_endpoint}. Refer to Docs.
            postargs (dict): POST payload that gets placed into the body of the request
                             (application/x-www-form-urlencoded).
            jsonargs (dict): JSON payload that gets placed into the body of the request (application/json).
                             If provided, takes precedence over postargs.
            getargs (dict): GET query args that get appended into the URL.
        """
        return rest_bouncer.make_rest_call(
            self.session_key,
            method,
            url,
            postargs=postargs,
            jsonargs=jsonargs,
            getargs=getargs,
            rawResult=rawResult,
        )

    @staticmethod
    def from_searchinfo(searchinfo, with_admin_token=False):
        if searchinfo is None:
            return None
        else:
            return SplunkRestProxy(
                searchinfo['splunkd_uri'],
                searchinfo['session_key'] if with_admin_token is False else searchinfo['admin_session_key'],
                searchinfo['app'],
                searchinfo['username']
            )

    @staticmethod
    def from_rest_request(request, with_admin_token=False):
        searchinfo = searchinfo_from_request(request, with_admin_token)
        return SplunkRestProxy.from_searchinfo(searchinfo, with_admin_token)


def rest_proxy_from_searchinfo(searchinfo, with_admin_token=False):
    # FIXME: get rid of this in a refactor plz
    return SplunkRestProxy.from_searchinfo(searchinfo, with_admin_token)
