import abc
import json

from conf_loader import RestLoadingStrategy

from rest_proxy import rest_proxy_from_searchinfo
from btool_proxy import BtoolProxy
import cexc

from util.searchinfo_util import get_user_and_roles_from_searchinfo

logger = cexc.get_logger(__name__)

#######################################################################################
# Strategy pattern to allow dynamically picking the algorithm loading method at runtime
#######################################################################################


def get_package_name(algo):
    """
    Get the package name from the algorithm dict returned from the Splunk REST API.

    Args:
        algo (dict): e.g.
            {
                'content': {
                    'package': 'algos',
                    ...
                },
                ...
            }

    Returns:
        (str): package name for the algorithm entry.

    """
    try:
        return algo['content']['package'].encode('utf-8')
    except KeyError:
        raise RuntimeError(
            'algos.conf file does not contain the required "package" attribute for algorithm %s' % algo['name'])


class AlgoLoader(object):
    """
    Class to use for loading conf items with the given conf loading strategy.

    """
    def __init__(self, algo_loading_strategy):
        self.algo_loading_strategy = algo_loading_strategy

    def load_algos(self):
        return self.algo_loading_strategy.load_algos()


class AlgoLoadingStrategy(object):
    """
    Interface for algorithm loading strategy

    load_algos() must be implemented by subclasses.

    """
    __metaclass__ = abc.ABCMeta

    # By using the abc module's abstractmethod, we can prevent any subclass from
    # being instantiated without implementing the abstract method. Normally in Python,
    # the missing method must be called before we can detect it.
    @abc.abstractmethod
    def load_algos(self):
        """
        Retrieve algorithm names and the associated configuration from algos.conf.

        Returns:
            (dict): algorithm name to configuration content from algos.conf mapping
        """
        # This should never really execute since abc.abstractmethod
        # will guard against this method being called.
        raise NotImplementedError("load_algos")


class RestAlgoLoadingStrategy(AlgoLoadingStrategy):

    def __init__(self, searchinfo):
        self.conf_loader = RestLoadingStrategy(conf_name='algos', searchinfo=searchinfo)

    @staticmethod
    def get_algo_names_from_rest_resp(resp):
        """
        Parse the raw REST response for algorithm configuration and return the algorithm names from it.

        Args:
            resp (dict): Return value of get_algos_from_splunk().

        Returns:
            (dict): e.g.
                {
                    'ACF': {
                        'app': 'Splunk_ML_Toolkit',
                        'disabled': False,
                        'package': 'algos'
                    },
                    ...
                }

        """
        content = json.loads(resp['content'])
        default_stanza = 'default'

        if not content.get('entry'):
            logger.debug('Invalid JSON response from REST API')
            return {}

        return dict(
            (algo['name'].encode('utf-8'), {
                'app': algo['acl']['app'].encode('utf-8'),
                'disabled': algo['content']['disabled'],
                'package': get_package_name(algo)})
            for algo in content['entry']
            if algo['name'] != default_stanza
        )

    def load_algos(self):
        """
        Load the list of algorithms from algos.conf file.

        Returns:
            algo_names (dict): mapping of algorithm name to a dict containing
                               information about the algo (app, disabled, package)
        """
        resp = self.conf_loader.load_conf()
        if not resp:
            logger.debug('Failed to load algos.conf using REST API')
            return {}

        return self.get_algo_names_from_rest_resp(resp)


class BtoolAlgoLoadingStrategy(AlgoLoadingStrategy):
    """
    Load algorithm configuration using the Splunk btool command.

    This is necessary on indexers since Splunk REST API does not find configuration files in the search bundle
    during a distributed search.

    """
    def __init__(self, searchinfo):

        self.proxy = BtoolProxy(
            users_and_roles=get_user_and_roles_from_searchinfo(searchinfo=searchinfo),
            app=searchinfo['app'],
            # bundle_path is optional
            target_dir=searchinfo.get('bundle_path')
        )

    def load_algos(self):
        """
        Use this method in terms of streaming apply mode, it calls a utility function to get algorithms from btool
        proxy, and merge them based on the roles.

        Returns:
            algo_names (dict): mapping of algorithm name to a dict containing
                               information about the algo (app, disabled, package)
        """
        algos = self.proxy.get_algos()

        return dict(
            (
                algo.encode('utf-8'),
                {
                    'app': self.proxy.app_name_from_conf_path(algos[algo]['conf_path']),
                    'disabled': algos[algo].get('disabled', False),
                    'package': algos[algo]['args']['package'],
                }
            ) for algo in algos
        )

