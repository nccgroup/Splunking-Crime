import abc
import json

from conf_loader import RestLoadingStrategy
from btool_proxy import BtoolProxy
from util.searchinfo_util import get_user_and_roles_from_searchinfo
import cexc


logger = cexc.get_logger(__name__)


def get_content_item_name(scoring, item):
    """Get the content item from the scoring dict returned from the Splunk REST API.

    Args:
        scoring (dict): e.g.
            {
                'content': {
                    'package': 'scorings',
                    'module': 'classification',
                    'class': 'AccuracyScoring',
                    ...
                },
                ...
            }
        item (str): name of the item
    Returns:
        (str): the content item, e.g. package, module, class name for the scoring entry.
    """
    try:
        return scoring['content'][item].encode('utf-8')
    except KeyError as e:
        logger.debug(e)
        err_msg = 'scorings.conf file does not contain the required {} attribute for scoring method {}'
        raise RuntimeError(err_msg.format(item, scoring['name']))


class ScoringLoader(object):
    """Class to use for loading conf items with the given conf loading strategy.
    """
    def __init__(self, scoring_loading_strategy):
        self.scoring_loading_strategy = scoring_loading_strategy

    def load_scorings(self):
        return self.scoring_loading_strategy.load_scorings()


class ScoringLoadingStrategy(object):
    """Interface for scoring loading strategy

    load_scorings() must be implemented by subclasses.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def load_scorings(self):
        """Retrieve scoring names and the associated configuration from scorings.conf.

        Returns:
            (dict): scoring name to configuration content from scorings.conf mapping
        """
        raise NotImplementedError("load_scorings")


class RestScoringLoadingStrategy(ScoringLoadingStrategy):

    def __init__(self, searchinfo):
        self.conf_loader = RestLoadingStrategy(conf_name='scorings', searchinfo=searchinfo)

    @staticmethod
    def get_scoring_names_and_infos_from_rest_resp(resp):
        """Parse the raw REST response for scoring configuration and return the
        scoring names from it.

        Args:
            resp (dict): return value of get_scorings_from_splunk().

        Returns:
            (dict): e.g.
                {
                    'accuracy_score': {
                        'app': 'Splunk_ML_Toolkit',
                        'disabled': False,
                        'package': 'scorings'
                        'module': 'classification',
                        'class': 'AccuracyScoring',
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
            (scoring['name'].encode('utf-8'),
             {
                 'app': scoring['acl']['app'].encode('utf-8'),
                 'disabled': scoring['content']['disabled'],
                 'package': get_content_item_name(scoring, 'package'),
                 'module': get_content_item_name(scoring, 'module'),
                 'class': get_content_item_name(scoring, 'class')
             })
            for scoring in content['entry'] if scoring['name'] != default_stanza
        )

    def load_scorings(self):
        """Load the list of scorings from scorings.conf file.

        Returns:
            (dict): mapping of scoring name to a dict containing information about
                the scoring (app, disabled, package, module, class)
        """
        resp = self.conf_loader.load_conf()
        if not resp:
            logger.debug('Failed to load scorings.conf using REST API')
            return {}

        return self.get_scoring_names_and_infos_from_rest_resp(resp)


class BtoolScoringLoadingStrategy(ScoringLoadingStrategy):
    """Load scoring configuration using the Splunk btool command.

    This is necessary on indexers since Splunk REST API does not find configuration
    files in the search bundle during a distributed search.

    """
    def __init__(self, searchinfo):

        self.proxy = BtoolProxy(
            users_and_roles=get_user_and_roles_from_searchinfo(searchinfo=searchinfo),
            app=searchinfo['app'],
            # bundle_path is optional
            target_dir=searchinfo.get('bundle_path')
        )

    def load_scorings(self):
        """Use this method in terms of streaming apply mode, it calls a utility
        function to get scorings from btool proxy, and merge them based on the roles.

        Returns:
            scoring_names (dict): mapping of scoring name to a dict containing
                information about the scoring (app, disabled, package, module, class)
        """
        scorings = self.proxy.get_scorings()

        return dict(
            (
                scoring.encode('utf-8'),
                {
                    'app': self.proxy.app_name_from_conf_path(scorings[scoring]['conf_path']),
                    'disabled': scorings[scoring].get('disabled', False),
                    'package': scorings[scoring]['args']['package'],
                    'module': scorings[scoring]['args']['module'],
                    'class': scorings[scoring]['args']['class']
                }
            ) for scoring in scorings
        )
