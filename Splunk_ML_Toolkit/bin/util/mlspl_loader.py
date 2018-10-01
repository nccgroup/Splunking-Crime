import abc
import json

import cexc
from btool_proxy import BtoolProxy
from searchinfo_util import get_user_and_roles_from_searchinfo, should_use_btool, validate_searchinfo_for_btool

logger = cexc.get_logger(__name__)

from conf_loader import RestLoadingStrategy

class MLSPLConf(object):
    """Utility class for loading and retrieving values from mlspl.conf."""
    def __init__(self, searchinfo):
        if should_use_btool(searchinfo):
            is_valid, err = validate_searchinfo_for_btool(searchinfo)
            if is_valid:
                self.strategy = MLSPLBtoolLoadingStrategy(searchinfo)
            else:
                raise RuntimeError('Failed to load mlspl.conf on remote Splunk: %s' % err)
        else:
            self.strategy = MLSPLRestLoadingStrategy(searchinfo)

        self.stanza_mapping = self.strategy.load_stanza_mapping()

    def get_stanza(self, stanza='default'):
        default_stanza = self.stanza_mapping.get('default')
        return self.stanza_mapping.get(stanza, default_stanza)

    def get_mlspl_prop(self, name, stanza='default', default=None):
        """ Utility to retrieve a specify property."""
        default_stanza = self.stanza_mapping.get('default')
        default_prop_value = default_stanza.get(name, default)
        if stanza == 'default':
            return default_prop_value

        other_stanza = self.stanza_mapping.get(stanza, default_stanza)
        return other_stanza.get(name, default_prop_value)


class MLSPLConfLoadingStrategy(object):
    """Abstract class (patterned after algo_loader) to ensure the presence
    of the load_stanza_mapping method. """
    __metaclass__ = abc.ABCMeta

    def load_stanza_mapping(self):
        """ Should return a dictionary of stanza to attribute mappings."""
        raise NotImplementedError

class MLSPLRestLoadingStrategy(MLSPLConfLoadingStrategy):
    """Strategy for loading the conf from REST endpoint."""
    def __init__(self, searchinfo):
        self.conf_loader = RestLoadingStrategy(conf_name='mlspl', searchinfo=searchinfo)

    def load_stanza_mapping(self):
        logger.debug('Loading mlspl.conf from REST.')
        response = self.conf_loader.load_conf()
        content = json.loads(response['content'])

        defaults = self.conf_loader.get_defaults()
        stanza_mapping = {'default': defaults}

        if 'entry' not in content.keys():
            return stanza_mapping

        for stanza in content['entry']:
            stanza_mapping[stanza['name']] = {
                key: value for key, value in stanza['content'].iteritems()
                if 'eai:' not in key}

        return stanza_mapping

class MLSPLBtoolLoadingStrategy(MLSPLConfLoadingStrategy):
    """Strategy for loading the conf from btool utility."""
    def __init__(self, searchinfo):

        self.proxy = BtoolProxy(
            users_and_roles=get_user_and_roles_from_searchinfo(searchinfo=searchinfo),
            app=searchinfo['app'],
            # bundle_path is optional
            target_dir=searchinfo.get('bundle_path')
        )

    def load_stanza_mapping(self):
        logger.debug('Loading mlspl.conf via btool.')
        return self.proxy.get_mlspl_stanza_mapping()
