from btool_util import get_algos_btool, get_mlspl_btool, get_scorings_btool
from util.base_util import get_apps_path


class BtoolProxy(object):
    """
    Thin object wrapper around btool_utils for getting the algo configuration.
    """
    def __init__(self, users_and_roles, app, target_dir):
        self.users_and_roles = users_and_roles
        self.app = app
        self.target_dir = target_dir

    def get_algos(self):
        """ Get algo information for all users and roles

        Returns:
            results (dict): Return value of get_algos_btool() for all users and roles.

        """
        algos = {}

        for user_or_role in self.users_and_roles:
            algos_for_role = get_algos_btool(user_or_role, self.app, self.target_dir)
            algos.update(algos_for_role)

        return algos

    def app_name_from_conf_path(self, conf_path):
        """ Extract the app name from the conf_path

        Args:
            conf_path (str): full path to the algos.conf file
                            (e.g. /tmp/splunk/etc/apps/Splunk_ML_Toolkit/default/algos.conf)

        Returns:
            app_name (str): e.g. Splunk_ML_Toolkit

        """
        prefix = get_apps_path(bundle_path=self.target_dir)

        # Make sure we don't have a leading slash character
        conf_path_no_prefix = conf_path[len(prefix):].lstrip('/')

        # Take the app name which should be the first directory in the remaining path string
        return conf_path_no_prefix.split('/')[0]

    def get_mlspl_stanza_mapping(self):
        """ Get stanza mapping for MLSPL Conf settings.

        Returns:
            settings (dict): stanza mapping for stanzas to settings from mlspl.conf
        """
        settings = {}
        for user_or_role in self.users_and_roles:
            mlspl_settings = get_mlspl_btool(user_or_role, self.app, self.target_dir)
            settings.update(mlspl_settings)

        # We do not need the 'args' key
        return {k: v['args'] for k, v in settings.iteritems()}

    def get_scorings(self):
        """ Get scoring information for all users and roles

        Returns:
            results (dict): Return value of get_scorings_btool() for all users and roles.

        """
        scorings = {}

        for user_or_role in self.users_and_roles:
            scorings_for_role = get_scorings_btool(user_or_role, self.app, self.target_dir)
            scorings.update(scorings_for_role)

        return scorings
