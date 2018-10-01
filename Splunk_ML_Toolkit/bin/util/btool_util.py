import os
import re
import subprocess

import cexc

logger = cexc.get_logger(__name__)

STANZA_REGEX = '^(?P<conf_path>.*\.conf)\s*\[(?P<stanza>[a-zA-Z_][a-zA-Z0-9_]*(:[a-zA-Z_][a-zA-Z0-9_]*)?)\]'
ATTRIBUTE_REGEX = '^(?P<conf_path>.*\.conf)\s*(?P<attribute>[a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*(?P<value>[^=]*)'
MODEL_REGEX = '^(?P<file_path>.*__mlspl_[a-zA-Z_][a-zA-Z0-9_]*\.csv)\s*\[__mlspl_(?P<model_name>[a-zA-Z_][a-zA-Z0-9_]*)\.csv\]'
EXP_REGEX = '^(?P<file_path>.*__mlsplexp_[a-zA-Z_][a-zA-Z0-9_]*\.csv)\s*\[__mlsplexp_(?P<exp_name>[a-zA-Z_][a-zA-Z0-9_]*)\.csv\]'


def btool(conf_file, user, app, target_dir=None):
    """
    Use subprocess to run the btool command of splunk, get the raw returns

    Args:
        conf_file (string): confFile for the btool command, 'lookups' or 'algos'
        user (string): username or role of the splunk user
        app (string): splunk app name
        target_dir (string): target dir for btool to search

    Returns:
        btool_results (string): raw output from btool command
    """

    if conf_file not in ['lookups', 'algos', 'mlspl']:
        logger.debug("Unrecognized conf file in btool call: expect either 'lookups' or 'algos' or 'mlspl'")
        raise RuntimeError("Please check mlspl.log for more details.")

    SPLUNK_HOME = os.environ['SPLUNK_HOME']
    SPLUNK_EXEC = os.path.join(SPLUNK_HOME, 'bin', 'splunk')

    try:
        btool_command = [SPLUNK_EXEC, 'cmd', 'btool', '--debug',
                         '--user=%s' % user, '--app=%s' % app]
        if target_dir:
            btool_command.append('--dir=%s' % target_dir)
        btool_command += [conf_file, 'list']
        btool_results = subprocess.check_output(btool_command, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        logger.debug("btool subprocess exited with non-zero error code '%d'" % e.returncode)
        logger.debug('> %s', e.output)
        raise RuntimeError("Please check mlspl.log for more details.")

    return btool_results


def get_lookups_btool(user, app, lookup_type, target_dir=None):
    """
    Use subprocess to run the btool lookups command,
    parse the results and extract mlspl models

    Args:
        user (string): username or role of the splunk user
        app (string): splunk app name
        target_dir (string): target dir for btool to search
        lookup_type (string): 'model' or 'experiment'
    Returns:
        results (dict):
            {
                <LOOKUP_NAME>: <ABSOLUTE_FILE_PATH_OF_THE_LOOKUP>,
                ...
            }
    """

    btool_results = btool(conf_file='lookups', user=user, app=app, target_dir=target_dir)
    return parse_btool_lookups(btool_results, lookup_type=lookup_type)


def get_algos_btool(user, app, target_dir=None):
    """
    Use subprocess to run the btool algos command,
    parse the results

    Args:
        user (string): username or role of the splunk user
        app (string): splunk app name
        target_dir (string): target dir for btool to search

    Returns:
        results (dict):
            {
                <ALGO_NAME>: {
                                'args': {
                                            <KEY_IN_STANZA>: <VALUE_IN_STANZA>,
                                            ...
                                        },
                                'conf_path': <ABSOLUTE_PATH_OF_THE_CONF_FILE>
                             },
                ...
            }
    """

    conf_file = 'algos'
    btool_results = btool(conf_file, user, app, target_dir)
    return parse_btool_stanzas(btool_results, conf_file, allow_duplicate_stanzas=False)


def get_mlspl_btool(user, app, target_dir=None):
    conf_file = 'mlspl'
    btool_results = btool(conf_file, user, app, target_dir)
    return parse_btool_stanzas(btool_results, conf_file)


def get_scorings_btool(user, app, target_dir=None):
    conf_file = 'scorings'
    btool_results = btool(conf_file, user, app, target_dir)
    return parse_btool_stanzas(btool_results, conf_file, allow_duplicate_stanzas=False)


def parse_btool_stanzas(btool_results, conf_name, allow_duplicate_stanzas=True):
    """
    Parse the stanzas and attributes into a dictionary from btool's output.

    Args:
        btool_results (string): raw output from btool <conf> list
        conf_name: the name of the conf for use in debug messages

    Returns:
        results (dict):
            {
                <STANZA>: {
                                'args': {
                                            <KEY_IN_STANZA>: <VALUE_IN_STANZA>,
                                            ...
                                        },
                                'conf_path': <ABSOLUTE_PATH_OF_THE_CONF_FILE>
                             },
                ...
            }
    """
    results = {}
    current_stanza = None

    stanza_re = re.compile(STANZA_REGEX)
    attr_re = re.compile(ATTRIBUTE_REGEX)

    for line in btool_results.splitlines():

        stanza_match = stanza_re.match(line)
        attr_match = attr_re.match(line)

        if stanza_match:
            current_stanza = stanza_match.group('stanza')
            conf_path = stanza_match.group('conf_path')

            if current_stanza not in results:
                results[current_stanza] = {'conf_path': None, 'args': {}}

            results[current_stanza]['conf_path'] = conf_path

        if attr_match:
            if current_stanza is None or current_stanza not in results:
                msg = "Failed parsing btool output: key value pairs specified before stanza name"
                logger.debug(msg)
                logger.debug('btool output: %s' % btool_results)
                raise RuntimeError("Please check mlspl.log for more details.")

            groups = ['conf_path', 'attribute', 'value']
            conf_path, attr_key, attr_value = [attr_match.group(g) for g in groups]

            if not allow_duplicate_stanzas:
                if results[current_stanza]['conf_path'] != conf_path:
                    msg = "Failed parsing btool output: stanza name and key value pairs are from different conf files"
                    logger.debug(msg)
                    logger.debug('btool output: %s' % btool_results)
                    cexc.messages.warn('There are duplicate {} stanzas defined in multiple conf files: '
                                       'please check {}.conf'.format(current_stanza, conf_name))
                    raise RuntimeError("Please check mlspl.log for more details.")

            results[current_stanza]['args'][attr_key] = attr_value

    return results


def parse_btool_lookups(btool_results, lookup_type):
    """
    Parse the results from btool lookups list

    Args:
        btool_results (string): raw output from btool lookups list
        lookup_type (string): 'model' or 'experiment'
    Returns:
        results (dict):
            {
                <MODEL_NAME>: <ABSOLUTE_FILE_PATH_OF_THE_MODEL>,
                ...
            }
    """
    if lookup_type == 'model':
        regex, group_name = MODEL_REGEX, 'model_name'
    elif lookup_type == 'experiment':
        regex, group_name = EXP_REGEX, 'exp_name'
    else:
        raise Exception("lookup_type can be either `model` or `experiment`")

    results = {}
    lookups_re = re.compile(regex)
    for lookup in btool_results.splitlines():
        match = lookups_re.match(lookup)
        if match:
            lookup_name = match.group(group_name)
            file_path = match.group('file_path')
            results[lookup_name] = file_path
    return results
