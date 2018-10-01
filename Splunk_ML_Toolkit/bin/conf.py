#!/usr/bin/env python

import os

APP_NAME = (
    os.path.basename(
        os.path.dirname(
            os.path.dirname(
                os.path.abspath(__file__)))))
CONF_NAME = 'mlspl'

from splunklite import conf
from util.base_util import get_apps_path


def get_app_path():
    return os.path.join(get_apps_path(), APP_NAME)


def get_mlspl_prop(name, stanza='default', default=None):
    return get_mlspl_conf(stanza).get(name, default)


def get_mlspl_conf(stanza=None, merge_default=True):
    app_path = get_app_path()
    props = conf.getAppConf(CONF_NAME, app_path=app_path)

    if stanza == None:
        return props
    elif stanza == 'default':
        return props.get('default', {})
    elif merge_default:
        stanza_props = props.get('default', {})
        conf._multi_update(stanza_props, props.get(stanza, {}))
        return stanza_props

    return props.get(stanza, {})
