#!/usr/bin/env python
# Copyright (C) 2015-2018 Splunk Inc. All Rights Reserved.
from exec_anaconda import exec_anaconda_or_die
exec_anaconda_or_die()

import json
import sys

import cexc
from cexc import BaseChunkHandler

from util.rest_proxy import rest_proxy_from_searchinfo
from util import command_util
from util.searchinfo_util import searchinfo_from_cexc
from util.param_util import parse_args

logger = cexc.get_logger('logexperiment')
messages = cexc.get_messages_logger()
EXP_HIST_REST_URL_FORMAT = '/servicesNS/{}/Splunk_ML_Toolkit/mltk/experiments/{}/history'


class LogExperimentCommand(BaseChunkHandler):
    """LogExperimentCommand logs the results of an experiment."""

    def __init__(self, handler_data=None, in_file=sys.stdin, out_file=sys.stdout, err_file=sys.stderr):
        super(LogExperimentCommand, self).__init__(handler_data, in_file, out_file, err_file)
        self.exp_id = None
        self.app = None
        self.searchinfo = None

    @staticmethod
    def handle_arguments(getinfo):
        """Take the getinfo metadata and return controller_options.

        Args:
            getinfo (dict): getinfo metadata

        Returns:
            controller_options (dict): options to be sent to controller
        """
        options = parse_args(getinfo['searchinfo']['args'])

        if options.get('params') is None or options['params'].get('id') is None:
            raise RuntimeError('Experiment ID must be specified, e.g: logexperiment id=... ')

        return options

    def setup(self):
        """Parse search string, choose processor, initialize controller.

        Returns:
            (dict): get info response (command type) and required fields. This
                response will be sent back to the CEXC process on the getinfo
                exchange (first chunk) to establish our execution type and
                required fields.
        """
        options = self.handle_arguments(self.getinfo)
        self.exp_id = options['params']['id']

        # The 'app' argument value is needed to correctly locate the experiment
        # as it may be a different app than the current app context this is invoked from.
        # By default, it's the current app the command is executed from, but override it
        # if the user specified a value for the app arg.
        app = options['params'].get("app")
        if app is not None:
            self.searchinfo["app"] = app

        return {'type': "stateful"}

    def handler(self, metadata, body):
        """Main handler we override from BaseChunkHandler.

        Handles the reading and writing of data to the CEXC process, and
        finishes negotiation of the termination of the process.

        Args:
            metadata (dict): metadata information
            body (str): data payload from CEXC

        Returns:
            (dict): metadata to be sent back to CEXC
            output_body (str): data payload to be sent back to CEXC
        """
        if command_util.is_getinfo_chunk(metadata):
            self.searchinfo = searchinfo_from_cexc(metadata['searchinfo'], extra_fields=['sid'])
            return self.setup()

        finished_flag = metadata.get('finished', False)

        if finished_flag:
            rest_proxy = rest_proxy_from_searchinfo(self.searchinfo)
            reply = rest_proxy.make_rest_call('POST',
                                              EXP_HIST_REST_URL_FORMAT.format(self.searchinfo["username"], self.exp_id),
                                              jsonargs=json.dumps({'sid': self.searchinfo["sid"]}))
            if reply['success'] is False:
                msg = reply['content']
                logger.warn(msg)
                raise Exception(msg)
        else:
            msg = "Unable to access experiment with id={}. It does not exist or you do not have access to it".format(
                self.exp_id)
            logger.warn(msg)
            raise Exception(msg)

        # Our final farewell
        return {'finished': finished_flag}, body


if __name__ == "__main__":
    logger.debug("Starting logexperiment.py.")
    LogExperimentCommand(handler_data=BaseChunkHandler.DATA_RAW).run()
    logger.debug("Exiting gracefully. Byee!!")
