#!/usr/bin/env python
# Copyright (C) 2015-2018 Splunk Inc. All Rights Reserved.
from exec_anaconda import exec_anaconda_or_die
exec_anaconda_or_die()

import cexc
from cexc import BaseChunkHandler

from util import param_util, command_util
from util.command_util import GeneratingCommand

logger = cexc.get_logger('summary')
messages = cexc.get_messages_logger()


class SummaryCommand(GeneratingCommand):
    """Summary command gets model summaries from ML-SPL models."""

    @staticmethod
    def handle_arguments(getinfo):
        """Catch invalid argument and return controller options.

        Args:
            getinfo (dict): getinfo metadata

        Return:
            controller_options (dict): controller options
        """
        if len(getinfo['searchinfo']['args']) == 0:
            raise RuntimeError('First argument must be a saved model')

        controller_options = param_util.parse_args(getinfo['searchinfo']['raw_args'][1:])
        controller_options['namespace'], controller_options['model_name'] = \
            param_util.parse_namespace_model_name(getinfo['searchinfo']['args'][0])
        controller_options['processor'] = 'SummaryProcessor'
        return controller_options

    def handler(self, metadata, body):
        """Main handler we override from BaseChunkHandler.

        Args:
            metadata (dict): metadata information
            body (str): data payload from CEXC

        Returns:
            (dict): metadata to be sent back to CEXC
            body (str): data payload to be sent back to CEXC
        """
        if command_util.is_invalid_chunk(metadata):
            logger.debug('Not running without session key.')
            return {'finished': True}

        if command_util.is_getinfo_chunk(metadata):
            return self.setup()

        self.controller.execute()
        body = self.controller.output_results()

        return ({'finished': True}, body)


if __name__ == "__main__":
    logger.debug("Starting summary.py.")
    SummaryCommand(handler_data=BaseChunkHandler.DATA_RAW).run()
    logger.debug("Exiting gracefully. Byee!!")
