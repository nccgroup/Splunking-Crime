#!/usr/bin/env python
# Copyright (C) 2015-2018 Splunk Inc. All Rights Reserved.
from exec_anaconda import exec_anaconda_or_die
exec_anaconda_or_die()

import cexc
from cexc import BaseChunkHandler

from util import command_util
from util.command_util import GeneratingCommand

logger = cexc.get_logger('list')
messages = cexc.get_messages_logger()


class ListModelsCommand(GeneratingCommand):
    """ListModelsCommand uses the ChunkedController & ListModelsProcessor to
    list saved models."""

    @staticmethod
    def handle_arguments(getinfo):
        """Check for invalid arguments and get controller_options.

        Args:
            getinfo (dict): getinfo metadata

        Returns:
            controller_options (dict): controller options
        """
        if len(getinfo['searchinfo']['args']) > 0:
            raise RuntimeError('Invalid arguments')  # TODO: more descriptive error message

        controller_options = {}
        controller_options['processor'] = 'ListModelsProcessor'
        return controller_options

    def handler(self, metadata, body):
        """Default handler we override from BaseChunkHandler.

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

        # Don't run in preview.
        if self.getinfo.get('preview', False):
            logger.debug('Not running in preview')
            return {'finished': True}

        self.controller.execute()
        body = self.controller.output_results()

        # Final farewell
        return ({'finished': True}, body)


if __name__ == "__main__":
    logger.debug("Starting list.py.")
    ListModelsCommand(handler_data=BaseChunkHandler.DATA_RAW).run()
    logger.debug("Exiting gracefully. Byee!!")
