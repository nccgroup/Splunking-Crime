#!/usr/bin/env python
# Copyright (C) 2015-2018 Splunk Inc. All Rights Reserved.
from exec_anaconda import exec_anaconda_or_die
exec_anaconda_or_die()

import cexc

from util import command_util, param_util
from util.command_util import GeneratingCommand

logger = cexc.get_logger('delete')
messages = cexc.get_messages_logger()


class DeleteModelCommand(GeneratingCommand):
    """DeleteModelCommand uses the ChunkedController & DeleteModelProcessor to
    delete models."""

    @staticmethod
    def handle_arguments(getinfo):
        """Check for invalid argument usage and return controller options.

        Args:
            getinfo(dict): getinfo metadata

        Returns:
            controller_options (dict): controller options
        """
        if len(getinfo['searchinfo']['args']) != 1:
            raise RuntimeError('Usage: deletemodel <modelname>')

        controller_options = {}
        controller_options['namespace'], controller_options['model_name'] = param_util.parse_namespace_model_name(getinfo['searchinfo']['args'][0])
        controller_options['processor'] = 'DeleteModelProcessor'
        return controller_options

    def handler(self, metadata, body):
        """Default handler we override from BaseChunkHandler.

        Args:
            metadata (dict): metadata information
            body (str): data payload from CEXC

        Returns:
            (dict): metadata to be sent back to Splunk
        """
        if command_util.is_invalid_chunk(metadata):
            logger.debug('Not running without session key.')
            return {'finished': True}

        if command_util.is_getinfo_chunk(metadata):
            return self.setup()

        self.controller.execute()
        return {'finished': True}


if __name__ == "__main__":
    logger.debug("Starting delete.py.")
    DeleteModelCommand().run()
    logger.debug("Exiting gracefully. Byee!!")
