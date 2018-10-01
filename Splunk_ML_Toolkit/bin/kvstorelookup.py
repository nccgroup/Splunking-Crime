#!/usr/bin/env python
# Copyright (C) 2015-2018 Splunk Inc. All Rights Reserved.
from exec_anaconda import exec_anaconda_or_die
exec_anaconda_or_die()

import cexc
from cexc import BaseChunkHandler

from util.param_util import parse_args
from util.command_util import GeneratingCommand, is_getinfo_chunk

logger = cexc.get_logger('kvstorelookup')
messages = cexc.get_messages_logger()


class KVStoreLookupCommand(GeneratingCommand):
    """KVStoreLookupCommand uses the ChunkedController & KVStoreLookupProcessor to read a KVStore collection"""

    @staticmethod
    def handle_arguments(getinfo):
        """Check for invalid arguments and get controller_options.

        Args:
            getinfo (dict): getinfo metadata

        Returns:
            controller_options (dict): controller options
        """

        options = parse_args(getinfo['searchinfo']['args'])
        params = options.get('params', {})

        collection_name = params.get('collection_name')
        experiment_id = params.get('experiment_id')

        if collection_name is None and experiment_id is None:
            raise RuntimeError('You must provide a KVStore collection name (collection_name=...) or an Experiment id (experiment_id=...)')

        controller_options = parse_args(getinfo['searchinfo']['raw_args'][1:])

        controller_options['processor'] = 'KVStoreLookupProcessor'
        controller_options['collection_name'] = collection_name
        controller_options['experiment_id'] = experiment_id

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
        if is_getinfo_chunk(metadata):
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
    logger.debug("Starting kvstorelookup.py.")
    KVStoreLookupCommand(handler_data=BaseChunkHandler.DATA_RAW).run()
    logger.debug("Exiting gracefully. Byee!!")
