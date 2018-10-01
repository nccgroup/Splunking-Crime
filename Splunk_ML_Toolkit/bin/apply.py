#!/usr/bin/env python
# Copyright (C) 2015-2018 Splunk Inc. All Rights Reserved.
from exec_anaconda import exec_anaconda_or_die
exec_anaconda_or_die()

import cexc
from cexc import BaseChunkHandler

from util.param_util import parse_args, parse_namespace_model_name
from util import command_util
from util.mlspl_loader import MLSPLConf
from util.search_util import add_distributed_search_info

from chunked_controller import ChunkedController

logger = cexc.get_logger('apply')
messages = cexc.get_messages_logger()


class ApplyCommand(BaseChunkHandler):
    """ApplyCommand uses the ChunkedController & ApplyProcessor to make
    predictions."""

    @staticmethod
    def handle_arguments(getinfo):
        """Take the getinfo metadata and return controller_options.

        Args:
            getinfo (dict): getinfo metadata

        Returns:
            controller_options (dict): options to be sent to controller
        """
        if len(getinfo['searchinfo']['args']) == 0:
            raise RuntimeError('First argument must be a saved model.')

        raw_options = parse_args(getinfo['searchinfo']['raw_args'][1:])
        controller_options = ApplyCommand.handle_raw_options(raw_options)
        controller_options['namespace'], controller_options['model_name'] = parse_namespace_model_name(getinfo['searchinfo']['args'][0])

        searchinfo = getinfo['searchinfo']
        getinfo['searchinfo'] = add_distributed_search_info(process_options=None, searchinfo=searchinfo)
        controller_options['mlspl_conf'] = MLSPLConf(getinfo['searchinfo'])
        return controller_options

    @staticmethod
    def handle_raw_options(raw_options):
        """Load command specific options.

        Args:
            raw_options (dict): raw options

        Raises:
            RuntimeError

        Returns:
            raw_options (dict): modified raw_options
        """
        raw_options['processor'] = 'ApplyProcessor'

        if 'args' in raw_options:
            raise RuntimeError('Apply does not accept positional arguments.')
        return raw_options

    def setup(self):
        """Parse search string, choose processor, initialize controller.

        Returns:
            (dict): get info response (command type) and required fields. This
                response will be sent back to the CEXC process on the getinfo
                exchange (first chunk) to establish our execution type and
                required fields.
        """
        controller_options = self.handle_arguments(self.getinfo)
        self.controller = ChunkedController(self.getinfo, controller_options)

        self.watchdog = command_util.get_watchdog(
            time_limit=-1,
            memory_limit=self.controller.resource_limits['max_memory_usage_mb']
        )

        streaming_apply = self.controller.resource_limits.get('streaming_apply', False)
        exec_type = 'streaming' if streaming_apply else 'stateful'

        required_fields = self.controller.get_required_fields()
        return {'type': exec_type, 'required_fields': required_fields}

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
        if command_util.is_invalid_chunk(metadata):
            logger.debug('Not running without session key.')
            return {'finished': True}

        if command_util.is_getinfo_chunk(metadata):
            return self.setup()

        finished_flag = metadata.get('finished', False)

        if not self.watchdog.started:
            self.watchdog.start()

        # Skip to next chunk if this chunk is empty
        if len(body) == 0:
            return {}

        # Load data, execute and collect results.
        self.controller.load_data(body)
        self.controller.execute()
        output_body = self.controller.output_results()

        if finished_flag:
            # Gracefully terminate watchdog
            if self.watchdog.started:
                self.watchdog.join()

        # Our final farewell
        return ({'finished': finished_flag}, output_body)


if __name__ == "__main__":
    logger.debug("Starting apply.py.")
    ApplyCommand(handler_data=BaseChunkHandler.DATA_RAW).run()
    logger.debug("Exiting gracefully. Byee!!")
