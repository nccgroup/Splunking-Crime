#!/usr/bin/env python
# Copyright (C) 2015-2018 Splunk Inc. All Rights Reserved.
from exec_anaconda import exec_anaconda_or_die
exec_anaconda_or_die()

import os

import cexc
from cexc import BaseChunkHandler
from util import command_util
from util.param_util import parse_args
from chunked_controller import ChunkedController

logger = cexc.get_logger('score')
messages = cexc.get_messages_logger()


class ScoreCommand(cexc.BaseChunkHandler):
    """ScoreCommand uses ChunkedController & processor(s) to score field(s). """

    @staticmethod
    def handle_arguments(getinfo):
        """Take the getinfo metadata and return controller_options.

        Args:
            getinfo (dict): getinfo metadata from first chunk

        Returns:
            controller_options (dict): options to be passed to controller
        """
        if len(getinfo['searchinfo']['raw_args']) == 0:
            raise RuntimeError('First argument must be a scoring method')

        raw_options = parse_args(getinfo['searchinfo']['raw_args'][1:])
        controller_options = ScoreCommand.handle_raw_options(raw_options)
        controller_options['scoring_name'] = getinfo['searchinfo']['args'][0]
        return controller_options

    @staticmethod
    def handle_raw_options(controller_options):
        """Load command specific options.

        Args:
            controller_options (dict): options from handle_arguments
        Returns:
            controller_options (dict): dict of controller options
        """
        controller_options['processor'] = 'ScoreProcessor'
        controller_options['variables'] = controller_options.pop('feature_variables', [])
        return controller_options

    def setup(self):
        """Parse search string, choose processor, initialize controller & watchdog.

        Returns:
            (dict): get info response (command type) and required fields. This
                response will be sent back to the CEXC process on the getinfo
                exchange (first chunk) to establish our execution type and
                required fields.
        """
        controller_options = self.handle_arguments(self.getinfo)
        self.controller = ChunkedController(self.getinfo, controller_options)

        self.watchdog = command_util.get_watchdog(
            self.controller.resource_limits['max_score_time'],
            self.controller.resource_limits['max_memory_usage_mb'],
            os.path.join(self.getinfo['searchinfo']['dispatch_dir'], 'finalize'))

        required_fields = self.controller.get_required_fields()
        return {'type': 'events', 'required_fields': required_fields}

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

        # Load data
        self.controller.load_data(body)

        # score will execute on the last chunk.
        if finished_flag:
            self.controller.execute()
            output_body = self.controller.output_results()
        else:
            output_body = None

        if finished_flag:
            if self.watchdog.started:
                self.watchdog.join()

        # Our final farewell
        return ({'finished': finished_flag}, output_body)


if __name__ == "__main__":
    logger.debug("Starting score.py.")
    ScoreCommand(handler_data=BaseChunkHandler.DATA_RAW).run()
    logger.debug("Exiting gracefully. Byee!!")
