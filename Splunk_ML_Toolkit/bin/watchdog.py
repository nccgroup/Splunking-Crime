#!/usr/bin/env python

import os
import sys
import time
from multiprocessing import Process
import psutil

import cexc

MONITORING_INTERVAL = 1.0


class Watchdog(object):
    def __init__(self, time_limit=60, memory_limit=100 * 1024 * 1024,
                 finalize_file=None, pid=None):
        if pid is None:
            self.victim = psutil.Process(pid=os.getpid())
        else:
            self.victim = psutil.Process(pid=pid)

        self.time_limit = time_limit
        self.memory_limit = memory_limit
        self.finalize_file = finalize_file
        self.started = False
        self._process = None
        self.start_time = None

    def start(self):
        self._process = Process(target=self.main, name="ML-SPL Watchdog")
        self._process.daemon = True
        self._process.start()
        self.started = True

    def __del__(self):
        if hasattr(self, '_process') and isinstance(self._process, Process):
            self.join()

    def join(self):
        self._process.terminate()
        self._process.join(1)

    def main(self):
        logger = cexc.setup_logging.get_logger('mlspl_watchdog')

        self.start_time = time.time()

        while True:
            # Check to see if parent is still running...
            if not self.victim.is_running():
                logger.info('Watchdog exiting because parent %s disappeared.',
                            self.victim)
                return 0

            delta = time.time() - self.start_time
            # Check time_limit
            if self.time_limit >= 0 and delta > self.time_limit:
                logger.info('Terminate %s: exceeded time limit (%d > %d)',
                            self.victim, delta, self.time_limit)
                # Note: this chunk output may race with our parent...
                cexc.BaseChunkHandler._internal_write_chunk( # pylint: disable=W0212
                    sys.stdout,
                    {'error': 'Time limit exceeded (> %d seconds)' % self.time_limit}
                )
                self.victim.terminate()
                return 1

            # Check memory limit
            rss = self.victim.memory_info().rss
            if self.memory_limit >= 0 and rss > self.memory_limit:
                logger.info('Terminating %s: exceeded memory limit (%d > %d)',
                            self.victim, rss, self.memory_limit)
                # Note: this chunk output may race with our parent...
                cexc.BaseChunkHandler._internal_write_chunk( # pylint: disable=W0212
                    sys.stdout,
                    {'error': 'Memory limit exceeded (> %d bytes)' % self.memory_limit}
                )
                self.victim.terminate()
                return 2

            # Check if finalize file exists
            if self.finalize_file and os.path.exists(self.finalize_file):
                logger.info('Terminating %s: finalize file detected', self.victim)
                # Note: this chunk output may race with our parent...
                cexc.BaseChunkHandler._internal_write_chunk( # pylint: disable=W0212
                    sys.stdout,
                    {'error': 'Aborting because job finalization was requested.'}
                )
                self.victim.terminate()
                return 3

            logger.debug('Monitoring %s: Running for %.1f secs, rss %d',
                         self.victim, delta, rss)
            time.sleep(MONITORING_INTERVAL)

        return 0
