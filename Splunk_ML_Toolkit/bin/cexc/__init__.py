#!/usr/bin/env python
"""Utility library for "chunked" custom search commands."""

import sys
import json
from cStringIO import StringIO
import re
import csv
import traceback
import logging
from collections import OrderedDict
import time

import setup_logging

logger = setup_logging.get_logger()
messages = logger.getChild('messages')


def get_logger(name=None):
    """Returns a logger for internal messages."""
    if name:
        return logger.getChild(name)
    else:
        return logger


def get_messages_logger():
    """Returns a logger for user-visible messages."""
    return messages


def log_traceback():
    """Logs a traceback. Useful in Exception handlers."""
    logger.error(traceback.format_exc())


def abort(msg):
    """Helper method to abort gracefully with a user-visible message.

    Do NOT use this method from within a running
    BaseChunkHandler. Instead, raise an Exception or RuntimeError.

    Invoke this function to gracefully exit a custom search command
    before a BaseChunkHandler object has been created and run. You may
    use this, for instance, if there is an exception during an import
    in your __main__ module.
    """
    AbortHandler.abort(msg)

class BaseChunkHandler(object):
    """Base class for custom search commands using the "chunked" protocol.

    This is a low-level implementation. You are strongly encouraged to
    use the Splunk Python SDK instead.

    To write an external search command, extend this class, override
    the handler() method, and invoke its run() method, e.g.:

        class Handler(BaseChunkHandler):
            def handler(self, metadata, data):
                ...
        if __name__ == "__main__":
            Handler().run()

    run() will read a chunk from stdin, call handler() with the
    metadata and data payloads, and write a chunk containing
    handler()'s return value. It will continue doing this in a loop
    until EOF is read.

    Parameters
    ----------
    handler_data : DATA_DICT | DATA_CSVROW | DATA_RAW
        Specifies how/whether data payload should be parsed.
        Defaults to DATA_DICT.

    in_file, out_file, err_file : file
        Files to use for input, output, and errors, respectively.
        Defaults to sys.stdin, sys.stdout, sys.stderr.

    Attributes
    ----------
    getinfo : dict, class attribute
        Metadata from the getinfo exchange. Set when
        action:getinfo is observed in _read_chunk().

    """

    (DATA_DICT,  # parse data payload with csv.DictReader
     DATA_CSVROW,  # parse data payload with csv.reader
     DATA_RAW  # don't parse data payload
     ) = range(3)

    def __init__(self,
                 handler_data=None,
                 in_file=sys.stdin, out_file=sys.stdout,
                 err_file=sys.stderr):
        if handler_data:
            self.handler_data = handler_data
        else:
            self.handler_data = self.DATA_DICT
        self.in_file = in_file
        self.out_file = out_file
        self.err_file = err_file
        self.getinfo = {}

        # Unmangle line-endings in Windows.

        # N.B. : Windows converts \n to \r such that transport headers do not
        # get received correctly by the CEXC protocol. However, this is really
        # only needed when the IO is actually an object with a file descriptor.
        # Python 2 docs note that file-like objects that don't have real file
        # descriptors should *not* implement a fileno method:

        if sys.platform == "win32":
            import os, msvcrt # pylint: disable=import-error
            for file_like_object in [self.in_file, self.out_file, self.err_file]:
                fileno = getattr(file_like_object, 'fileno', None)
                if fileno is not None:
                    if callable(fileno):
                        try:
                            msvcrt.setmode(file_like_object.fileno(), os.O_BINARY) # pylint: disable=E1103 ; the Windows version of os has O_BINARY
                        except ValueError:
                            # This can be safely skipped, as it is raised
                            # from pytest which incorreclty implements a fileno
                            pass

        # Logger instance for user-visible messages.
        self.messages_logger = get_messages_logger()
        self.messages_handler = logging.handlers.BufferingHandler(100000)
        self.messages_logger.addHandler(self.messages_handler)

        # Variables to track time spent in different chunk handling
        # states.
        self._read_time = 0.0
        self._handle_time = 0.0
        self._write_time = 0.0

        self.controller_options = None
        self.controller = None
        self.watchdog = None
        self.partial_fit = None

    def run(self):
        """Handle chunks in a loop until EOF is read.

        If an exception is raised during chunk handling, a chunk
        indicating the error will be written and the process will exit.
        """
        try:
            while self._handle_chunk():
                pass
        except Exception as e:
            if isinstance(e, RuntimeError):
                error_message = str(e)
            else:
                error_message = '(%s) %s' % (
                    type(e).__name__, e
                )
            self.die(error_message)
            # sadly pylint does not get the joke
            # laskdjflakj  # ;-)

    def handler(self, metadata, body):
        """Default chunk handler, returns empty metadata and data payloads."""
        return ({}, [])

    def die(self, message, log_traceback=True):
        """Logs a message, writes a user-visible error, and exits."""

        logger.error(message)
        if log_traceback:
            logger.error(traceback.format_exc())

        metadata = {'finished': True, 'error': message}

        # Insert inspector messages from messages_logger.
        messages = self._pop_messages()
        # Convert non-DEBUG messages to ERROR so the user can see them...
        messages = [['ERROR', y] for x, y in messages if x != 'DEBUG']

        if len(messages) > 0:
            metadata.setdefault('inspector', {}).setdefault('messages', []).extend(messages)

        # Sort the keys in reverse order! 'inspector' must come before 'error'.
        metadata = OrderedDict([(k, metadata[k]) for k in sorted(metadata, reverse=True)])

        self._write_chunk(metadata, '')
        sys.exit(1)

    _header_re = re.compile(r'chunked\s+1.0,(?P<metadata_length>\d+),(?P<body_length>\d+)')

    def _read_chunk(self):
        """Attempts to read a single "chunk" from self.in_file.

        Returns
        -------
        None, if EOF during read.
        (metadata, data) : dict, str
            metadata is the parsed contents of the chunk JSON metadata
            payload, and data is contents of the chunk data payload.

        Raises on any exception.
        """
        header = self.in_file.readline()

        if len(header) == 0:
            return None

        m = self._header_re.match(header)
        if m is None:
            raise ValueError('Failed to parse transport header: %s' % header)

        metadata_length = int(m.group('metadata_length'))
        body_length = int(m.group('body_length'))

        metadata_buf = self.in_file.read(metadata_length)
        body = self.in_file.read(body_length)

        metadata = json.loads(metadata_buf)

        return (metadata, body)

    def _write_chunk(self, metadata=None, body=''):
        """Attempts to write a single "chunk" to self.out_file.

        Parameters
        ----------
        metadata : dict or None, metadata payload.
        body : str, body payload

        Returns None. Raises on exception.
        """
        self._internal_write_chunk(self.out_file, metadata, body)

    @staticmethod
    def _internal_write_chunk(out_file, metadata=None, body=''):
        metadata_buf = None
        if metadata:
            metadata_buf = json.dumps(metadata)

        metadata_length = len(metadata_buf) if metadata_buf else 0
        body_length = len(body)

        out_file.write('chunked 1.0,%d,%d\n' % (metadata_length, body_length))

        if metadata:
            out_file.write(metadata_buf)

        out_file.write(body)
        out_file.flush()

    def _handle_chunk(self):
        """Handle (read, process, write) a chunk."""
        with Timer() as t:
            ret = self._read_chunk()
            if not ret:
                return False  # EOF

            metadata, body = ret

            if self.handler_data == self.DATA_DICT:
                body = list(csv.DictReader(StringIO(body)))
            elif self.handler_data == self.DATA_CSVROW:
                body = list(csv.reader(StringIO(body)))
            elif self.handler_data == self.DATA_RAW:
                pass

            # Cache a copy of the getinfo metadata.
            if metadata.get('action', None) == 'getinfo':
                self.getinfo = dict(metadata)

        self._read_time += t.interval

        with Timer() as t:
            # Invoke handler. Hopefully someone overloaded it!
            ret = self.handler(metadata, body)

            if isinstance(ret, dict):
                metadata, body = ret, None
            else:
                try:
                    metadata, body = ret
                except:
                    raise TypeError("Handler must return (metadata, body), got: %.128s" % repr(ret))

            # Insert inspector messages from messages_logger.
            messages = self._pop_messages()
            if len(messages) > 0:
                metadata.setdefault('inspector', {}).setdefault('messages', []).extend(messages)

        self._handle_time += t.interval

        with Timer() as t:
            if body is not None and len(body) > 0:
                sio = StringIO()

                if self.handler_data == self.DATA_DICT:
                    assert hasattr(body, '__iter__')

                    keys = set()
                    for r in body:
                        keys.update(r.keys())

                    writer = csv.DictWriter(sio, fieldnames=list(keys))
                    writer.writeheader()

                    for r in body:
                        writer.writerow(r)
                    body = sio.getvalue()

                elif self.handler_data == self.DATA_CSVROW:
                    writer = csv.writer(sio)
                    for r in body:
                        writer.writerow(r)
                    body = sio.getvalue()
                elif self.handler_data == self.DATA_RAW:
                    pass

                assert isinstance(body, basestring)

            else:
                body = ''

            self._write_chunk(metadata, body)

        self._write_time += t.interval

        return True

    def _pop_messages(self):
        """Drain logging.MemoryHandler holding user-visible messages."""
        messages = []
        for r in self.messages_handler.buffer:
            # Map message levels to Splunk equivalents.
            level = {'DEBUG': 'DEBUG', 'INFO': 'INFO', 'WARNING': 'WARN',
                     'ERROR': 'ERROR', 'CRITICAL': 'ERROR'}[r.levelname]
            messages.append([level, r.message])

        self.messages_handler.flush()
        return messages


class AbortHandler(BaseChunkHandler):
    def __init__(self, msg):
        self.msg = msg
        super(AbortHandler, self).__init__()

    def handler(self, metadata, body):
        raise RuntimeError(self.msg)

    @classmethod
    def abort(cls, msg):
        cls(msg).run()
        sys.exit(1)


class Timer:
    def __enter__(self):
        self.start = time.clock()
        return self

    def __exit__(self, *args):
        self.end = time.clock()
        self.interval = self.end - self.start
