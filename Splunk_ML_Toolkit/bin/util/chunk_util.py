#!/usr/bin/env python

import sys
import json
from cStringIO import StringIO
import re
import csv
import traceback
from util.file_util import  fix_line_ending

fix_line_ending()
getinfo = {}


def read_chunk(f):
    """Attempts to read a single "chunk" from the given file.

    On error (e.g. exception during read, parsing failure), returns None

    Otherwise, returns [metadata, body], where
        metadata is a dict with the parsed contents of the chunk JSON metadata
        body is a string with the body contents
    """

    try:
        header = f.readline()
    except:
        return None

    if not header or len(header) == 0:
        return None

    m = re.match('chunked\s+1.0\s*,\s*(?P<metadata_length>\d+)\s*,\s*(?P<body_length>\d+)\s*\n', header)
    if m is None:
        print >> sys.stderr, 'Failed to parse transport header: %s' % header
        return None

    try:
        metadata_length = int(m.group('metadata_length'))
        body_length = int(m.group('body_length'))
    except:
        print >> sys.stderr, 'Failed to parse metadata or body length'
        return None

    try:
        metadata_buf = f.read(metadata_length)
        body = f.read(body_length)
    except Exception as e:
        print >> sys.stderr, 'Failed to read metadata or body: %s' % str(e)
        return None

    try:
        metadata = json.loads(metadata_buf)
    except:
        print >> sys.stderr, 'Failed to parse metadata JSON'
        return None

    return [metadata, body]


def write_chunk(f, metadata, body):
    """Attempts to write a single "chunk" to the given file.

    metadata should be a Python dict with the contents of the metadata
    payload. It will be encoded as JSON.

    body should be a string of the body payload.

    no return, may throw an IOException
    """
    metadata_buf = None
    if metadata:
        metadata_buf = json.dumps(metadata)
    f.write('chunked 1.0,%d,%d\n' % (len(metadata_buf) if metadata_buf else 0, len(body)))
    if metadata:
        f.write(metadata_buf)
    f.write(body)
    f.flush()


def add_message(metadata, level, msg):
    ins = metadata.setdefault('inspector', {})
    msgs = ins.setdefault('messages', [])
    k = '[' + str(len(msgs)) + '] '
    msgs.append([level, k + msg])


def die(metadata=None, msg="Error in external search commmand", print_stacktrace=True):
    if print_stacktrace:
        traceback.print_exc(sys.stderr)

    if metadata is None:
        metadata = {}

    metadata['finished'] = True
    metadata['error'] = msg
    sio = StringIO()
    writer = csv.writer(sio)
    writer.writerow(['ERROR'])
    writer.writerow([msg])
    write_chunk(sys.stdout, metadata, sio.getvalue())
    sys.exit(1)


def log_and_warn(metadata, msg, search_msg=None):
    search_msg = search_msg or msg
    sys.stderr.write('WARNING: ' + msg)
    add_message(metadata, 'WARN', search_msg)


def log_and_die(metadata, msg, search_msg=None):
    search_msg = search_msg or msg
    sys.stderr.write('ERROR: ' + msg)
    die(metadata, msg)
