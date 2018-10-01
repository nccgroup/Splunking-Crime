#!/usr/bin/env python
# Simple script to bounce the lookup-update-notify REST endpoint.
# Needed to trigger model replication in SHC environments.
#
# This needs to launch a subprocess for the simple reason that the
# interpreter that ML-SPL runs under (Splunk_SA_Scientific_Python)
# doesn't have OpenSSL. We marshall the required Splunk auth token
# through stdin to avoid leaks via environment variables/command line
# arguments/etc.
import json
import sys


def make_rest_call(session_key, method, url, postargs=None, jsonargs=None, getargs=None, rawResult=False):
    import os
    import subprocess
    import cexc

    logger = cexc.get_logger(__name__)

    payload = {
        'session_key': session_key,
        'url': url,
        'method': method,
        'postargs': postargs,
        'jsonargs': jsonargs,
        'getargs': getargs,
        'rawResult': rawResult,
    }

    try:
        python_path = os.path.join(os.environ['SPLUNK_HOME'], 'bin', 'python')
        p = subprocess.Popen([python_path, os.path.abspath(__file__)],
                             stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        (stdoutdata, stderrdata) = p.communicate(json.dumps(payload))
        p.wait()

        for errline in stderrdata.splitlines():
            logger.debug('> %s', errline)

        if p.returncode != 0:
            raise RuntimeError("rest_bouncer subprocess exited with non-zero error code '%d'" % p.returncode)

        reply = json.loads(stdoutdata)
    except Exception as e:
        logger.warn('rest_bouncer failure: %s: %s', type(e).__name__, str(e))
        return False

    return reply


if __name__ == "__main__":
    from splunk import rest, RESTException
    import httplib

    reply = {
        'success': False,
        'response': None,
        'content': None,
        'error_type': None,
        'status': None,
    }

    # Read JSON payload from stdin
    try:
        line = sys.stdin.next()
        payload = json.loads(line)

        session_key = payload['session_key']
        method = payload['method']
        url = payload['url']
        postargs = payload['postargs']
        jsonargs = payload['jsonargs']
        getargs = payload['getargs']
        rawResult = payload['rawResult']

        response, content = rest.simpleRequest(
            url,
            method=method,
            postargs=postargs,
            sessionKey=session_key,
            raiseAllErrors=False,
            jsonargs=jsonargs,
            getargs=getargs,
            rawResult=rawResult,
        )

        reply['response'] = response
        reply['content'] = content
        status = response.status
        reply['status'] = status

        if status > 199 and status < 300:
            reply['success'] = True
    except RESTException as e:
        reply['error_type'] = type(e).__name__
        reply['content'] = '{"messages":[{"type": "ERROR", "text": "%s"}]}' % str(e)
        reply['status'] = e.statusCode
    except Exception as e:
        error_type = type(e).__name__
        reply['content'] = '{"messages":[{"type": "%s", "text": "%s"}]}' % (error_type, str(e))
        reply['error_type'] = error_type
        reply['status'] = httplib.INTERNAL_SERVER_ERROR
    print json.dumps(reply)
