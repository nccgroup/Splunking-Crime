"""
A generic REST handler that allows the MLTK to provide REST endpoints
For each REST handler, expects a snake-case file such as my_rest_handler in /rest_handlers
and a matching camel-cased class inside the file such as MyRestHandler
Each class should have a handle_* method for each method the handler supports
For example a to handle GET requests, there should be a method called handle_get
Each handler method will be passed information about the request and a list of url path fragments
REST handlers are located at "servicesNS/admin/Splunk_ML_Toolkit/mltk/my_rest_handler"
"""

import importlib
import json
import os
import sys
from splunk.persistconn.application import PersistentServerConnectionApplication

# import the parent directory of the current file's parent
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))

import cexc
from util.base_util import is_valid_identifier

logger = cexc.get_logger(__name__)
messages = cexc.get_messages_logger()

if sys.platform == "win32":
    import msvcrt
    # Binary mode is required for persistent mode on Windows.
    msvcrt.setmode(sys.stdin.fileno(), os.O_BINARY)
    msvcrt.setmode(sys.stdout.fileno(), os.O_BINARY)
    msvcrt.setmode(sys.stderr.fileno(), os.O_BINARY)


class RestHandler(PersistentServerConnectionApplication):
    def __init__(self, command_line, command_arg):
        PersistentServerConnectionApplication.__init__(self)

    def handle(self, in_string):
        """
        The base handler method, inheriting from PersistentServerConnectionApplication

        Args:
            in_string (str): a JSON string containing information about the request

        Returns:
            The return value, as decided by the handler method that this method delegates to
        """
        request = json.loads(in_string)

        method = request['method']
        path_parts = self.get_path_info_parts(request)

        try:
            handler_name = path_parts[0]
            if not is_valid_identifier(handler_name):
                raise Exception('REST handlers can only start with letters and underscores and can contain only letters, numbers, and underscores')

            try:
                handler_package = importlib.import_module("rest_handlers.%s" % handler_name)
                handler_class = getattr(handler_package, handler_name.title().replace('_', ''))
                
            except (AttributeError, ImportError) as e:
                logger.exception(e)
                return {
                    'payload': 'Unknown REST endpoint: %s' % handler_name,
                    'status': 404
                }
            try:
                handler_method = getattr(handler_class, "handle_%s" % method.lower())
                reply = handler_method(request, path_parts)
            except AttributeError as e:
                logger.debug(e)
                return {
                    'payload': 'Unsupported method: %s' % method,
                    'status': 405
                }
        except Exception as e:
            logger.debug(e)
            return {
                'payload': 'Internal REST handler error',
                'status': 500
            }

        return reply

    @staticmethod
    def get_path_info_parts(request):
        try:
            # the filter() call removes empty strings, ie. /path returns ['path'] instead of ['', 'path']
            return filter(bool, request['path_info'].split('/'))
        except KeyError:
            return []
