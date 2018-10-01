import httplib
import logging
from experiment.experiment_store import ExperimentStore
from experiment.history_store import ExperimentHistoryStore
from rest.proxy import SplunkRestProxyException, SplunkRestException

import cexc
logger = cexc.get_logger(__name__)


def capture_exception(callback, request, url_parts):
    """
    execute `callback` with `request` and `path_parts` parameters
    and handles exceptions generated from `callback`

    Args:
        callback (function): the callback function to execute
        request ([type]): the request object being passed in from rest handler
        path_parts ([type]): the url parts passed in from rest handler

    Returns:
        dict
    """

    try:

        return callback(request, url_parts)
    except (SplunkRestProxyException, SplunkRestException) as e:
        return e.to_http_response()
    except Exception as e:
        return SplunkRestProxyException('Can not complete the request: %s' % str(e), logging.ERROR, httplib.INTERNAL_SERVER_ERROR).to_http_response()

def get_invalid_path_error(path_parts):
    return SplunkRestProxyException('Invalid request path. path: %s' % str(path_parts), logging.ERROR, httplib.BAD_REQUEST)


class Experiments(object):
    """
    A handler for experiments endpoint
    """

    experiment_store = ExperimentStore()
    history_store = ExperimentHistoryStore(with_admin_token=True)

    @classmethod
    def check_experiment_exists(cls, request, url_parts):
        """
        A util function to check if the experiment with the given id exists, and return the response if it does.

        Args:
            request: (dict): the request object being passed in from rest handler
            url_parts: ([type]): the url parts passed in from rest handler

        Returns: (dict) a dict of response from experiment GET request

        """

        experiment_fetch_reply = cls.experiment_store.get(request, url_parts, with_raw_reply=True)
        if not experiment_fetch_reply['success']:
            raise SplunkRestException(experiment_fetch_reply)

        return experiment_fetch_reply

    @classmethod
    def safe_handle_get(cls, request, path_parts):
        path_part_length = len(path_parts)
        url_parts = []

        # Experiment GET request handling
        if 0 < path_part_length <= 2:
            if path_part_length == 2:
                url_parts.append(path_parts[1])
            return cls.experiment_store.get(request, url_parts)

        # History GET request handling
        elif path_part_length == 3:
            if path_parts[2] != "history":
                raise get_invalid_path_error(path_parts)
            url_parts.append(path_parts[1])
            cls.check_experiment_exists(request, url_parts)
            return cls.history_store.get(request, url_parts)

        # Oh noooo we can't do that
        else:
            raise get_invalid_path_error(path_parts)

    @classmethod
    def safe_handle_post(cls, request, path_parts):
        path_part_length = len(path_parts)
        url_parts = []

        # Experiment POST request handling
        if path_part_length <= 2:
            if path_part_length == 2:
                url_parts.append(path_parts[1])
            return cls.experiment_store.post(request, url_parts)

        elif path_part_length == 3:

            # History POST request handling
            if path_parts[2] == "history":
                url_parts.append(path_parts[1])
                cls.check_experiment_exists(request, url_parts)
                return cls.history_store.post(request, url_parts)

            # clone models POST request handling
            elif path_parts[2] == 'clone_models':
                url_parts.append(path_parts[1])
                experiment_fetch_reply = cls.check_experiment_exists(request, url_parts)
                return cls.experiment_store.clone_experiment_models(experiment_fetch_reply, request, url_parts)

            else:
                raise get_invalid_path_error(path_parts)

        # Welp we don't know this
        else:
            raise get_invalid_path_error(path_parts)

    @classmethod
    def safe_handle_delete(cls, request, path_parts):
        url_parts = []
        # Experiment DELETE request handling
        if len(path_parts) == 2:
            url_parts.append(path_parts[1])
            # Delete history when we delete the experiment
            cls.history_store.delete(request, url_parts)
            return cls.experiment_store.delete(request, url_parts)
        else:
            raise get_invalid_path_error(path_parts)

    @classmethod
    def handle_get(cls, request, path_parts):
        return capture_exception(cls.safe_handle_get, request, path_parts)

    @classmethod
    def handle_post(cls, request, path_parts):
        return capture_exception(cls.safe_handle_post, request, path_parts)

    @classmethod
    def handle_delete(cls, request, path_parts):
        return capture_exception(cls.safe_handle_delete, request, path_parts)
