
"""
A handler for exposing an equivalent to the mlspl "listmodels" command via a REST endpoint
"""
from models import listmodels
from util.searchinfo_util import searchinfo_from_request


class ListModels(object):
    @classmethod
    def handle_get(cls, request, path_parts):
        """
        Handles GET requests

        Args:
            request: a dictionary providing information about the request
            path_parts: a list of strings describing the request path
        """
        searchinfo = searchinfo_from_request(request)

        models = listmodels.list_models(searchinfo, query_params=[tuple(r) for r in request['query']])

        return {
            'payload': models,
            'status': 200
        }
