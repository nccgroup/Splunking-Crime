import csv
import os

import args_util
import cexc

from searchinfo_util import is_parsetmp

logger = cexc.get_logger(__name__)

def info_csv_to_dict(info_csv_path):
    """
    Parse info.csv to a dict.
    
    Args: 
        info_csv_path (string): Path to the info.csv file
    """
    with open(info_csv_path, mode='r') as f:
        info_reader = csv.DictReader(f)
        info = info_reader.next()
    return info


def is_remote_search(info):
    """
    Check if this search is a remote search.

    Args: 
        info: Dict contains the data from info.csv
    """
    return info.get('_is_remote') == '1'


def get_bundle_path(info):
    """
    Get the path to the current search bundle.

    Args: 
        info: Dict contains the data from info.csv
    """
    result = info['_ppc.bs']

    # Check if is environment variable
    if result.startswith("$"):
        env_var = result[1:]
        result = os.environ[env_var]

    return result


def add_distributed_search_info(process_options, searchinfo):
    """
    Add additional information required for distributed search to searchinfo given.

    Args:
        process_options (dict): the process options to pass to the processor
        searchinfo (dict): information required for search

    Returns:
        searchinfo (dict): the original input searchinfo dict updated with information for distributed search
    """

    # For MLA-1989, in parsetmp search, we do not add anything
    if is_parsetmp(searchinfo):
        return searchinfo

    # In the case we need this before process_options exists
    if process_options is None:
        process_options = searchinfo

    try:
        dispatch_dir = process_options.get('dispatch_dir')
        info = info_csv_to_dict(os.path.join(dispatch_dir, 'info.csv'))
        
        dispatch_base_folder = os.path.dirname(dispatch_dir)

        def get_root_from_info(dispatch_dir):
            """Recursively get _root_sid from info.csv until we find args.txt.

             If _root_sid is present without a value, it should be '' (empty string)
             if it is not present, we will default to None, which are both falsy

            Args:
                dispatch_dir (str): the dispatch directory path or the previous _root_sid value
            Returns
                dispatch_dir (str): the dispatch directory path where we can find args.txt 
            """

            if not dispatch_dir.startswith(dispatch_base_folder):
                dispatch_dir = os.sep.join([dispatch_base_folder, dispatch_dir])

            try:
                if 'args.txt' in os.listdir(dispatch_dir):
                    return dispatch_dir

                some_info = info_csv_to_dict(os.path.join(dispatch_dir, 'info.csv'))
                if some_info.get('_root_sid'):
                    return get_root_from_info(some_info['_root_sid'])
            except IOError as e:
                pass
            return dispatch_dir

        dispatch_dir = get_root_from_info(dispatch_dir)

        searchinfo['bundle_path'] = get_bundle_path(info)
        searchinfo['is_remote'] = is_remote_search(info)

        if searchinfo['is_remote']:
            searchinfo['roles'] = args_util.parse_roles(os.path.join(dispatch_dir, 'args.txt'))

    except Exception as e:
        logger.debug(e)
        cexc.log_traceback()
        raise RuntimeError('Failed to load model "%s": ' % (process_options['model_name']))

    return searchinfo
