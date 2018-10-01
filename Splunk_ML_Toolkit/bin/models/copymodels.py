import os
import cexc

from util.models_util import (
    move_model_file_from_staging,
    copy_model_to_staging,
    model_name_to_filename,
)

logger = cexc.get_logger(__name__)


def copy_model(source_searchinfo, source_model_name, target_searchinfo, target_model_name):
    """
    copy the source_model_name from given namespace (source_searchinfo) to target namespace (target_searchinfo) with a new name.

    Args:
        source_searchinfo: used to get the namespace of the source model
        source_model_name: the name of the source model
        target_searchinfo: used to get the namespace of the target model
        target_model_name: the name of the target model

    Returns:
        (dict) the reply of the last lookup file POST request
    """
    # copy the model to staging directory
    staging_model_filepath = copy_model_to_staging(source_model_name, source_searchinfo)
    target_file_name = model_name_to_filename(target_model_name)

    # send the model to target space with overwritten
    if os.access(staging_model_filepath, os.R_OK):
        reply = move_model_file_from_staging(target_file_name, target_searchinfo, namespace='user', model_filepath=staging_model_filepath)
    else:
        cexc.log_traceback()
        raise Exception('The temp model file %s is missing or permission denied' % staging_model_filepath)

    return reply
