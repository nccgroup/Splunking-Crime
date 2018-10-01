#!/usr/bin/env python
# Copyright (C) 2015-2017 Splunk Inc. All Rights Reserved.
import errno
import cexc
from models import deletemodels
from BaseProcessor import BaseProcessor

logger = cexc.get_logger(__name__)
messages = cexc.get_messages_logger()


class DeleteModelProcessor(BaseProcessor):
    """The delete processor deletes a saved ML-SPL model."""

    @staticmethod
    def delete_model(process_options, searchinfo, namespace):
        """Actually delete the model.

        Args:
            process_options (dict): process options
            searchinfo (dict): information required for search
            namespace (string): namespace, 'user' or 'app'
        """
        try:
            deletemodels.delete_model(process_options['model_name'], searchinfo, namespace=namespace)
        except (OSError, IOError) as e:
            if e.errno == errno.ENOENT:
                raise RuntimeError('model "%s" does not exist.' % process_options['model_name'])
            raise RuntimeError('Failed to delete model "%s": %s.' % (
                process_options['model_name'], str(e)))
        except Exception as e:
            cexc.log_traceback()
            raise RuntimeError('Failed to delete model "%s": %s.' % (
                process_options['model_name'], str(e)))

        messages.info('Deleted model "%s"' % process_options['model_name'])

    def process(self):
        """Delete the model."""
        self.delete_model(self.process_options, self.searchinfo, namespace=self.namespace)
