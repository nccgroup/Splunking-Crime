#!/usr/bin/env python
# Copyright (C) 2015-2017 Splunk Inc. All Rights Reserved.
import errno

import cexc
import models.base
from util.base_util import MLSPLNotImplementedError
from util.mlspl_loader import MLSPLConf
from BaseProcessor import BaseProcessor

logger = cexc.get_logger(__name__)
messages = cexc.get_messages_logger()


class SummaryProcessor(BaseProcessor):
    """The summary processor calls the summary method of a saved model."""

    def __init__(self, process_options, searchinfo):
        """Initialize options for the processor.

        We do not need tmp_dir, which is passed from the controller.

        Args:
            process_options (dict): process options
            searchinfo (dict): information required for search
        """
        if 'tmp_dir' in process_options:
            del process_options['tmp_dir']
        self.namespace = process_options.pop('namespace', None)
        self.process_options = process_options
        self.searchinfo = searchinfo

    @staticmethod
    def load_model(model_name, searchinfo, namespace):
        """Try to load the model, error otherwise.

        Args:
            model_name (str): model name
            searchinfo (dict): information required for search
            namespace (string): namespace, 'user' or 'app'
        Returns:
            algo_name (str): algo name
            algo (model object): algo object
            model_options (dict): model options
        """
        try:
            algo_name, algo, model_options = models.base.load_model(model_name, searchinfo,
                                                                    namespace=namespace)
        except (OSError, IOError) as e:
            if e.errno == errno.ENOENT:
                raise RuntimeError('model "%s" does not exist.' % model_name)
            raise RuntimeError('Failed to load model "%s": %s.' % (
                model_name, str(e)))
        except Exception as e:
            cexc.log_traceback()
            raise RuntimeError('Failed to load model "%s": %s.' % (
                model_name, str(e)))
        return algo_name, algo, model_options

    @staticmethod
    def get_summary(algo_name, algo, process_options):
        """Retrieve summary from the algorithm.

        Args:
            algo_name (str): algo name
            algo (object): loaded algo (from a model)
            process_options (dict): process options
        Returns:
            df (dataframe): output dataframe
        """
        try:
            df = algo.summary(process_options)
        except MLSPLNotImplementedError:
            msg = '"{}" models do not support summarization'
            msg = msg.format(algo.__class__.__name__)
            raise RuntimeError(msg)
        except ValueError as e:
            raise RuntimeError(e)
        return df

    def process(self):
        """Load model and call the summary method."""
        algo_name, algo, model_options = self.load_model(self.process_options['model_name'], self.searchinfo,
                                                         namespace=self.namespace)

        # Get defaults from conf file after model is loaded so that we know the algo name
        mlspl_conf = MLSPLConf(self.searchinfo)
        self.process_options['mlspl_limits'] = mlspl_conf.get_stanza(algo_name)
        self.df = self.get_summary(algo_name, algo, self.process_options)
