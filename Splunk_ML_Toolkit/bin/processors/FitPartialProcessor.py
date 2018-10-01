#!/usr/bin/env python
# Copyright (C) 2015-2017 Splunk Inc. All Rights Reserved.
import pandas as pd

import cexc
import models
from FitBatchProcessor import FitBatchProcessor
from util.base_util import MLSPLNotImplementedError
from util.mlspl_loader import MLSPLConf
from util.lookup_exceptions import ModelNotFoundException
from util.processor_utils import (
    split_options,
    load_resource_limits,
)

logger = cexc.get_logger(__name__)
messages = cexc.get_messages_logger()


class FitPartialProcessor(FitBatchProcessor):
    """The fit partial processor receives and returns pandas DataFrames.

    This processor inherits from FitBatchProcessor and uses a handful of its
    methods. The partial processor does not need sampling and has a few
    additional things it needs to keep track of, including:
        - attempting to load a model
        - checking for discrepancies between search & saved model
        - handling new categorical values as specified by the unseen_value param
    """

    def __init__(self, process_options, searchinfo):
        """Initialize options for processor.

        Args:
            process_options (dict): process options
            client (SplunkRestProxy): splunk rest bouncer wrapper
        """
        # Split apart process & algo options
        self.namespace = process_options.pop('namespace', None)
        mlspl_conf = MLSPLConf(searchinfo)
        self.process_options, self.algo_options = split_options(process_options, mlspl_conf, process_options['algo_name'])
        self.searchinfo = searchinfo

        # Convenience / readability
        self.tmp_dir = self.process_options['tmp_dir']

        # Try load algo from a saved model
        self.algo, self.algo_options = self.initialize_algo_from_model(self.algo_options, self.searchinfo,
                                                                       namespace=self.namespace)
        if self.algo is None:
            # Initialize algo from scratch
            self.algo = self.initialize_algo(self.algo_options, self.searchinfo)
            # Ensure model name is present
            self.check_algo_options(self.algo, self.algo_options)
        else:
            # Check if the loaded model supports partial_fit
            self.check_algo_options(self.algo, self.algo_options)
            # Warn
            self.warn_about_new_parameters()

        self.save_temp_model(self.algo_options, self.tmp_dir)
        self.resource_limits = load_resource_limits(self.algo_options['algo_name'], mlspl_conf)

    @staticmethod
    def initialize_algo_from_model(algo_options, searchinfo, namespace):
        """Init algo from model if possible, and catch discrepancies.

        Args:
            algo_options (dict): algo options
            searchinfo (dict): information required for search
            namespace (string): namespace, 'user' or 'app'
        Returns:
            algo (object/None): loaded algo or None
            algo_options (dict): algo option
        """
        algo = None
        if 'model_name' in algo_options:
            try:
                model_algo_name, algo, model_options = models.base.load_model(
                    algo_options['model_name'], searchinfo, namespace=namespace)
            except ModelNotFoundException:
                algo = None
            except Exception as e:
                cexc.log_traceback()
                raise RuntimeError('Failed to load model "%s". Exception: %s.' % (
                    algo_options['model_name'], str(e)))

            if algo is not None:
                FitPartialProcessor.catch_model_discrepancies(algo_options,
                                                              model_options,
                                                              model_algo_name)

                # Pre 2.2 models do not save algo_name in their model options
                # So we must re add them here to be compatible with 2.2+ versions
                model_options['algo_name'] = algo_options['algo_name']
                algo_options = model_options

        return algo, algo_options

    @staticmethod
    def warn_about_new_parameters():
        cexc.messages.warn(
            'Partial fit on existing model ignores newly supplied parameters. '
            'Parameters supplied at model creation are used instead')

    @staticmethod
    def catch_model_discrepancies(algo_options, model_options, model_algo_name):
        """Check to see if algo name or input columns are different from the model.

        Args:
            algo_options (dict): algo options
            model_options (dict): model options
            model_algo_name (str): name of algo from loaded model
        """
        # Check for discrepancy between algorithm name of the model loaded and algorithm name specified in input
        try:
            assert (algo_options['algo_name'] == model_algo_name)
        except AssertionError:
            raise RuntimeError("Model was trained using algorithm %s but found %s in input" % (
                model_algo_name, algo_options['algo_name']))

        # Check for discrepancy between model columns and input columns
        model_target = model_options.get('target_variable', [])
        algo_target = algo_options.get('target_variable', [])
        model_features = model_options.get('feature_variables', [])
        algo_features = algo_options.get('feature_variables', [])
        if (model_target != algo_target or model_features != algo_features):
            raise RuntimeError("Model was trained on data with different columns than given input")

    @staticmethod
    def check_algo_options(algo, algo_options):
        """Validate processor options.

        Args:
            algo (object): initialized algo
            algo_options (dict): algo options
        """
        if 'model_name' not in algo_options:
            raise RuntimeError('You must save a model if you fit the model with partial_fit enabled')

    @staticmethod
    def fit(algo, df, options):
        """Perform the partial fit.

        Args:
            algo (object): algo object
            df (dataframe): dataframe to fit on
            options (dict): process options

        Returns:
            algo (object): updated algorithm
        """
        try:
            algo.partial_fit(df, options)
        except MLSPLNotImplementedError:
            raise RuntimeError('Algorithm "%s" does not support partial fit' % options['algo_name'])
        except Exception as e:
            cexc.log_traceback()
            raise RuntimeError('Error while fitting "%s" model: %s' % (options['algo_name'], str(e)))

        return algo

    def receive_input(self, df):
        """Override FitBatchProcessor, simply attach df to self.

        Args:
            df (dataframe): dataframe to receive
        """
        self.df = df

    def process(self):
        """Run fit and update algo."""
        self.algo = self.match_and_assign_variables(self.df.columns, self.algo,
                                                    self.algo_options)
        self.algo = self.fit(self.algo,
                             self.df,
                             self.algo_options)

    def get_output(self):
        """Predict if necessary & return appropriate dataframe.

        Returns:
            (dataframe): output dataframe
        """
        self.df = self.algo.apply(self.df, self.algo_options)
        if self.df is None:
            messages.warn('Apply method did not return any results.')
            self.df = pd.DataFrame()

        return self.df
