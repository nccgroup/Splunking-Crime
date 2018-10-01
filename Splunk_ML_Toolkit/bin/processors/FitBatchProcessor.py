#!/usr/bin/env python
# Copyright (C) 2015-2017 Splunk Inc. All Rights Reserved.
import pandas as pd

import cexc
from BaseProcessor import BaseProcessor
import models.base
from models import deletemodels
from util.base_util import match_field_globs
from util.base_util import MLSPLNotImplementedError
from util.algos import initialize_algo_class
from util.mlspl_loader import MLSPLConf
from util.processor_utils import (
    split_options,
    load_resource_limits,
    load_sampler_limits,
    get_sampler,
    check_sampler,
)

logger = cexc.get_logger(__name__)
messages = cexc.get_messages_logger()


class FitBatchProcessor(BaseProcessor):
    """The fit batch processor receives and returns pandas DataFrames."""

    def __init__(self, process_options, searchinfo):
        """Initialize options for processor.

        Args:
            process_options (dict): process options
            searchinfo (dict): information required for search
        """
        # Split apart process & algo options
        self.namespace = process_options.pop('namespace', None)
        mlspl_conf = MLSPLConf(searchinfo)
        self.process_options, self.algo_options = split_options(process_options, mlspl_conf, process_options['algo_name'])

        self.searchinfo = searchinfo

        # Convenience / readability
        self.tmp_dir = self.process_options['tmp_dir']

        self.algo = self.initialize_algo(self.algo_options, self.searchinfo)

        self.check_algo_options(self.algo_options, self.algo)
        self.save_temp_model(self.algo_options, self.tmp_dir)

        self.resource_limits = load_resource_limits(self.algo_options['algo_name'], mlspl_conf)

        self._sampler_time = 0.0
        self.sampler_limits = load_sampler_limits(self.process_options, self.algo_options['algo_name'], mlspl_conf)
        self.sampler = get_sampler(self.sampler_limits)

    @staticmethod
    def initialize_algo(algo_options, searchinfo):

        algo_name = algo_options['algo_name']
        try:
            algo_class = initialize_algo_class(algo_name, searchinfo)
            return algo_class(algo_options)
        except Exception as e:
            cexc.log_traceback()
            raise RuntimeError('Error while initializing algorithm "%s": %s' % (
                algo_name, str(e)))

    @staticmethod
    def check_algo_options(algo_options, algo):
        """Raise errors if options are incompatible

        Args:
            algo_options (dict): algo options
            algo (dict): initialized algo object

        Raises:
            RuntimeError
        """
        # Pre-validate whether or not this algo supports saved models.
        if 'model_name' in algo_options:
            try:
                algo.register_codecs()
            except MLSPLNotImplementedError:
                raise RuntimeError('Algorithm "%s" does not support saved models' % algo_options['algo_name'])
            except Exception as e:
                logger.debug("Error while calling algorithm's register_codecs method. {}".format(str(e)))
                raise RuntimeError('Error while initializing algorithm. See search.log for details.')

    @staticmethod
    def match_and_assign_variables(columns, algo, algo_options):
        """Match field globs and attach variables to algo.

        Args:
            columns (list): columns from dataframe
            algo (object): initialized algo object
            algo_options (dict): algo options

        """
        if hasattr(algo, 'feature_variables'):
            algo.feature_variables = match_field_globs(columns, algo.feature_variables)
        else:
            algo.feature_variables = []

        # Batch fit
        if 'target_variable' in algo_options:
            target_variable = algo_options['target_variable'][0]

            if target_variable in algo.feature_variables:
                algo.feature_variables.remove(target_variable)

        # Partial fit
        elif hasattr(algo, 'target_variable'):
            if algo.target_variable in algo.feature_variables:
                algo.feature_variables.remove(algo.target_variable)

        return algo

    @staticmethod
    def save_temp_model(algo_options, tmp_dir):
        """Save temp model for follow-up apply.

        Args:
            algo_options (dict): algo options
            tmp_dir (str): temp directory to save model to
        """
        if 'model_name' in algo_options:
            try:
                models.base.save_model(algo_options['model_name'], None,
                                  algo_options['algo_name'], algo_options,
                                  model_dir=tmp_dir, tmp=True)
            except Exception as e:
                cexc.log_traceback()
                raise RuntimeError(
                    'Error while saving temporary model "%s": %s' % (algo_options['model_name'], e))

    def get_relevant_fields(self):
        """Ask algo for relevant variables and return as relevant fields.

        Returns:
            relevant_fields (list): relevant fields
        """
        relevant_fields = []
        if 'feature_variables' in self.algo_options:
            self.algo.feature_variables = self.algo_options['feature_variables']
            relevant_fields.extend(self.algo_options['feature_variables'])

        if 'target_variable' in self.algo_options:
            self.algo.target_variable = self.algo_options['target_variable'][0]
            relevant_fields.extend(self.algo_options['target_variable'])

        return relevant_fields

    def save_model(self):
        """Attempt to save the model, delete the temporary model."""
        if 'model_name' in self.algo_options:
            try:
                models.base.save_model(self.algo_options['model_name'], self.algo,
                                  self.algo_options['algo_name'], self.algo_options,
                                  max_size=self.resource_limits['max_model_size_mb'],
                                  searchinfo=self.searchinfo, namespace=self.namespace)
            except Exception as e:
                cexc.log_traceback()
                raise RuntimeError('Error while saving model "%s": %s' % (self.algo_options['model_name'], e))
            try:
                deletemodels.delete_model(self.algo_options['model_name'],
                                    model_dir=self.tmp_dir, tmp=True)
            except Exception as e:
                cexc.log_traceback()
                logger.warn('Exception while deleting tmp model "%s": %s', self.algo_options['model_name'], e)

    def receive_input(self, df):
        """Receive dataframe and append to sampler if necessary.

        Args:
            df (dataframe): dataframe received from controller
        """
        if self.sampler_limits['sample_count'] - len(df) < self.sampler.count <= self.sampler_limits['sample_count']:
            check_sampler(sampler_limits=self.sampler_limits, class_name=self.algo_options['algo_name'])

        with cexc.Timer() as sampler_t:
            self.sampler.append(df)
        self._sampler_time += sampler_t.interval

        logger.debug('sampler_time=%f', sampler_t.interval)

    def process(self):
        """Get dataframe, update algo, and possibly make predictions."""
        self.df = self.sampler.get_df()
        self.algo = self.match_and_assign_variables(self.df.columns, self.algo, self.algo_options)
        self.algo, self.df, self.has_applied = self.fit(self.df, self.algo, self.algo_options)

    @staticmethod
    def fit(df, algo, algo_options):
        """Perform the literal fitting process.

        This method updates the algo by fitting with input data. Some of the
        algorithms additionally make predictions within their fit method, thus
        the predictions are returned in dataframe type. Some other algorithms do
        not make prediction in their fit method, thus None is returned.

        Args:
            df (dataframe): dataframe to fit the algo
            algo (object): initialized/loaded algo object
            algo_options (dict): algo options

        Returns:
            algo (object): updated algo object
            df (dataframe):
                - if algo.fit makes prediction, return prediction
                - if algo.fit does not make prediction, return input df
            has_applied (bool): flag to indicate whether df represents
                original df or prediction df
        """
        try:
            prediction_df = algo.fit(df, algo_options)
        except Exception as e:
            cexc.log_traceback()
            raise RuntimeError('Error while fitting "%s" model: %s' % (algo_options['algo_name'], str(e)))

        has_applied = isinstance(prediction_df, pd.DataFrame)

        if has_applied:
            df = prediction_df

        return algo, df, has_applied

    def get_output(self):
        """Override get_output from BaseProcessor.

        Check if prediction was already made, otherwise make prediction.

        Returns:
            (dataframe): output dataframe
        """
        if not self.has_applied:
            try:
                self.df = self.algo.apply(self.df, self.algo_options)
            except Exception as e:
                cexc.log_traceback()
                logger.debug('Error during apply phase of fit command. Check apply method of algorithm.')
                raise RuntimeError('Error while fitting "%s" model: %s' % (self.algo_options['algo_name'], str(e)))

        if self.df is None:
            messages.warn('Apply method did not return any results.')
            self.df = pd.DataFrame()

        return self.df
