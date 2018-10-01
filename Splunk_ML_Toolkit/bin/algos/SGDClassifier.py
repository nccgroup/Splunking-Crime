#!/usr/bin/env python

import numpy as np
import pandas as pd
from sklearn.linear_model import SGDClassifier as _SGDClassifier
from sklearn.preprocessing import StandardScaler

from base import ClassifierMixin, BaseAlgo
from codec import codecs_manager
from codec.codecs import NoopCodec
from util import df_util
from util.param_util import convert_params
from util.algo_util import add_missing_attr


class SGDClassifier(ClassifierMixin, BaseAlgo):

    def __init__(self, options):
        self.handle_options(options)

        out_params = convert_params(
            options.get('params', {}),
            bools=['fit_intercept'],
            ints=['random_state', 'n_iter'],
            floats=['l1_ratio', 'alpha', 'eta0', 'power_t'],
            strs=['loss', 'penalty', 'learning_rate'],
        )

        if 'loss' in out_params:
            try:
                assert (out_params['loss'] in ['hinge', 'log', 'modified_huber', 'squared_hinge', 'perceptron'])
            except AssertionError:
                raise RuntimeError(
                    'Value for parameter "loss" has to be one of "hinge", "log", "modified_huber", "squared_hinge", or "perceptron"')

        self.scaler = StandardScaler()
        self.estimator = _SGDClassifier(**out_params)

    def fit(self, df, options):
        # Check target variable
        df[self.target_variable] = df_util.check_and_convert_target_variable(df, self.target_variable)

        # Make a copy of data, to not alter original dataframe
        X = df.copy()

        relevant_variables = self.feature_variables + [self.target_variable]
        mlspl_limits = options.get('mlspl_limits', {})
        max_classes = int(mlspl_limits.get('max_distinct_cat_values_for_classifier', 100))
        df_util.limit_classes_for_classifier(X, self.target_variable, max_classes)
        X, y, self.columns = df_util.prepare_features_and_target(
            X=X,
            variables=relevant_variables,
            target=self.target_variable,
            mlspl_limits=mlspl_limits,
        )

        scaled_X = self.scaler.fit_transform(X.values)
        self.estimator.fit(scaled_X, y.values)
        self.classes = np.unique(self.target_variable)

    def partial_fit(self, df, options):
        # Handle backwards compatibility.
        add_missing_attr(self.estimator, attr='max_iter', value=5, param_key='n_iter')
        add_missing_attr(self.estimator, attr='tol', value=None)

        # Check target variable
        df[self.target_variable] = df_util.check_and_convert_target_variable(df, self.target_variable)

        # Make a copy of data, to not alter original dataframe
        X = df.copy()

        relevant_variables = self.feature_variables + [self.target_variable]
        mlspl_limits = options.get('mlspl_limits', {})
        max_classes = int(mlspl_limits.get('max_distinct_cat_values_for_classifier', 100))
        df_util.limit_classes_for_classifier(X, self.target_variable, max_classes)
        X, y, columns = df_util.prepare_features_and_target(
            X=X,
            variables=relevant_variables,
            target=self.target_variable,
            mlspl_limits=mlspl_limits,
        )

        if self.classes is None:
            self.classes = np.unique(y)
            scaled_X = self.scaler.fit_transform(X.values)
            self.estimator.partial_fit(scaled_X, y, classes=self.classes)
            self.columns = columns
        else:
            X, y = df_util.handle_new_categorical_values(X, y, options, self.columns, self.classes)
            if X.empty:
                return
            self.scaler.partial_fit(X.values)
            scaled_X = self.scaler.transform(X.values)
            self.estimator.partial_fit(scaled_X, y)

    def apply(self, df, options=None):
        # Handle backwards compatibility.
        add_missing_attr(self.estimator, attr='max_iter', value=5, param_key='n_iter')
        add_missing_attr(self.estimator, attr='tol', value=None)

        # Make a copy of data, to not alter original dataframe
        X = df.copy()

        X, nans, columns = df_util.prepare_features(
            X=X,
            variables=self.feature_variables,
            final_columns=self.columns,
            mlspl_limits=options.get('mlspl_limits'),
        )

        scaled_X = self.scaler.transform(X.values)
        y_hat = self.estimator.predict(scaled_X)

        default_name = 'predicted({})'.format(self.target_variable)
        output_name = options.get('output_name', default_name)

        output = df_util.create_output_dataframe(
            y_hat=y_hat,
            nans=nans,
            output_names=output_name,
        )

        output = df_util.merge_predictions(df, output)
        return output

    def summary(self, options):
        if len(options) != 2:  # only model name and mlspl_limits
            raise RuntimeError('"%s" models do not take options for summarization' % self.__class__.__name__)

        df = pd.DataFrame()
        n_classes = len(self.estimator.classes_)
        limit = 1 if n_classes == 2 else n_classes

        for i, c in enumerate(self.estimator.classes_[:limit]):
            cdf = pd.DataFrame({'feature': self.columns,
                                'coefficient': self.estimator.coef_[i].ravel()})
            cdf = cdf.append(pd.DataFrame({
                'feature': ['_intercept'],
                'coefficient': [self.estimator.intercept_[i]]
            })
            )
            cdf['class'] = c
            df = df.append(cdf)

        return df

    @staticmethod
    def register_codecs():
        from codec.codecs import SimpleObjectCodec
        codecs_manager.add_codec('algos.SGDClassifier', 'SGDClassifier', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.linear_model.stochastic_gradient', 'SGDClassifier', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.preprocessing.data', 'StandardScaler', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.linear_model.sgd_fast', 'Hinge', NoopCodec)
        codecs_manager.add_codec('sklearn.linear_model.sgd_fast', 'Log', NoopCodec)
        codecs_manager.add_codec('sklearn.linear_model.sgd_fast', 'ModifiedHuber', NoopCodec)
        codecs_manager.add_codec('sklearn.linear_model.sgd_fast', 'SquaredHinge', NoopCodec)
