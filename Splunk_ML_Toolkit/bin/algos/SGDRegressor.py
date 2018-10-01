#!/usr/bin/env python

import pandas as pd
from sklearn.linear_model import SGDRegressor as _SGDRegressor
from sklearn.preprocessing import StandardScaler

import cexc
from codec import codecs_manager
from base import RegressorMixin, BaseAlgo
from util.param_util import convert_params
from util import df_util
from util.algo_util import add_missing_attr


class SGDRegressor(RegressorMixin, BaseAlgo):

    def __init__(self, options):
        self.handle_options(options)

        out_params = convert_params(
            options.get('params', {}),
            bools=['fit_intercept'],
            ints=['random_state', 'n_iter'],
            floats=['l1_ratio', 'alpha', 'eta0', 'power_t'],
            strs=['penalty', 'learning_rate'],
        )

        self.scaler = StandardScaler()
        self.estimator = _SGDRegressor(**out_params)
        self.columns = None

    def fit(self, df, options):
        # Make a copy of data, to not alter original dataframe
        X = df.copy()

        relevant_variables = self.feature_variables + [self.target_variable]
        X, y, self.columns = df_util.prepare_features_and_target(
            X=X,
            variables=relevant_variables,
            target=self.target_variable,
            mlspl_limits=options.get('mlspl_limits'),
        )

        scaled_X = self.scaler.fit_transform(X.values)
        self.estimator.fit(scaled_X, y.values)

    def partial_fit(self, df, options):
        # Handle backwards compatibility.
        add_missing_attr(self.estimator, attr='max_iter', value=5, param_key='n_iter')
        add_missing_attr(self.estimator, attr='tol', value=None)

        # Make a copy of data, to not alter original dataframe
        X = df.copy()

        relevant_variables = self.feature_variables + [self.target_variable]
        X, y, columns = df_util.prepare_features_and_target(
            X=X,
            variables=relevant_variables,
            target=self.target_variable,
            mlspl_limits=options.get('mlspl_limits'),
        )

        if self.columns is not None:
            X, y = df_util.handle_new_categorical_values(X, y, options, self.columns)
            if X.empty:
                return
        else:
            self.columns = columns
        self.scaler.partial_fit(X.values)
        scaled_X = self.scaler.transform(X.values)
        self.estimator.partial_fit(scaled_X, y)
        cexc.messages.warn('n_iter is set to 1 when partial fit is performed')

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

        df = df_util.merge_predictions(df, output)
        return df

    def summary(self, options):
        if len(options) != 2:  # only model name and mlspl_limits
            raise RuntimeError('"%s" models do not take options for summarization' % self.__class__.__name__)
        df = pd.DataFrame({'feature': self.columns,
                           'coefficient': self.estimator.coef_.ravel()})
        idf = pd.DataFrame({'feature': '_intercept',
                            'coefficient': self.estimator.intercept_})
        return pd.concat([df, idf])

    @staticmethod
    def register_codecs():
        from codec.codecs import SimpleObjectCodec
        codecs_manager.add_codec('algos.SGDRegressor', 'SGDRegressor', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.linear_model.stochastic_gradient', 'SGDRegressor', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.preprocessing.data', 'StandardScaler', SimpleObjectCodec)
