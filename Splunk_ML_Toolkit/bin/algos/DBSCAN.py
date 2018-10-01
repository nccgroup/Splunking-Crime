#!/usr/bin/env python

from sklearn.cluster import DBSCAN as _DBSCAN

from base import BaseAlgo, ClustererMixin
from util import df_util
from util.param_util import convert_params


class DBSCAN(ClustererMixin, BaseAlgo):

    def __init__(self, options):
        self.handle_options(options)
        out_params = convert_params(options.get('params', {}), floats=['eps'])

        self.estimator = _DBSCAN(**out_params)

    def fit(self, df, options):
        # Make a copy of data, to not alter original dataframe
        X = df.copy()

        X, nans, _ = df_util.prepare_features(
            X=X,
            variables=self.feature_variables,
            mlspl_limits=options.get('mlspl_limits'),
        )

        y_hat = self.estimator.fit_predict(X.values)

        default_name = 'cluster'
        output_name = options.get('output_name', default_name)

        output = df_util.create_output_dataframe(
            y_hat=y_hat,
            nans=nans,
            output_names=output_name,
        )
        df = df_util.merge_predictions(df, output)
        return df
