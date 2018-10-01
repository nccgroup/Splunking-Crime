#!/usr/bin/env python

import numpy as np
from sklearn.cluster import SpectralClustering as _SpectralClustering
from sklearn.preprocessing import StandardScaler

from base import BaseAlgo, ClustererMixin
from util import df_util
from util.param_util import convert_params


class SpectralClustering(ClustererMixin, BaseAlgo):

    def __init__(self, options):
        self.handle_options(options)

        out_params = convert_params(
            options.get('params', {}),
            floats=['gamma'],
            strs=['affinity'],
            ints=['k', 'random_state'],
            aliases={'k': 'n_clusters'},
        )

        self.estimator = _SpectralClustering(**out_params)
        self.scaler = StandardScaler()

    def fit(self, df, options):
        # Make a copy of data, to not alter original dataframe
        X = df.copy()

        X, nans, _ = df_util.prepare_features(
            X=X,
            variables=self.feature_variables,
            mlspl_limits=options.get('mlspl_limits'),
        )

        if len(X) > 0 and len(X) <= self.estimator.n_clusters:
            raise RuntimeError(
                "k must be smaller than the number of events used as input")

        scaled_X = self.scaler.fit_transform(X.values)
        y_hat = self.estimator.fit_predict(scaled_X)
        y_hat = ['' if np.isnan(v) else str('%.0f' % v) for v in y_hat]

        default_name = 'cluster'
        output_name = options.get('output_name', default_name)

        output = df_util.create_output_dataframe(
            y_hat=y_hat,
            nans=nans,
            output_names=output_name,
        )
        df = df_util.merge_predictions(df, output)
        return df
