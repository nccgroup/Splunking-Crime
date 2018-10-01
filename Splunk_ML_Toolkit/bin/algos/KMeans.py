#!/usr/bin/env python

from itertools import izip

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans as _KMeans

from base import BaseAlgo, ClustererMixin
from codec import codecs_manager
from util.param_util import convert_params
from util import df_util


class KMeans(ClustererMixin, BaseAlgo):

    def __init__(self, options):
        self.handle_options(options)

        out_params = convert_params(
            options.get('params', {}),
            ints=['k', 'random_state'],
            aliases={'k': 'n_clusters'},
        )

        self.estimator = _KMeans(**out_params)

    def summary(self, options):
        if len(options) != 2:  # only model name and mlspl_limits
            raise RuntimeError('"%s" models do not take options for summarization' % self.__class__.__name__)

        df = pd.DataFrame(data=self.estimator.cluster_centers_, columns=self.columns)
        df['cluster'] = pd.Series(map(str, range(len(self.estimator.cluster_centers_))), df.index)
        idf = pd.DataFrame(data=[self.estimator.inertia_], columns=['inertia'])
        return pd.concat([df, idf], axis=0, ignore_index=True)

    def apply(self, df, options):
        """Apply is overridden to add additional 'cluster_distance' column."""
        # Make a copy of data, to not alter original dataframe
        X = df.copy()


        X, nans, _ = df_util.prepare_features(
            X=X,
            variables=self.feature_variables,
            final_columns=self.columns,
            mlspl_limits=options.get('mlspl_limits'),
        )
        y_hat = self.estimator.predict(X.values)

        default_name = 'cluster'
        output_name = options.get('output_name', default_name)

        output = df_util.create_output_dataframe(
            y_hat=y_hat,
            nans=nans,
            output_names=output_name,
        )
        df_values = X[self.columns].values
        cluster_ctrs = self.estimator.cluster_centers_

        dist = [np.nan if np.isnan(cluster) else
                np.sum(np.square(cluster_ctrs[cluster] - row))
                for (cluster, row) in izip(y_hat, df_values)]

        dist_df = df_util.create_output_dataframe(
            y_hat=dist,
            nans=nans,
            output_names='cluster_distance',
        )

        output = df_util.merge_predictions(output, dist_df)
        df = df_util.merge_predictions(df, output)
        df[output_name] = df[output_name].apply(lambda c: '' if np.isnan(c) else int(c))
        return df

    @staticmethod
    def register_codecs():
        from codec.codecs import SimpleObjectCodec
        codecs_manager.add_codec('algos.KMeans', 'KMeans', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.cluster.k_means_', 'KMeans',
                                 SimpleObjectCodec)
