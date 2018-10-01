#!/usr/bin/env python

from sklearn.decomposition import PCA as _PCA

from base import BaseAlgo, TransformerMixin
from codec import codecs_manager
from util.param_util import convert_params


class PCA(TransformerMixin, BaseAlgo):

    def __init__(self, options):
        self.handle_options(options)
        out_params = convert_params(
            options.get('params', {}),
            ints=['k'],
            aliases={'k': 'n_components'}
        )

        self.estimator = _PCA(**out_params)

    def rename_output(self, default_names, new_names):
        if new_names is None:
            new_names = 'PC'
        output_names = ['{}_{}'.format(new_names, i+1) for i in xrange(len(default_names))]
        return output_names

    @staticmethod
    def register_codecs():
        from codec.codecs import SimpleObjectCodec
        codecs_manager.add_codec('algos.PCA', 'PCA', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.decomposition.pca', 'PCA', SimpleObjectCodec)
