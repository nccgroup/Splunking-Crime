#!/usr/bin/env python

from pandas import DataFrame
from sklearn.ensemble import RandomForestClassifier as _RandomForestClassifier

from base import ClassifierMixin, BaseAlgo
from codec import codecs_manager
from util.param_util import convert_params
from util.algo_util import handle_max_features


class RandomForestClassifier(ClassifierMixin, BaseAlgo):

    def __init__(self, options):
        self.handle_options(options)

        out_params = convert_params(
            options.get('params', {}),
            ints=['random_state', 'n_estimators', 'max_depth',
                  'min_samples_split', 'max_leaf_nodes'],
            strs=['max_features', 'criterion'],
        )

        if 'max_depth' not in out_params:
            out_params.setdefault('max_leaf_nodes', 2000)

        if 'max_features' in out_params:
            out_params['max_features'] = handle_max_features(out_params['max_features'])

        self.estimator = _RandomForestClassifier(class_weight='balanced',
                                                 **out_params)

    def summary(self, options):
        if len(options) != 2:  # only model name and mlspl_limits
            raise RuntimeError('"%s" models do not take options for summarization' % self.__class__.__name__)
        df = DataFrame({
            'feature': self.columns,
            'importance': self.estimator.feature_importances_.ravel()
        })
        return df

    @staticmethod
    def register_codecs():
        from codec.codecs import SimpleObjectCodec, TreeCodec
        codecs_manager.add_codec('algos.RandomForestClassifier',
                                 'RandomForestClassifier', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.ensemble.forest',
                                 'RandomForestClassifier', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.tree.tree', 'DecisionTreeClassifier',
                                 SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.tree._tree', 'Tree', TreeCodec)
