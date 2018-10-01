#!/usr/bin/env python

from pandas import DataFrame
from sklearn.ensemble import GradientBoostingClassifier as _GradientBoostingClassifier

from cexc import get_messages_logger
from base import ClassifierMixin, BaseAlgo
from util.param_util import convert_params
from util.algo_util import handle_max_features
from codec import codecs_manager
from codec.codecs import SimpleObjectCodec

messages = get_messages_logger()

class GradientBoostingClassifier(ClassifierMixin, BaseAlgo):
    def __init__(self, options):
        self.handle_options(options)
        params = options.get('params', {})
        out_params = convert_params(
            params,
            strs=['loss', 'max_features'],
            floats=['learning_rate', 'min_weight_fraction_leaf'],
            ints=['n_estimators', 'max_depth', 'min_samples_split',
                  'min_samples_leaf', 'max_leaf_nodes', 'random_state'],
        )

        valid_loss = ['deviance', 'exponential']
        if 'loss' in out_params:
            if out_params['loss'] not in valid_loss:
                msg = "loss must be one of: {}".format(', '.join(valid_loss))
                raise RuntimeError(msg)

        if 'max_features' in out_params:
            out_params['max_features'] = handle_max_features(out_params['max_features'])

        if 'max_leaf_nodes' in out_params and 'max_depth' in out_params:
            messages.warn('max_depth ignored when max_leaf_nodes is set')

        self.estimator = _GradientBoostingClassifier(**out_params)

    def apply(self, df, options):
        # needed for backward compatibility with sklearn 0.17
        # since n_features_ was added in version 0.18
        self.estimator.n_features_ = len(self.columns)
        return super(GradientBoostingClassifier, self).apply(df, options)

    def summary(self, options):
        if len(options) != 2:  # only model name and mlspl_limits
            msg = '"%s" models do not take options for summarization' % self.__class__.__name__
            raise RuntimeError(msg)
        df = DataFrame({
            'feature': self.columns,
            'importance': self.estimator.feature_importances_.ravel()
        })
        return df

    @staticmethod
    def register_codecs():
        from codec.codecs import TreeCodec
        codecs_manager.add_codec('algos.GradientBoostingClassifier',
                                 'GradientBoostingClassifier', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.ensemble.gradient_boosting',
                                 'GradientBoostingClassifier', GBTCodec)
        codecs_manager.add_codec('sklearn.ensemble.gradient_boosting',
                                 'MultinomialDeviance', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.ensemble.gradient_boosting',
                                 'ExponentialLoss', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.ensemble.gradient_boosting',
                                 'LogOddsEstimator', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.ensemble.gradient_boosting',
                                 'ScaledLogOddsEstimator', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.ensemble.gradient_boosting',
                                 'BinomialDeviance', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.ensemble.gradient_boosting',
                                 'PriorProbabilityEstimator', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.tree.tree',
                                 'DecisionTreeRegressor', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.tree._tree',
                                 'Tree', TreeCodec)


class GBTCodec(SimpleObjectCodec):
    @classmethod
    def encode(cls, obj):
        import sklearn.ensemble
        is_clf = (type(obj) == sklearn.ensemble.GradientBoostingClassifier)
        is_reg = (type(obj) == sklearn.ensemble.GradientBoostingRegressor)
        assert is_clf or is_reg

        obj.estimators_ = obj.estimators_.tolist()
        return SimpleObjectCodec.encode(obj)
       
    @classmethod
    def decode(cls, obj):
        import numpy as np
        obj = SimpleObjectCodec.decode(obj)
        obj.estimators_ = np.array(obj.estimators_)
        return obj

