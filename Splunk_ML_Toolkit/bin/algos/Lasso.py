#!/usr/bin/env python

import pandas as pd
from sklearn.linear_model import Lasso as _Lasso

from base import BaseAlgo, RegressorMixin
from codec import codecs_manager
from util.param_util import convert_params


class Lasso(RegressorMixin, BaseAlgo):

    def __init__(self, options):
        self.handle_options(options)
        out_params = convert_params(options.get('params', {}), floats=['alpha'])
        self.estimator = _Lasso(**out_params)

    def summary(self, options):
        if len(options) != 2:  # only model name and mlspl_limits
            raise RuntimeError('"%s" models do not take options for summarization' % self.__class__.__name__)

        df = pd.DataFrame({'feature': self.columns,
                           'coefficient': self.estimator.coef_.ravel()})
        idf = pd.DataFrame({'feature': ['_intercept'],
                            'coefficient': [self.estimator.intercept_]})
        return pd.concat([df, idf])

    @staticmethod
    def register_codecs():
        from codec.codecs import SimpleObjectCodec
        codecs_manager.add_codec('algos.Lasso', 'Lasso', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.linear_model.coordinate_descent', 'Lasso', SimpleObjectCodec)
