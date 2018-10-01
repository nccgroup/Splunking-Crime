#!/usr/bin/env python

import pandas as pd
from sklearn.kernel_ridge import KernelRidge as _KernelRidge

from base import BaseAlgo, RegressorMixin
from util.param_util import convert_params
from codec import codecs_manager
from util import df_util


class KernelRidge(RegressorMixin, BaseAlgo):

    def __init__(self, options):
        self.handle_options(options)

        out_params = convert_params(options.get('params', {}), floats=['gamma'])
        out_params['kernel'] = 'rbf'

        self.estimator = _KernelRidge(**out_params)

    def apply(self, df, options=None):
        if options is not None:
            func = super(self.__class__, self).apply
            return df_util.apply_in_chunks(df, func, 1000, options)

    @staticmethod
    def register_codecs():
        from codec.codecs import SimpleObjectCodec
        codecs_manager.add_codec('algos.KernelRidge', 'KernelRidge', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.kernel_ridge', 'KernelRidge', SimpleObjectCodec)
