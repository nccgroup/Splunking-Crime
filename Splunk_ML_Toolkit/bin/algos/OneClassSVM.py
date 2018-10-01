#!/usr/bin/env python

from sklearn.svm.classes import OneClassSVM as _OneClassSVM

from base import BaseAlgo, ClustererMixin
from codec import codecs_manager
from util.param_util import convert_params


class OneClassSVM(ClustererMixin, BaseAlgo):

    def __init__(self, options):
        self.handle_options(options)

        out_params = convert_params(
            options.get('params', {}),
            floats=['gamma', 'coef0', 'tol', 'nu'],
            ints=['degree'],
            bools=['shrinking'],
            strs=['kernel'],
        )

        self.estimator = _OneClassSVM(**out_params)

    def rename_output(self, default_names, new_names):
        return new_names if new_names is not None else 'isNormal'


    @staticmethod
    def register_codecs():
        from codec.codecs import SimpleObjectCodec
        codecs_manager.add_codec('algos.OneClassSVM', 'OneClassSVM', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.svm.classes', 'OneClassSVM', SimpleObjectCodec)
