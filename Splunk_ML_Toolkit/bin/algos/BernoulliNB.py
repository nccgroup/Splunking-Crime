#!/usr/bin/env python

from sklearn.naive_bayes import BernoulliNB as _BernoulliNB

from base import BaseAlgo, ClassifierMixin
from util.param_util import convert_params
from codec import codecs_manager


class BernoulliNB(ClassifierMixin, BaseAlgo):

    def __init__(self, options):
        self.handle_options(options)

        out_params = convert_params(
            options.get('params', {}),
            floats=['alpha', 'binarize'],
            bools=['fit_prior'],
        )

        self.estimator = _BernoulliNB(**out_params)

    @staticmethod
    def register_codecs():
        from codec.codecs import SimpleObjectCodec
        codecs_manager.add_codec('algos.BernoulliNB', 'BernoulliNB', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.naive_bayes', 'BernoulliNB', SimpleObjectCodec)
