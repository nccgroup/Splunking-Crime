#!/usr/bin/env python

from sklearn.naive_bayes import GaussianNB as _GaussianNB

from base import BaseAlgo, ClassifierMixin
from codec import codecs_manager


class GaussianNB(ClassifierMixin, BaseAlgo):

    def __init__(self, options):
        self.handle_options(options)
        self.estimator = _GaussianNB()

    @staticmethod
    def register_codecs():
        from codec.codecs import SimpleObjectCodec
        codecs_manager.add_codec('algos.GaussianNB', 'GaussianNB', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.naive_bayes', 'GaussianNB', SimpleObjectCodec)
