#!/usr/bin/env python

from sklearn import __version__ as sklearn_version
from distutils.version import StrictVersion
from codec import codecs_manager
from base import BaseAlgo, ClassifierMixin
from util.param_util import convert_params

required_version = '0.18.2'


# Checks sklearn version
def has_required_version():
    return StrictVersion(sklearn_version) >= StrictVersion(required_version)


def raise_import_error():
    msg = 'MLP Classifier is not available in this version of scikit-learn ({}): version {} or higher required'
    msg = msg.format(sklearn_version, required_version)
    raise ImportError(msg)


class MLPClassifier(ClassifierMixin, BaseAlgo):
    def __init__(self, options):
        self.handle_options(options)
        out_params = convert_params(
            options.get('params', {}),
            ints=['batch_size', 'max_iter', 'random_state'],
            floats=['tol', 'momentum'],
            strs=['activation', 'solver', 'learning_rate', 'hidden_layer_sizes'],
        )

        if has_required_version():
            from sklearn.neural_network import MLPClassifier as _MLPClassifier
        else:
            raise_import_error()

        if 'hidden_layer_sizes' in out_params:
            try:
                out_params['hidden_layer_sizes'] = tuple(int(i) for i in out_params['hidden_layer_sizes'].split('-'))
                if len(out_params['hidden_layer_sizes']) < 1:
                    raise RuntimeError('Syntax Error:'
                                       ' hidden_layer_sizes requires range (e.g., hidden_layer_sizes=60-80-100)')
            except RuntimeError:
                raise RuntimeError('Syntax Error:'
                                   ' hidden_layer_sizes requires range (e.g., hidden_layer_sizes=60-80-100)')

        # whitelist valid values for learning_rate, as error raised by sklearn for invalid values is uninformative
        valid_learning_methods = ['constant', 'invscaling', 'adaptive']

        if 'learning_rate' in out_params and out_params.get('learning_rate') not in valid_learning_methods:
            msg = "learning_rate must be one of: {}".format(', '.join(valid_learning_methods))
            raise RuntimeError(msg)

        # stop trying to fit if tol value is invalid
        if out_params.get('tol', 0) < 0:
            raise RuntimeError('Invalid value for tol: "{}" must be > 0.'.format(out_params['tol']))

        if out_params.get('batch_size', 0) < 0:
            raise RuntimeError('Invalid value for batch_size: "{}" must be > 0.'.format(out_params['batch_size']))

        self.estimator = _MLPClassifier(**out_params)

    @staticmethod
    def register_codecs():
        from codec.codecs import SimpleObjectCodec
        codecs_manager.add_codec('algos.MLPClassifier', 'MLPClassifier', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.preprocessing.label', 'LabelBinarizer', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.neural_network.multilayer_perceptron', 'MLPClassifier', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.neural_network._stochastic_optimizers', 'AdamOptimizer', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.neural_network._stochastic_optimizers', 'SGDOptimizer', SimpleObjectCodec)