#!/user/bin/env python

import pandas as pd
from sklearn.preprocessing import RobustScaler as _RobustScaler
from distutils.version import StrictVersion
from sklearn import __version__ as sklearn_version

import cexc
from base import BaseAlgo, TransformerMixin
from codec import codecs_manager
from util.param_util import convert_params

messages = cexc.get_messages_logger()
quantile_range_required_version = '0.18.2'

class RobustScaler(TransformerMixin, BaseAlgo):

    def __init__(self, options):
        self.handle_options(options)

        out_params = convert_params(
            options.get('params', {}),
            bools=['with_centering', 'with_scaling'],
            strs=['quantile_range'], 
        )

        if StrictVersion(sklearn_version) < StrictVersion(quantile_range_required_version) and 'quantile_range' in out_params.keys():
            out_params.pop('quantile_range')
            msg = 'The quantile_range option is ignored in this version of scikit-learn ({}): version {} or higher required'
            msg = msg.format(sklearn_version, quantile_range_required_version)
            messages.warn(msg)

        if 'quantile_range' in out_params.keys():
            try:
                out_params['quantile_range'] = tuple(int(i) for i in out_params['quantile_range'].split('-'))
                assert len(out_params['quantile_range']) == 2
            except:
                raise RuntimeError('Syntax Error: quantile_range requires a range, e.g., quantile_range=25-75')

        self.estimator = _RobustScaler(**out_params)

    def rename_output(self, default_names, new_names=None):
        if new_names is None:
            new_names = 'RS'
        output_names = [new_names + '_' + feature for feature in self.columns]
        return output_names

    def summary(self, options):
        if len(options) != 2:  # only model name and mlspl_limits
            raise RuntimeError('"%s" models do not take options for summarization' % self.__class__.__name__)
        return pd.DataFrame({'feature': self.columns,
                             'center': self.estimator.center_,
                             'scale': self.estimator.scale_})

    @staticmethod
    def register_codecs():
        from codec.codecs import SimpleObjectCodec
        codecs_manager.add_codec('algos.RobustScaler', 'RobustScaler', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.preprocessing.data', 'RobustScaler', SimpleObjectCodec)
