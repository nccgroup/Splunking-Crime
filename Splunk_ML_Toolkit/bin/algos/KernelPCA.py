#!/usr/bin/env python

from sklearn.decomposition import KernelPCA as _KPCA

from .PCA import PCA as PCAAlgo
from util.param_util import convert_params
from codec import codecs_manager
from util import df_util


class KernelPCA(PCAAlgo):

    def __init__(self, options):
        self.handle_options(options)

        out_params = convert_params(
            options.get('params', {}),
            ints=['k', 'degree', 'alpha', 'max_iteration'],
            floats=['gamma', 'tolerance'],
            aliases={'k': 'n_components', 'tolerance': 'tol',
                     'max_iteration': 'max_iter'},
        )

        out_params['kernel'] = 'rbf'

        if 'n_components' not in out_params:
            out_params['n_components'] = min(2, len(options['feature_variables']))
        elif out_params['n_components'] == 0:
            raise RuntimeError('k needs to be greater than zero.')

        self.estimator = _KPCA(**out_params)

    # sklearn's KernelPCA.transform tries to form a complete kernel
    # matrix of its input and the original data the model was fit
    # on. Unfortunately, this might consume a colossal amount of
    # memory for large inputs. We chunk the input to cut down on this.

    def apply(self, df, options=None):
        # Handle backwards compatibility.
        self.estimator.n_jobs = 1

        if options is not None:
            func = super(self.__class__, self).apply
            return df_util.apply_in_chunks(df, func, 1000, options)

    @staticmethod
    def register_codecs():
        from codec.codecs import SimpleObjectCodec
        codecs_manager.add_codec('algos.KernelPCA', 'KernelPCA', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.preprocessing.data', 'KernelCenterer', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.decomposition.kernel_pca', 'KernelPCA', SimpleObjectCodec)
