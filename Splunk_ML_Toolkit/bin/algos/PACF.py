#!/usr/bin/env python

from statsmodels.tsa.stattools import pacf

from algos.ACF import ACF
from util.algo_util import confidence_interval_to_alpha
from util.param_util import convert_params


class PACF(ACF):
    """Compute partial autocorrelation function."""

    def __init__(self, options):

        self.handle_options(options)

        params = options.get('params', {})
        converted_params = convert_params(
            params,
            ints=['k', 'conf_interval'],
            strs=['method'],
            aliases={'k': 'nlags'},
        )

        # Used in ACF
        self.default_name = 'pacf({})'

        # Set the lags and  method parameters
        self.nlags = converted_params.pop('nlags', 40)
        self.method = converted_params.pop('method', 'ywunbiased')

        conf_int = converted_params.pop('conf_interval', 95)
        if conf_int <= 0 or conf_int >= 100:
            raise RuntimeError('conf_interval cannot be less than 1 or more than 99.')
        self.alpha = confidence_interval_to_alpha(conf_int)

    def _calculate(self, df):
        """Calculate the PACF.

        Args:
            X (dataframe): input data

        Returns:
            partial_autocors (array): array of partial autocorrelations
            conf_int (array): array of confidence intervals
        """

        partial_autocors, conf_int = pacf(
            x=df.values,
            nlags=self.nlags,
            alpha=self.alpha,
            method=self.method
        )

        return partial_autocors, conf_int
