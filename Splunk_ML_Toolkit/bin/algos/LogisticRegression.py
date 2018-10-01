#!/usr/bin/env python

import pandas as pd
from sklearn.linear_model import LogisticRegression as _LogisticRegression

from base import BaseAlgo, ClassifierMixin
from codec import codecs_manager
from util import df_util
from util.param_util import convert_params, is_truthy


class LogisticRegression(ClassifierMixin, BaseAlgo):

    def __init__(self, options):
        self.handle_options(options)

        out_params = convert_params(
            options.get('params', {}),
            bools=['fit_intercept', 'probabilities'],
        )

        if 'probabilities' in out_params:
            del out_params['probabilities']

        self.estimator = _LogisticRegression(class_weight='balanced', **out_params)

    def apply(self, df, options):
        # Make a copy of data, to not alter original dataframe
        X = df.copy()

        # Prepare the dataset
        X, nans, columns = df_util.prepare_features(
            X=X,
            variables=self.feature_variables,
            final_columns=self.columns,
            mlspl_limits=options.get('mlspl_limits'),
        )
        # Make predictions
        y_hat = self.estimator.predict(X.values)

        # Assign output_name
        default_name = 'predicted({})'.format(self.target_variable)
        output_name = options.get('output_name', default_name)

        # Create output
        output = df_util.create_output_dataframe(
            y_hat=y_hat,
            nans=nans,
            output_names=output_name,
        )
        if self.check_probabilities(options):
            # predict probs
            y_hat_proba = self.estimator.predict_proba(X.values)

            # get names
            class_names = ['probability({}={})'.format(self.target_variable, cls_name)
                           for cls_name in self.estimator.classes_]

            # create output data frame
            output_proba = df_util.create_output_dataframe(
                y_hat=y_hat_proba,
                nans=nans,
                output_names=class_names,
            )
            # combine
            output = pd.concat([output, output_proba], axis=1)

        df = df_util.merge_predictions(df, output)
        return df

    @staticmethod
    def check_probabilities(options):
        out_params = convert_params(
            options.get('params', {}),
            bools=['probabilities'],
            ignore_extra=True)

        if 'probabilities' in out_params:
            probabilities = is_truthy(out_params['probabilities'])
            del options['params']['probabilities']
        else:
            probabilities = False
        return probabilities

    def summary(self, options):
        if len(options) != 2:  # only model name and mlspl_limits
            raise RuntimeError('"%s" models do not take options for summarization' % self.__class__.__name__)
        df = pd.DataFrame()

        n_classes = len(self.estimator.classes_)
        limit = 1 if n_classes == 2 else n_classes

        for i, c in enumerate(self.estimator.classes_[:limit]):
            cdf = pd.DataFrame({'feature': self.columns,
                                'coefficient': self.estimator.coef_[i].ravel()})
            if not isinstance(self.estimator.intercept_, float):
                cdf = cdf.append(
                    pd.DataFrame({'feature': ['_intercept'], 'coefficient': [self.estimator.intercept_[i]]}))
            cdf['class'] = c
            df = df.append(cdf)

        return df

    @staticmethod
    def register_codecs():
        from codec.codecs import SimpleObjectCodec
        codecs_manager.add_codec('algos.LogisticRegression', 'LogisticRegression', SimpleObjectCodec)
        codecs_manager.add_codec('sklearn.linear_model.logistic', 'LogisticRegression', SimpleObjectCodec)
