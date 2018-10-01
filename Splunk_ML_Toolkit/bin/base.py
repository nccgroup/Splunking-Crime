#!/usr/bin/env python

import numpy as np

import cexc
from util import df_util
from util.base_util import MLSPLNotImplementedError
from util import algo_util

messages = cexc.get_messages_logger()


class BaseAlgo(object):
    """The BaseAlgo class defines the interface for ML-SPL algorithms.

    All of the relevant entry and exit points to the algo, methods, and special
    attributes are listed below. Inheriting from the BaseAlgo class is not
    required - however, doing so will ensure that the algorithm implements the
    required methods or if that method is called, an error is raised.
    """

    def __init__(self, options):
        """The initialization function.

        This method is **required**. The __init__ method provides the chance to
        check grammar, convert parameters passed into the search, and initialize
        additional objects or imports needed by the algorithm. If none of these
        things are needed, a simple pass or return is sufficient.

        This will be called before the first batch of data comes in.

        The `options` argument passed to this method is closely related to the
        SPL search query. For a simple query such as:

            | fit LinearRegression sepal_width from petal* fit_intercept=t

        The `options` returned will be:

            {
                'args': [u'sepal_width', u'petal*'],
                'params': {u'fit_intercept': u't'},
                'feature_variables': ['petal*'],
                'target_variable': ['sepal_width']
                'algo_name': u'LinearRegression',
                'mlspl_limits': { .. },
            }

        This dictionary of `options` includes:

            - args (list): a list of the fields used
            - params (dict): any parameters (key-value) pairs in the search
            - feature_variables (list): fields to be used as features
            - target_variable (str): the target field for prediction
            - algo_name (str): the name of algorithm
            - mlspl_limits (dict): mlspl.conf stanza properties that may be used in utility methods

        Other keys that may exist depending on the search:

            - model_name (str): the name of the model being saved ('into' clause)
            - output_name (str): the name of the output field ('as' clause)

        The feature_fields and target_field are related to the syntax of the
        search as well. If a 'from' clause is present:

            | fit LinearRegression target_variable from feature_variables

        whereas with an unsupervised algorithm such as KMeans,

            | fit KMeans feature_variables

        It is important to note is that these feature_variables in the `options`
        have not been wildcard matched against the available data, meaning, that
        if there is a wildcard * in the field names, the wildcards are still
        present.
        """
        self.feature_variables = []
        self.target_variable = None
        msg = 'The {} algorithm cannot be initialized.'
        msg = msg.format(self.__class__.__name__)
        raise MLSPLNotImplementedError(msg)

    def fit(self, df, options):
        """The fit method creates and updates a model - it may make predictions.

        The fit method is only called during the fit command and is **required**.
        The fit method is the central and most important part of adding an algo.
        After the __init__ has been called, the field wildcards have been matched
        and the available variables are now attached to two attributes on self:

            self.feature_variables (list): fields to use for predicting

        and if the search uses a 'from' clause:

            self.target_variable (str): the field to predict

        If the algorithm necessarily makes predictions while fitting, return
        the output DataFrame here. Additionally, if the algorithm cannot be
        saved, make predictions and return them. Otherwise, make predictions in
        the apply method and do not return anything here.

        The `df` argument is a pandas DataFrame from the search results. Note
        that modification to `df` within this method will also modify the
        dataframe to be used in the subsequent apply method.

        The `options` argument is the same as those described in the __init__
        method.
        """
        msg = 'The {} algorithm does not support fit.'
        msg = msg.format(self.__class__.__name__)
        raise MLSPLNotImplementedError(msg)

    def partial_fit(self, df, options):
        """The partial_fit method updates a model incrementally.

        partial_fit is used in the fit command when partial_fit=t is added to the
        search. It is for incrementally updating an algorithm. If the algorithm
        does not require a full dataset in order to update, partial_fit can
        be used to update the estimator with each "chunk" of data, rather than
        waiting for the full dataset to arrive.

        On the initial partial_fit, the `options` are the same as described in
        the fit method, however, on the subsequent calls - the `options` from the
        initial fit are used.

        The `df` argument is a pandas DataFrame from the search results.

        The `options` argument is the same as those described in the __init__
        method.
        """
        msg = 'The {} algorithm does not support partial_fit.'
        msg = msg.format(self.__class__.__name__)
        raise MLSPLNotImplementedError(msg)

    def apply(self, df, options):
        """The apply method creates predictions.

        The apply method is used in both the fit command and the apply command.
        In the fit command, the apply method is used when the fit method does
        not return a pandas DataFrame. If apply=f is added to the fit command,
        the apply method will not be called.

        In the apply command, this method is always called. The apply method is
        only necessary when a saved model is needed.

        When the apply method is used in the fit command, the `options` are the
        same as those in the __init__ method. A search like this:

            | fit LinearRegression y from X1 X2

        would return `options` in the apply method like this:

            {
                'args': [u'y', u'X1', 'X2'],
                'algo_name': u'LinearRegression',
                'feature_variables': ['X1', 'X2'],
                'target_variable': ['y'],
                'mlspl_limits': { ... },
            }

        When the apply method is used in the apply command, the `options`
        represent those saved with the model in addition to those passed into
        the search. Algorithm specific parameters such as k=4, are ignored when
        applying a model. The `options` from the following search:

            | fit LogisticRegression y from X1 X2 into model as new_name

        would be:

            {
                'args': [u'y', u'X1', 'X2'],
                'algo_name': u'LogisticRegression',
                'model_name': 'model',
                'output_name': 'new_name',
                'feature_variables': ['X1', 'X2'],
                'target_variable': ['y'],
                'mlspl_limits': { ... },
            }

        however, if applying the model so:

            | apply model as some_other_name

        the `options` would be:

            {
                'args': [u'y', u'X1', 'X2'],
                'algo_name': u'LogisticRegression',
                'model_name': 'model',
                'output_name': 'some_other_name',
                'feature_variables': ['X1', 'X2'],
                'target_variable': ['y'],
                'mlspl_limits': { ... },
            }

        where the output_name has been updated.

        The `df` argument is a pandas DataFrame from the search results.

        The `options` argument is the same as those described in the __init__
        method.
        """
        msg = 'The {} algorithm does not support apply.'
        msg = msg.format(self.__class__.__name__)
        raise MLSPLNotImplementedError(msg)

    def summary(self, options):
        """The summary method defines how to summarize the model.

        The summary method is only necessary with a saved model. This method
        must return a pandas DataFrame.

        By default, the `options` dictionary only returns:

            {
                'model_name': 'some_custom_model_name',
                'mlspl_limits': { ... },
            }


        Parameters added to the search will be added to the `options`.

        An example:

            | summary my_custom_model key=value

        will return

            {
                'model_name': 'some_custom_model_name',
                'mlspl_limits': { ... },
                'params': {'key': 'value'},
            }

        as the `options`.
        """
        msg = 'The {} algorithm does not support summary.'
        msg = msg.format(self.__class__.__name__)
        raise MLSPLNotImplementedError(msg)

    @staticmethod
    def register_codecs():
        """The register codecs method defines how to save a model.

        ML-SPL uses custom codecs to serialize (save) and deserialize (load)
        the python objects that represent the model. The MLTK comes with a
        variety of pre-defined codecs to serialize objects like numpy arrays,
        pandas DataFrames, and other common python objects.

        Most likely, a model can be saved by using the SimpleObjectCodec:

            >>> from codec.codecs import SimpleObjectCodec
            >>> codecs_manager.add_codec('algos.CustomAlgo', 'CustomAlgo', SimpleObjectCodec)

        If there are imported modules from the Python for Scientific Computing
        app, such as scikit-learn's StandardScaler, they must also be added:

            >>> codecs_manager.add_codec('sklearn.preprocessing.data', 'StandardScaler', SimpleObjectCodec)

        In the less likely chance that a algorithm has circular references or
        something unusual about it, a custom codec might be required. Codecs
        define how to serialize and deserialize a python object into a string.
        More examples can be found in codec/codecs.py.
        """
        msg = 'The algorithm does not support saving.'
        raise MLSPLNotImplementedError(msg)


class RegressorMixin(object):
    """Defines methods to setup and make numeric predictions.

    The RegressorMixin is useful for supervised learning problems where the
    target variable is numeric. Additional methods defined here are:

        - handle_options
        - rename_output

    See algos/LinearRegression.py for an example of using this mixin.
    """

    def handle_options(self, options):
        """Utility to ensure there are both target and feature variables"""
        if len(options.get('target_variable', [])) != 1 or len(options.get('feature_variables', [])) == 0:
            raise RuntimeError('Syntax error: expected "<target> FROM <field> ..."')

    def fit(self, df, options):
        # Make a copy of data, to not alter original dataframe
        X = df.copy()

        # Union of variables are needed
        used_variables = self.feature_variables + [self.target_variable]

        # Prepare the dataset
        X, y, self.columns = df_util.prepare_features_and_target(
            X=X,
            variables=used_variables,
            target=self.target_variable,
            mlspl_limits=options.get('mlspl_limits'),
        )

        # Fit the estimator
        self.estimator.fit(X.values, y.values)

    def apply(self, df, options):
        # Make a copy of data, to not alter original dataframe
        X = df.copy()

        # Prepare the dataset
        X, nans, _ = df_util.prepare_features(
            X=X,
            variables=self.feature_variables,
            final_columns=self.columns,
            mlspl_limits=options.get('mlspl_limits'),
        )
        # Make predictions
        y_hat = self.estimator.predict(X.values)

        # Assign output_name
        default_name = 'predicted({})'.format(self.target_variable)
        new_name = options.get('output_name', None)
        output_name = self.rename_output(default_names=default_name,
                                         new_names=new_name)

        # Create output
        output = df_util.create_output_dataframe(
            y_hat=y_hat,
            nans=nans,
            output_names=output_name,
        )

        # Merge with original dataframe
        output = df_util.merge_predictions(df, output)
        return output

    def rename_output(self, default_names, new_names=None):
        """Utility hook to rename output.

        The default behavior is to take the default_names passed in and simply
        return them. If however a particular algo needs to rename the columns of
        the output, this method can be overridden.
        """
        return new_names if new_names is not None else default_names


class ClassifierMixin(object):
    """Defines methods to setup and make categorical predictions.

    The ClassifierMixin is useful for supervised learning problems where the
    target variable is categorical. One special aspect of the classifier is that
    we set the 'classes' attribute on self to keep track of the target_variable's
    unique values.

    Additional methods defined here are:
        - handle_options
        - rename_output

    See algos/SVM.py for an example of using this mixin.
    """
    def handle_options(self, options):
        """Utility to ensure both feature_variables and target_variable exist."""
        if len(options.get('target_variable', [])) != 1 or len(options.get('feature_variables', [])) == 0:
            raise RuntimeError('Syntax error: expected "<target> FROM <field> ..."')
        self.classes = None

    def fit(self, df, options):
        # Check target variable
        df[self.target_variable] = df_util.check_and_convert_target_variable(df, self.target_variable)

        # Make a copy of data, to not alter original dataframe
        X = df.copy()

        # Ensure there aren't too many classes
        mlspl_limits = options.get('mlspl_limits', {})
        max_classes = int(mlspl_limits.get('max_distinct_cat_values_for_classifiers', 100))
        df_util.limit_classes_for_classifier(X, self.target_variable, max_classes)

        # Use all the variables
        used_variables = self.feature_variables + [self.target_variable]
        X, y, self.columns = df_util.prepare_features_and_target(
            X=X,
            variables=used_variables,
            target=self.target_variable,
            mlspl_limits=mlspl_limits,
        )
        # Fit the estimator
        self.estimator.fit(X.values, y.values)

        # Save the classes
        self.classes = np.unique(y)

    def partial_fit(self, df, options):
        # Check target variable
        df[self.target_variable] = df_util.check_and_convert_target_variable(df, self.target_variable)

        # Make a copy of data, to not alter original dataframe
        X = df.copy()

        # Ensure that partial_fit method is defined for the estimator
        algo_util.assert_estimator_supports_partial_fit(self.estimator)

        # Ensure there aren't too many classes
        mlspl_limits = options.get('mlspl_limits', {})
        max_classes = int(mlspl_limits.get('max_distinct_cat_values_for_classifiers', 100))
        df_util.limit_classes_for_classifier(X, self.target_variable, max_classes)

        # Use all the variables
        used_variables = self.feature_variables + [self.target_variable]

        # Prepare the dataset
        X, y, columns = df_util.prepare_features_and_target(
            X=X,
            variables=used_variables,
            target=self.target_variable,
            mlspl_limits=mlspl_limits,
        )

        # On the very first partial call
        if self.classes is None:
            self.classes = np.unique(y)
            self.estimator.partial_fit(X.values, y.values, self.classes)
            self.columns = columns

        # On subsequent partial_fit calls
        else:
            X, y = df_util.handle_new_categorical_values(X, y, options, self.columns, self.classes)
            if not X.empty:
                self.estimator.partial_fit(X.values, y.values)

    def apply(self, df, options):
        # Make a copy of data, to not alter original dataframe
        X = df.copy()

        # Prepare the dataset
        X, nans, _ = df_util.prepare_features(
            X=X,
            variables=self.feature_variables,
            final_columns=self.columns,
            mlspl_limits=options.get('mlspl_limits'),
        )
        # Make predictions
        y_hat = self.estimator.predict(X.values)

        # Assign output_name
        default_name = 'predicted({})'.format(self.target_variable)
        new_name = options.get('output_name', None)
        output_name = self.rename_output(default_names=default_name,
                                         new_names=new_name)

        # Create output dataframe
        output = df_util.create_output_dataframe(
            y_hat=y_hat,
            nans=nans,
            output_names=output_name,
        )

        # Merge with original dataframe
        output = df_util.merge_predictions(df, output)
        return output

    def rename_output(self, default_names, new_names=None):
        """Utility hook to rename output.

        The default behavior is to take the default_names passed in and simply
        return them. If however a particular algo needs to rename the columns of
        the output, this method can be overridden.
        """
        return new_names if new_names is not None else default_names


class ClustererMixin(object):
    """Defines methods to setup and cluster data.

    The ClustererMixin is useful for unsupervised learning problems.

    Additional methods defined here are:
        - handle_options
        - rename_output

    See algos/KMeans.py for an example of using this mixin.
    """
    def handle_options(self, options):
        """Utility to ensure there are feature_variables and no target_variable."""
        if len(options.get('feature_variables', [])) == 0 or len(options.get('target_variable', [])) > 0:
            raise RuntimeError('Syntax error: expected "<field> ..."')

    def fit(self, df, options):
        # Make a copy of data, to not alter original dataframe
        X = df.copy()

        X, _, self.columns = df_util.prepare_features(
            X=X,
            variables=self.feature_variables,
            mlspl_limits=options.get('mlspl_limits'),
        )
        self.estimator.fit(X.values)

    def partial_fit(self, df, options):
        # Make a copy of data, to not alter original dataframe
        X = df.copy()

        algo_util.assert_estimator_supports_partial_fit(self.estimator)
        X, _, columns = df_util.prepare_features(
            X=X,
            variables=self.feature_variables,
            mlspl_limits=options.get('mlspl_limits'),
        )

        if getattr(self, 'columns', None):
            df_util.handle_new_categorical_values(X, None, options, self.columns)
            if X.empty:
                return
        else:
            self.columns = columns

        self.estimator.partial_fit(X)

    def apply(self, df, options):
        # Make a copy of data, to not alter original dataframe
        X = df.copy()

        X, nans, _ = df_util.prepare_features(
            X=X,
            variables=self.feature_variables,
            final_columns=self.columns,
            mlspl_limits=options.get('mlspl_limits'),
        )
        y_hat = self.estimator.predict(X.values)

        # Ensure the output has no floating points
        y_hat = y_hat.astype('str')

        # Assign output_name
        default_name = 'cluster'
        new_name = options.get('output_name', None)
        output_name = self.rename_output(default_names=default_name,
                                         new_names=new_name)

        # Create output dataframe
        output = df_util.create_output_dataframe(
            y_hat=y_hat,
            nans=nans,
            output_names=output_name,
        )

        # Merge with original dataframe
        output = df_util.merge_predictions(df, output)
        return output

    def rename_output(self, default_names, new_names=None):
        """Utility hook to rename output.

        The default behavior is to take the default_names passed in and simply
        return them. If however a particular algo needs to rename the columns of
        the output, this method can be overridden.
        """
        return new_names if new_names is not None else default_names


class TransformerMixin(object):
    """Defines methods to setup and arbitrarily transform data.

    The TransformerMixin is useful for unsupervised learning problems or
    arbitrary data transformations are required.

    Additional methods defined here are:
        - handle_options
        - rename_output
        - make_output_names

    See algos/PCA.py for an example of using this mixin.
    """
    def handle_options(self, options):
        """Utility to ensure there are feature_variables and no target_variable."""
        if len(options.get('feature_variables', [])) == 0 or len(options.get('target_variable', [])) > 0:
            raise RuntimeError('Syntax error: expected "<field> ..."')

    def fit(self, df, options):
        # Make a copy of data, to not alter original dataframe
        X = df.copy()

        # Prepare features
        X, _, self.columns = df_util.prepare_features(
            X=X,
            variables=self.feature_variables,
            mlspl_limits=options.get('mlspl_limits'),
        )

        # Fit the estimator
        self.estimator.fit(X.values)

    def make_output_names(self, output_name=None, n_names=None):
        """Utility to produce default column names for the output."""
        if output_name is None:
            output_name = str(self.__class__.__name__)

        output_names = [feature + '_' + str(index) for index, feature in
                        enumerate([output_name] * n_names)]
        return output_names

    def apply(self, df, options):
        # Make a copy of data, to not alter original dataframe
        X = df.copy()

        # Prepare the features
        X, nans, _ = df_util.prepare_features(
            X=X,
            variables=self.feature_variables,
            final_columns=self.columns,
            mlspl_limits=options.get('mlspl_limits'),
        )

        # Call the transform method
        y_hat = self.estimator.transform(X.values)

        # Assign output_name
        output_name = options.get('output_name', None)
        default_names = self.make_output_names(
            output_name=output_name,
            n_names=y_hat.shape[1],
        )
        output_names = self.rename_output(default_names, output_name)

        # Create output dataframe
        output = df_util.create_output_dataframe(
            y_hat=y_hat,
            nans=nans,
            output_names=output_names,
        )

        # Merge with original dataframe
        output = df_util.merge_predictions(df, output)
        return output

    def rename_output(self, default_names, new_names=None):
        """Utility hook to rename output.

        The default behavior is to take the default_names passed in and simply
        return them. If however a particular algo needs to rename the columns of
        the output, this method can be overridden.
        """
        return new_names if new_names is not None else default_names


class BaseScoring(object):
    """The BaseScore class defines the interface for ML-SPL score command methods.

    """

    def __init__(self, options):
        """The initialization function.
        Handle the options, 1) parse the params, 2) gather field names
        """
        self.variables = None
        msg = 'The {} scoring cannot be initialized.'
        msg = msg.format(self.__class__.__name__)
        raise MLSPLNotImplementedError(msg)

    def score(self, df, options):
        """The main function to be implemented for score command.
        """
        msg = 'The {} scoring does not support score.'
        msg = msg.format(self.__class__.__name__)
        raise MLSPLNotImplementedError(msg)
