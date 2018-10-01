#!/usr/bin/env python
import pandas as pd
import numpy as np

import cexc
from base import BaseScoring
from util.param_util import convert_params
from util.base_util import MLSPLNotImplementedError
from util.scoring_util import (
    prepare_classification_scoring_data,
    assert_pos_label_in_field,
    get_union_of_field_values,
    assert_pred_values_are_scores,
    make_true_binary,
    check_class_intersection,
    make_categorical,
    load_scoring_function,
    check_valid_average,
    check_pos_label_and_average,
    assert_binary_field,
    add_default_params,
    check_warn_ignore_pos_label,
)

messages = cexc.get_messages_logger()


class ClassificationScoringMixin(object):
    """Compute classification scorings."""

    def __init__(self, options):
        """ Initialize Scoring class, check options, parse params, and check
        scoring method.
        """
        self.scoring_module_name = 'sklearn.metrics'
        self.scoring_name = options.get('scoring_name')
        self.scoring_function = load_scoring_function(self.scoring_module_name, self.scoring_name)

        self.params, self._meta_params = self.handle_options(options)
        self.actual_field = self.params.pop('actual')
        self.predicted_field = self.params.pop('predicted')
        self.variables = [self.actual_field, self.predicted_field]

    def handle_options(self, options):
        """ Utility to handle options. Verifies that valid options are passed.

        Args:
            options (dict): options containing scoring function params

        Returns:
            params (dict): validated parameters for scoring function
            _meta_params (dict): parameters used in backend but not passed
                                to the scoring function.
        """
        params = options.get('params', {})
        params, _meta_params = self.convert_param_types(params)  # Convert params

        err_msg = ('Syntax error: fields should only be specified as "actual=<actual_field> '
                   'predicted=<predicted_field>".')
        
        if len(options.get('variables', [])) > 0:
            raise RuntimeError(err_msg + ' Found unexpected variable(s) "{}"'.format(', '.join(options['variables'])))

        if 'actual' not in params or 'predicted' not in params:
            raise RuntimeError(err_msg)
        return params, _meta_params

    @staticmethod
    def convert_param_types(params):
        """ Convert scoring function parameters to their correct type.

        Args:
            params (dict): parameters passed through options

        Returns:
            converted_params (dict): Validated parameters of the correct type
            _meta_params (dict): parameters used in backend but not passed to
                                the scoring function
        """
        # Convert parameters
        converted_params = convert_params(params, strs=['actual', 'predicted'])
        # _meta_params dict holds parameters used in backend but not passed to sklearn scorer
        _meta_params = {}
        return converted_params, _meta_params

    def check_params_with_data(self, df, actual_field, predicted_field):
        """ Check parameters against ground-truth values.

        Handle errors regarding cardinality of ground-truth labels
        and check pos_label param, if applicable. Assumed data has already
        been cleaned and made categorical. Overwritten as needed.

        Args:
            df (pd.dataframe): input dataframe
            actual_field (str): name of ground-truth field
            predicted_field (str): name of predicted field

        Raises:
            RuntimeError if params are incompatible with passed data
        """
        msg = 'Scoring method {} does not support "check_params_with_data" method.'
        raise MLSPLNotImplementedError(msg.format(self.scoring_name))

    def prepare_input_data(self, df, actual_field, predicted_field, options):
        """ Prepare the data prior to scoring.

        Preprocess input data, perform parameter validation and
        handles errors. Overwritten as needed.

        Args:
            df (pd.dataframe): input dataframe
            actual_field (str): ground-truth labels field name
            predicted_field (str): predicted labels field name
            options (dict): input options
            
        Returns:
            y_actual (np.array): preprocessed ground-truth labels
            y_predicted (np.array): preprocessed predicted labels
        """
        # remove nans and check limits
        clean_df = prepare_classification_scoring_data(df, actual_field, predicted_field, options.get('mlspl_limits', None))
        # convert to str if needed
        categorical_df = make_categorical(clean_df, [actual_field, predicted_field])

        # Check for inconsistencies with data
        self.check_params_with_data(categorical_df, actual_field, predicted_field)
        # warn if no intersection of actual/predicted fields
        check_class_intersection(categorical_df, actual_field, predicted_field)

        if self._meta_params.get('all_labels', False):  # when average=None or for confusion matrix
            self.params['labels'] = get_union_of_field_values(categorical_df, [actual_field, predicted_field])

        y_actual, y_predicted = categorical_df[actual_field].values, categorical_df[predicted_field].values
        return y_actual, y_predicted

    def score(self, df, options):
        """ Compute the score.

        Args:
            df (pd.DataFrame): input dataframe
            options (dict): passed options

        Returns:
            df_output (pd.dataframe): output dataframe
        """
        # Prepare ground-truth and predicted labels
        y_actual, y_predicted = self.prepare_input_data(df, self.actual_field, self.predicted_field, options)
        # Get the scoring result
        result = self.scoring_function(y_actual, y_predicted, **self.params)
        # Create the output df
        df_output = self.create_output(self.scoring_name, result)
        return df_output

    def create_output(self, scoring_name, result):
        """ Create output dataframe

        Args:
            scoring_name (str): scoring function name
            result (float, dict or array): output of sklearn scoring function

        Returns:
            output_df (pd.DataFrame): output dataframe
        """

        labels = self.params.get('labels', None)

        if labels is not None:  # labels is union of predicted & actual classes. (eg. average=none, confusion matrix)
            output_df = pd.DataFrame(data=[result], columns=labels)
        else:  # otherwise, use scoring name
            output_df = pd.DataFrame(data=[result], columns=[scoring_name])
        return output_df
    

class ROCMixin(ClassificationScoringMixin, BaseScoring):
    """ Mixin class for ROC_curve and ROC_AUC_score"""

    @staticmethod
    def convert_param_types(params):
        out_params = convert_params(params, strs=['actual', 'predicted', 'pos_label'])
        _meta_params = {'pos_label': out_params.pop('pos_label', '1')}  # Pos label used to create true-binary
        return out_params, _meta_params

    def check_params_with_data(self, df, actual_field, predicted_field):
        """roc_auc_score does not accepts multiclass targets"""
        assert_pred_values_are_scores(df, predicted_field)
        multiclass_warn_msg = (
            'Found multiclass ground-truth field "{}". Converting to true binary by setting "{}" as positive and all '
            'other classes as negative'. format(actual_field, self._meta_params['pos_label'])
        )

        assert_binary_field(df, actual_field, multiclass_warn_msg, warn=True)
        assert_pos_label_in_field(df, actual_field, self._meta_params['pos_label'], default_pos_label='1')

    def prepare_input_data(self, df, actual_field, predicted_field, options):
        """ Overwriting parent method.

        Roc_curve & roc_auc_score require binary ground-truth labels in
        true-binary format; pos_label parameter allows for conversion to true
        binary; y_predicted values are scores and cardinality of predicted_field is not
        limited; Since multiclass not supported, average param is disabled.
        """
        # Cleaning and conversions
        clean_df = prepare_classification_scoring_data(df, actual_field, predicted_field, options.get('mlspl_limits', None), False)
        categorical_df = make_categorical(clean_df, [actual_field])
        # Checking values
        self.check_params_with_data(categorical_df, actual_field, predicted_field)
        # Final conversions
        y_actual = make_true_binary(categorical_df, actual_field, self._meta_params['pos_label']).values
        y_predicted = categorical_df[predicted_field].values
        return y_actual, y_predicted


class AccuracyScoring(ClassificationScoringMixin, BaseScoring):
    """ Implements sklearn.metrics.accuracy_score """

    @staticmethod
    def convert_param_types(params):
        out_params = convert_params(params, strs=['actual', 'predicted'], bools=['normalize'])
        _meta_params = {}
        return out_params, _meta_params

    def check_params_with_data(self, df, actual_field, predicted_field):
        """ No parameter checking required"""
        pass


class PrecisionScoring(ClassificationScoringMixin, BaseScoring):
    """ Implements sklearn.metrics.precision_score """

    @staticmethod
    def convert_param_types(params):
        out_params = convert_params(params, strs=['actual', 'predicted', 'average', 'pos_label'])
        check_warn_ignore_pos_label(params, default_average='binary')
        out_params = add_default_params(out_params, {'pos_label': '1', 'average': 'binary'})
        out_params, _meta_params = check_valid_average(out_params, _meta_params={})
        return out_params, _meta_params

    def check_params_with_data(self, df, actual_field, predicted_field):
        check_pos_label_and_average(df, actual_field, self.params)


class RecallScoring(ClassificationScoringMixin, BaseScoring):
    """Implements sklearn.metrics.recall_score """

    @staticmethod
    def convert_param_types(params):
        out_params = convert_params(params, strs=['actual', 'predicted', 'average', 'pos_label'])
        check_warn_ignore_pos_label(params, default_average='binary')
        out_params = add_default_params(out_params, {'pos_label': '1', 'average': 'binary'})
        out_params, _meta_params = check_valid_average(out_params, _meta_params={})
        return out_params, _meta_params

    def check_params_with_data(self, df, actual_field, predicted_field):
        check_pos_label_and_average(df, actual_field, self.params)


class F1Scoring(ClassificationScoringMixin, BaseScoring):
    """ Implements sklearn.metrics.f1_score """

    @staticmethod
    def convert_param_types(params):
        out_params = convert_params(params, strs=['actual', 'predicted', 'average', 'pos_label'])
        check_warn_ignore_pos_label(params, default_average='binary')
        out_params = add_default_params(out_params, {'pos_label': '1', 'average': 'binary'})
        out_params, _meta_params = check_valid_average(out_params, _meta_params={})
        return out_params, _meta_params

    def check_params_with_data(self, df, actual_field, predicted_field):
        check_pos_label_and_average(df, actual_field, self.params)


class PrecisionRecallFscoreSupportScoring(ClassificationScoringMixin, BaseScoring):
    """ Implements sklearn.metrics.precision_recall_fscore_support"""

    @staticmethod
    def convert_param_types(params):
        out_params = convert_params(params, strs=['actual', 'predicted', 'pos_label', 'average'], floats=['beta'])
        check_warn_ignore_pos_label(params, default_average='None')
        out_params = add_default_params(out_params, {'pos_label': '1', 'average': 'None'})
        out_params, _meta_params = check_valid_average(out_params, _meta_params={})
        return out_params, _meta_params

    def check_params_with_data(self, df, actual_field, predicted_field):
        check_pos_label_and_average(df, actual_field, self.params)

    def create_output(self, scoring_name, result):
        """ Output dataframe differs from parent.

        The output shape of precision_recall_fscore_support depends on the
        average value. If average!=None, output is 1x4. If average=None, output
        is nx4 where n is the number of unique classes in y_actual and y_predicted.
        """

        # Labels is populated when average=None. In this case, metrics are computed for each target class.
        labels = self.params.get('labels', None)

        if labels is not None:
            stacked_array = np.vstack(result)  # n x 4
            index_labels = np.array(['precision', 'recall', 'fbeta_score', 'support']).reshape(-1, 1)
            output_array = np.hstack((index_labels, stacked_array))
            col_labels = ['metric'] + ['scored({})'.format(i) for i in labels]  # named for alphabetical sorting
            output_df = pd.DataFrame(data=output_array, columns=col_labels)
        else:
            array = np.array(result).reshape(1, -1)  # 1 x 4
            output_df = pd.DataFrame(data=array, columns=['precision', 'recall', 'fbeta_score', 'support'])
        return output_df


class ConfusionMatrixScoring(ClassificationScoringMixin, BaseScoring):
    """Implements sklearn.metrics.confusion_matrix"""

    @staticmethod
    def convert_param_types(params):
        out_params = convert_params(params, strs=['actual', 'predicted'])
        _meta_params = {'all_labels': True}  # Confusion matrix populates rows & cols with all labels
        return out_params, _meta_params

    def check_params_with_data(self, df, actual_field, predicted_field):
        """ No parameter checking required"""
        pass

    def create_output(self, scoring_name, result):
        """Output dataframe differs from parent.

        The indices of confusion matrix columns/rows should correspond.
        Columns represent predicted results, rows represent ground-truth.
        """
        labels = self.params['labels']  # labels = union of predicted & actual classes
        # Predicted (column) and ground-truth (row) labels
        col_labels = ['label'] + ['predicted({})'.format(i) for i in labels]
        row_labels = pd.DataFrame(['actual({})'.format(i) for i in labels])
        # Create output df
        result_df = pd.DataFrame(result)
        output_df = pd.concat((row_labels, result_df), axis=1)
        output_df.columns = col_labels
        return output_df


class ROCCurveScoring(ROCMixin):
    """ Implements sklearn.metrics.roc_score"""

    @staticmethod
    def convert_param_types(params):
        out_params = convert_params(params, strs=['actual', 'predicted', 'pos_label'], bools=['drop_intermediate'])
        _meta_params = {'pos_label': out_params.pop('pos_label', '1')}
        return out_params, _meta_params

    def create_output(self, scoring_name, result):
        """ Outputs false-positive rate, true-positive rate and thresholds."""
        fpr, tpr, thresholds = result
        return pd.DataFrame({'fpr': fpr, 'tpr': tpr, 'thresholds': thresholds})


class ROCAUCScoring(ROCMixin):
    """Implements sklearn.metrics.roc_auc_score"""
    pass
