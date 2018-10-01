# !/usr/bin/env python
import importlib

import numpy as np

import cexc
from df_util import (
    assert_field_present,
    drop_unused_and_missing,
    assert_any_rows,
)


###################################
# Common scoring utilities
###################################
def load_scoring_function(module_name, func_name):
    """Load the scoring algorithm from correct module.

    Args:
        module_name (str): name of the module to load (eg. sklearn.metrics)
        func_name (str): name of the scoring function to load

    Returns:
        scoring (function): scoring function loaded from module
    """
    try:
        scoring_module = importlib.import_module(module_name)
        scoring = getattr(scoring_module, func_name)
    except (ImportError, AttributeError):
        cexc.log_traceback()
        err_msg = 'Scoring method {} is not available'.format(func_name)
        raise RuntimeError(err_msg)
    return scoring


def assert_binary_field(df, field, msg=None, warn=False):
    """ Assert that the cardinality of the field is <=2.

    Args:
        df (pd.Dataframe): input pd dataframe
        field (str): field to assert binary
        msg (str): The error message to return if field is not binary
        warn (bool): Whether to raise warning or runtime error

    Raises/warns:
        RuntimeError/cexc messages warning
    """
    assert_field_present(df, field)
    field_cardinality = df[field].nunique()
    if msg is None:
        msg = 'Expected field "{}" to be binary, but found {} classes.'.format(field, field_cardinality)
    if field_cardinality > 2:
        if warn:
            cexc.messages.warn(msg)
        else:
            raise RuntimeError(msg)


def make_true_binary(df, field, pos_label):
    """ Converts targets to true-binary targets in {0, 1}.

    Assumes all entries in df are strings. Meant to be called
    after "make_categorical".

    Args:
        df (pd.Dataframe): input pd dataframe
        field (str): field to encode to true-binary
        pos_label (str): value to set as true-binary-positive (i.e. 1)

    Returns:
        true_binary_outp (pd series): true-binary output series
    """
    # Ensure fields present
    assert_field_present(df, field)
    # Check if field values are already in true-binary form
    classes_set = set(df[field].unique())
    if classes_set == {0, 1} or classes_set == {-1, 1}:
        return df[field]  # Already true-binary
    else:
        # Create a mask and apply map
        positive_mask = df[field] == pos_label
        true_binary_outp = positive_mask.astype(int)
        return true_binary_outp


def field_is_numeric(df, field):
    """ Check if the given field is numeric.

    Args:
        df (pd.DataFrame): input dataframe
        field (str): field to check kind of

    Returns:
        bool: whether field is numeric or not
    """
    return df[field].dtype.kind in ['i', 'u', 'f', 'c']  # Is field numeric


def get_union_of_field_values(df, fields):
    """ Gets the union of values from each field.

    Args:
        df (pd.DataFrame): input dataframe
        fields (list): fields to involve in the union

    Return:
        all labels (np.array of str): union of field-values sorted
        alphabetically and converted to strings.
    """
    unique_values = []
    for field in fields:
        unique_values.append(df[field].unique())
    all_labels = np.sort(np.unique(np.concatenate(unique_values)))
    return all_labels


def check_class_intersection(df, true_field, pred_field):
    """ Warns when there is no intersection of pred and true field values.

    Args:
        df (pd.dataframe): input dataframe
        true_field (str): name of ground-truth field
        pred_field (str): name of predicted field

    Raises:
        Warning that there is no intersection if applicable.
    """
    true_set = set(df[true_field])
    pred_set = set(df[pred_field])
    intersection = true_set.intersection(pred_set)
    if len(intersection) == 0:
        msg = ('The predicted and actual fields do not contain the same class variables. Please ensure that the '
               '"actual=<actual_field> predicted=<predicted_field>" are correctly identified.')
        cexc.messages.warn(msg)


def make_categorical(df, fields):
    """ Converts field values to categorical representations.

    Converts to categorical representation.
    - Converts integer-elements to string
    - Converts floats --> int --> str if float ~= int
    - Raises error if float !~= int (i.e. ambiguous categorical representation)

    Args:
        df (pd.dataframe): input dataframe
        fields (list of strs): fields to convert

    Returns:
        df (pd.dataframe): input dataframe converted to categorical inplace
    """

    float_err_msg = ('Value error: field "{}" is not a valid categorical field since '
                     'it contains floats with ambiguous categorical representation.')
    for field in fields:
        float_mask = df[field].apply(lambda x: isinstance(x, float))  # Get a mask of any floats
        if float_mask.sum() > 0:  # Contains float-types
            float_components = df[field][float_mask]
            if np.isclose(float_components.astype(float), float_components.astype(int)).all():
                df[field] = df[field].apply(lambda x: int(x) if isinstance(x, float) else x)  # convert to int
            else:
                raise RuntimeError(float_err_msg.format(field))

        df[field] = df[field].astype(str)
    return df


###################################
# Classification scoring utilities
###################################
def prepare_classification_scoring_data(df, true_field,  pred_field, mlspl_limits=None, limit_pred_field=True):
    """Prepare classification-scoring data.

    This method defines conventional steps to prepare features:
        - drop unused columns
        - drop rows that have missing values

    Args:
        df (dataframe): input dataframe
        true_field (str): ground_truth label field
        pred_field (str): predicted labels field
        mlspl_limits (dict): a dictionary containing values from mlspl conf
        limit_pred_field (bool): whether or not to check cardinality of pred field

    Returns:
        df (dataframe): prepared classification-scoring data dataframe
    """
    assert_field_present(df, true_field)
    assert_field_present(df, pred_field)

    if mlspl_limits is None:
        mlspl_limits = {}

    max_distinct_cat_values = int(mlspl_limits.get('max_distinct_cat_values_for_scoring', 100))

    # Remove nans
    n_rows_original = len(df)
    clean_df, nans = drop_unused_and_missing(df, [true_field, pred_field])
    total_nan = nans.sum().sum()
    if total_nan > 0:
        cexc.messages.warn('Removed {} rows containing missing values ({} % of original '
                           'data.)'.format(total_nan, float(total_nan) / n_rows_original * 100))

    msg = 'Value error: cardinality of {} field "{}" exceeds limit of {}.'
    true_cardinality = df[true_field].nunique()
    if true_cardinality > max_distinct_cat_values:
        raise RuntimeError(msg.format('ground-truth', true_field, max_distinct_cat_values))

    if limit_pred_field:
        pred_cardinality = df[pred_field].nunique()
        if pred_cardinality > max_distinct_cat_values:
            raise RuntimeError(msg.format('predicted', pred_field, max_distinct_cat_values))

        if pred_cardinality > true_cardinality:
            cexc.messages.warn('Predicted-field cardinality ({}) exceeds true-field cardinality ({}). Please ensure '
                               'correct fields have been passed.'.format(pred_cardinality, true_cardinality))
    assert_any_rows(clean_df)

    return clean_df


def assert_pos_label_in_field(df, field, pos_label, default_pos_label='1'):
    """ Assert that the pos_label parameter is present in df[field].

    Args:
        df (pd.Dataframe): input dataframe
        pos_label (str): pos_label value
        field (str): field to check for value
        default_pos_label (str): value to check for

    Raises:
        RuntimeError
    """
    if df[field][df[field].isin([pos_label])].empty:
        msg = ('Value error: {} for pos_label not found in ground-truth field "{}". '
               'Please specify a valid value for pos_label')

        if pos_label == default_pos_label:
            raise RuntimeError(msg.format('default value "{}"'.format(default_pos_label), field))
        else:
            raise RuntimeError(msg.format('value "{}"'.format(pos_label), field))


def assert_pred_values_are_scores(df, field):
    """ Assert predicted field is numeric.

    Used for roc_curve and roc_auc_score. Used to assert
    that the predicted fields are numeric (corresponding to
    probability estimates or confidence intervals, etc.)

    Args:
        df (pd.dataframe): input dataframe
        field (str): Name of field in question

    Raises:
        Runtime error
    """
    found_dtype = df[field].dtype
    if found_dtype.kind not in ['i', 'u', 'f', 'c']:
        msg = ('Expected field "{}" to be numeric and correspond to probability estimates or confidence intervals '
               'of the positive class, but field contains non-numeric events.')
        raise RuntimeError(msg.format(field))


def check_warn_ignore_pos_label(params, default_average='binary'):
    """ Outputs warning that pos_label will be ignored.

    When pos_label is explicitly set but average != binary,
    warn that pos_label param is ignored.

    Args:
        params (dict): input parameters
        default_average (str): default value for "average" param

    Warns:
        Warns that pos_label is ignored if applicable
    """
    average = params.get('average', default_average)
    if average != 'binary':
        if params.get('pos_label', None) is not None:
            # Pos_label has been explicitly set by the user, but will be ignored since average is not binary
            cexc.messages.warn('Warning: pos_label will be ignored since average is not binary (found average={})'.
                               format(average))


def check_multiclass(cardinality, params):
    """ Raise error if target is multiclass but average=binary.

    Args:
        cardinality (int): number of unique target classes
        params (dict): parameters

    Raises:
        RuntimeError
    """
    if cardinality > 2:
        if params['average'] == 'binary':
            raise RuntimeError("Value error: Target is multiclass but average=binary. "
                               "Please choose another average setting.")


def check_valid_average(params, _meta_params):
    """ Checks average parameter.

    Checks if passed average value is valid. If average=none,
    converts to nonetype and updated all_labels=True in _meta_params.

    Args:
        params (dict): parameters
        _meta_params (dict): parameters used in backend but not in sklearn

    Returns:
        params (dict): Updated parameters.
    """
    average = params['average']
    if average.lower() == 'none':  # allow for average to be set to None.
        average = None
        _meta_params['all_labels'] = True  # set all_labels=True
    if average not in [None, 'binary', 'macro', 'micro', 'weighted']:  # ensure average has a valid value
        err_msg = 'Value error: average={} is not supported'
        raise RuntimeError(err_msg.format(average))
    params['average'] = average
    return params, _meta_params


def check_pos_label_and_average(df, true_field, params, default_pos_label='1'):
    """ Checks validity of pos_label & average params against true_field.

    Assumes that pos_label is a key in params.

    Args:
        df (pd.dataframe): input dataframe
        true_field (str): name of ground-truth field
        params (dict): parameters dictionary
        default_pos_label (str): default value of pos_label

    Raises RuntimeError if params incompatible with data
    """
    cardinality = df[true_field].nunique()
    check_multiclass(cardinality, params)

    if params['average'] == 'binary':
        assert_pos_label_in_field(df, true_field, params['pos_label'], default_pos_label)


def add_default_params(params, default_params_dict):
    """" Updates params with default params.

    Args:
        params (dict): params dict
        default_params_dict (dict): default parameters to update params with

    Returns:
        params (dict): updated params dict with defaults
    """
    for k, v in default_params_dict.iteritems():
        params.setdefault(k, v)
    return params
