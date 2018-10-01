#!/usr/bin/env python

import json

import pandas as pd

from util.param_util import convert_params, is_truthy
import base


def tree_summary(algo, options=None):
    """Create summary for tree based models.

    Args:
        algo (object): an algo object
        options (dict): options

    Returns:
        (dataframe): dataframe representation of the tree summary
    """
    if options:
        out_params = convert_params(
            options.get('params', {}),
            ints=['limit'],
            bools=['json'],
        )
        if 'json' in out_params:
            return_json = out_params['json']
        if 'limit' in out_params:
            depth_limit = out_params['limit']

    mlspl_limits = options.get('mlspl_limits', {})
    if 'return_json' not in locals():
        return_json = is_truthy(mlspl_limits.get('summary_return_json', 'f'))
    if 'depth_limit' not in locals():
        depth_limit = int(mlspl_limits.get('summary_depth_limit', 5))

    if depth_limit <= 0:
        raise ValueError('Limit = %d. Value for limit should be greater than 0.' % depth_limit)

    root = 0
    depth = 0
    if return_json:
        output_data = [json.dumps(tree_summary_dict(algo, depth_limit, root, depth), sort_keys=True)]
    else:
        output_data = tree_summary_str(algo, depth_limit, root, depth)
    return pd.DataFrame({'Decision Tree Summary': output_data})


def tree_summary_str(algo, depth_limit, root, depth):
    """Recursively go down a tree/subtree and render splits as strings.

    Args:
        algo (object): algo object
        depth_limit (int): depth limit of a tree for summary representation
        root (int): tree id
        depth (int): depth

    Returns:
        output (list): tree splits
    """
    t = algo.estimator.tree_
    features = algo.columns

    left_child = t.children_left[root]
    right_child = t.children_right[root]

    n_nodes = t.n_node_samples[root]
    impurity = t.impurity[root]

    if isinstance(algo, base.ClassifierMixin):
        classes = algo.estimator.classes_
        value = t.value[root][0]
        class_value = classes[value.argmax()]
        value_str = "class:%s  " % class_value
    else:
        value_str = "value:%.3f  " % t.value[root][0][0]

    indent = '----' * depth + ' '

    if left_child > 0 or right_child > 0:
        feature = features[t.feature[root]]
        if feature in algo.feature_variables:
            feature_val = t.threshold[root]
            split_str = "split:%s<=%.3f" % (feature, feature_val)
        else:
            split_str = "split:%s" % feature
    else:
        split_str = "split:N/A - Leaf node"
    output_str = "|--" + indent + "count:%d  %s  %simpurity:%.3f" % (
        n_nodes, split_str, value_str, impurity)
    output = [output_str]

    if depth_limit >= 1:
        depth += 1
        depth_limit -= 1
        if left_child > 0:
            output.extend(tree_summary_str(algo, depth_limit, left_child, depth))
        if right_child > 0:
            output.extend(tree_summary_str(algo, depth_limit, right_child, depth))
    return output


def tree_summary_dict(algo, depth_limit, root, depth):
    """Recursively go down a tree/subtree and render splits as dictionaries.

    Args:
        algo (object): algo object
        depth_limit (int): depth limit of a tree
        root (int): root of the tree/subtree
        depth (int): depth of the tree/subtree

    Return:
        output (dict): tree splits
    """
    t = algo.estimator.tree_
    features = algo.columns

    left_child = t.children_left[root]
    right_child = t.children_right[root]

    output = {}
    output["count"] = int(t.n_node_samples[root])

    if isinstance(algo, base.ClassifierMixin):
        classes = algo.estimator.classes_
        value = t.value[root][0]
        output["class"] = classes[value.argmax()]
    else:
        output["value"] = round(t.value[root][0][0], 3)

    if left_child > 0 or right_child > 0:
        feature = features[t.feature[root]]
        if feature in algo.feature_variables:
            feature_val = t.threshold[root]
            output["split"] = "%s<=%.3f" % (feature, feature_val)
        else:
            output["split"] = "split:%s" % feature
    else:
        output["split"] = "split:N/A - Leaf node"

    output["impurity"] = round(t.impurity[root], 3)

    if depth_limit >= 1:
        depth += 1
        depth_limit -= 1
        if left_child > 0:
            output["left child"] = tree_summary_dict(algo, depth_limit, left_child, depth)
        if right_child > 0:
            output["right child"] = tree_summary_dict(algo, depth_limit, right_child, depth)
    return output


def assert_estimator_supports_partial_fit(estimator):
    """Assert the estimator has a partial_fit method, otherwise raise error.

    Args:
        estimator (object): a scikit-learn estimator

    Raises:
        RuntimeError
    """
    if not hasattr(estimator, 'partial_fit'):
        text = 'Algorithm {} does not support partial fit'
        msg = text.format(estimator.__class__.__name__)
        raise RuntimeError(msg)


def confidence_interval_to_alpha(x):
    """ Transform confidence interval to alpha. """
    if x >= 100 or x <= 0:
        raise RuntimeError(
            'conf_interval cannot be less than 0 or more than 100.'
        )
    return 1 - x / 100.0


def alpha_to_confidence_interval(x):
    """ Transform alpha to confidence interval."""
    return int(round((1 - x) * 100))


def handle_max_features(max_features):
    """Deal with the multiple types of max_features and error accordingly

    Args:
        max_features (string): the value of the max_features paramter
    Returns:
        max_features: it could be a float, an int, a string, or None
    """
    if max_features.lower() == "none":
        max_features = None
    else:
        # EAFP... convert max_features to int if it is a number.
        try:
            max_features = float(max_features)
            max_features_int = int(max_features)
            if max_features == max_features_int:
                max_features = max_features_int
        except:
            pass
    return max_features


def add_missing_attr(estimator, attr, value, param_key=None):
    """Set attributes on the estimator.

    Between versions of scikit-learn, estimators may be missing certain attributes.
    Sometimes those attributes are simply renamed, other times they do not already
    exist. This method is just a utility to set those on the estimator.

    Args:
        estimator (obj): the estimator object
        attr (str): the attribute to add
        value (str, float, int, or str): the value to set if param_key is not used
        param_key (str): the name of an existing param (from get_params) to use. If
            the value is not set, it will default to the value arg.
    """
    if param_key is not None:
        params = estimator.get_params()
        new_value = params.get(param_key, value)
    else:
        new_value = value

    if not hasattr(estimator, attr):
        setattr(estimator, attr, new_value)
