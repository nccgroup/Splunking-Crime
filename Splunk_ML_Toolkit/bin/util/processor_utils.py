#!/usr/bin/env python
# Copyright (C) 2015-2018 Splunk Inc. All Rights Reserved.
import cexc
from sampler import ReservoirSampler
from util.param_util import is_truthy, convert_params

logger = cexc.get_logger(__name__)
messages = cexc.get_messages_logger()


def split_options(options, mlspl_conf, stanza_name):
    """ Split options into class and processor options.

    In general, "class" may refer to algorithms or scoring
    methods. Pop tmp_dir from the options. Also, parse sample count
    and sample seed from original params and add them to process options.

    Args:
        options (dict): process options
        mlspl_conf (obj): the conf utility for mlspl conf settings
        stanza_name (str): class stanza name in mlspl.conf

    Returns:
        process_options (dict): the process options we use here
        class_options (dict): the class options to be passed to the scorer
    """
    sample_params = {}
    if 'params' in options:
        try:
            sample_params = convert_params(options['params'],
                                           ignore_extra=True,
                                           ints=['sample_count',
                                                 'sample_seed'])

            if 'sample_count' in sample_params:
                del options['params']['sample_count']

            if 'sample_seed' in sample_params:
                del options['params']['sample_seed']

        except ValueError as e:
            raise RuntimeError(str(e))

    # copy everything from leftover options to class options
    class_options = options.copy()
    class_options['mlspl_limits'] = mlspl_conf.get_stanza(stanza_name)

    # brand new process options
    process_options = {
        # sample options are added to the process options
        'sample_seed': sample_params.get('sample_seed', None),
        'sample_count': sample_params.get('sample_count', None),
        # needed by processor, not class
        'tmp_dir': class_options.pop('tmp_dir')
    }

    return process_options, class_options


def load_sampler_limits(process_options, stanza_name, mlspl_conf):
    """Read sampling limits from conf file and decide sample count.

    Args:
        process_options (dict): process options
        stanza_name (str): algo/scorer stanza name in mlspl.conf
        mlspl_conf (obj): the conf utility for mlspl conf settings

    Returns:
        sampler_limits (dict): sampler limits
    """
    max_inputs = int(mlspl_conf.get_mlspl_prop('max_inputs', stanza_name, -1))

    sampler_limits = {
        'use_sampling': is_truthy(str(mlspl_conf.get_mlspl_prop('use_sampling', stanza_name, 'yes'))),
        'sample_seed': process_options['sample_seed']  # simply set sample seed
    }

    # setting up the logic to choose the sample count
    if process_options['sample_count']:
        sampler_limits['sample_count'] = min(process_options['sample_count'], max_inputs)
    else:
        sampler_limits['sample_count'] = max_inputs

    return sampler_limits


def load_resource_limits(stanza_name, mlspl_conf):
    """Load class-specific resource limits.

    Load resources limits for scoring and algo methods.

    Args:
        stanza_name (str): name opf algo/scorer stanza in mlspl.conf
        mlspl_conf (obj): the conf utility for mlspl conf settings

    Returns:
        resource_limits (dict): dictionary of resource limits including
        max_fit_time (or max_score_time), max_memory_usage_mb, and max_model_size_mb
    """
    # Can return scoring or algorithm properties
    runtime_key = 'max_score_time' if 'score:' in stanza_name else 'max_fit_time'

    resource_limits = {
        'max_memory_usage_mb': int(mlspl_conf.get_mlspl_prop('max_memory_usage_mb', stanza_name, -1)),
        runtime_key:  int(mlspl_conf.get_mlspl_prop(runtime_key, stanza_name, -1)),
        'max_model_size_mb': int(mlspl_conf.get_mlspl_prop('max_model_size_mb', stanza_name, -1))}
    return resource_limits


def get_sampler(sampler_limits):
    """Initialize the sampler and use resource limits from processor.

    Args:
        sampler_limits (dict): sampler limits

    Returns:
        (object): sampler object
    """
    return ReservoirSampler(sampler_limits['sample_count'], random_state=sampler_limits['sample_seed'])


def check_sampler(sampler_limits, class_name):
    """Inform user if sampling is on. Raise error if sampling is off and
    events exceed limit.

    Args:
        sampler_limits (dict): sampler limits
        class_name (str): name of algo/scorer class
    """
    if is_truthy(sampler_limits['use_sampling']):
        messages.warn(
            'Input event count exceeds max_inputs for %s (%d), model will be fit on a sample of events.' % (
                class_name, sampler_limits['sample_count']))
    else:
        raise RuntimeError('Input event count exceeds max_inputs for %s (%d) and sampling is disabled.' % (
            class_name, sampler_limits['sample_count']))
