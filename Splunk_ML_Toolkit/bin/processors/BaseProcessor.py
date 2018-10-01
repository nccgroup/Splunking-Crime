#!/usr/bin/env python
# Copyright (C) 2015-2017 Splunk Inc. All Rights Reserved.

import pandas as pd

import cexc
from util.base_util import MLSPLNotImplementedError

logger = cexc.get_logger(__name__)
messages = cexc.get_messages_logger()


class BaseProcessor(object):
    """Skeleton for all processors, also implements some utility methods."""

    def __init__(self, process_options=None, searchinfo=None):
        """Pass process_options.

        Args:
            process_options (dict): process options
            searchinfo (dict): information required for search
        """
        self.process_options = process_options
        self.namespace = process_options.pop('namespace', None)
        self.searchinfo = searchinfo

    def receive_input(self, df):
        """Get dataframe.

        Args:
            df (dataframe): input dataframe
        """
        self.df = df

    def process(self):
        """Necessary process method."""
        raise MLSPLNotImplementedError

    def get_output(self):
        """Simply return the output dataframe.

        Returns:
            (dataframe): output dataframe
        """
        if self.df is None:
            self.df = pd.DataFrame()
        return self.df
