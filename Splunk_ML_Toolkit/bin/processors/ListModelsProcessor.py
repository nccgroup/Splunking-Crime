#!/usr/bin/env python
# Copyright (C) 2015-2017 Splunk Inc. All Rights Reserved.
import json
import cexc
import models.listmodels as listmodels
import pandas as pd
from BaseProcessor import BaseProcessor

logger = cexc.get_logger(__name__)
messages = cexc.get_messages_logger()


class ListModelsProcessor(BaseProcessor):
    """The list models processor lists the saved ML-SPL models."""

    @classmethod
    def list_models(cls, searchinfo):
        """
        Create the output table of models and options.

        Args:
            searchinfo (dict): information required for search
        """
        # the tuple (count, -1) is provided to get an unlimited number of models
        model_response = listmodels.list_models(searchinfo, [('count', '-1')])
        list_of_models = map(cls.get_model_as_dictionary, model_response['entry'])
        return pd.DataFrame(list_of_models)

    @staticmethod
    def get_model_as_dictionary(model):
        acl = model['acl']
        model_info = model['content']['mlspl:model_info']

        return {
            'name': model_info['model_name'],
            'type': model_info['algo_name'],
            'options': json.dumps(model_info),
            'owner': acl['owner'],
            'app': acl['app'],
            'sharing': acl['sharing']
        }

    def process(self):
        """List the saved models."""
        self.df = self.list_models(self.searchinfo)
