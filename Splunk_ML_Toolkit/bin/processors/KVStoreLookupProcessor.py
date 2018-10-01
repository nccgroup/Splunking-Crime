#!/usr/bin/env python
# Copyright (C) 2015-2017 Splunk Inc. All Rights Reserved.
import cexc
import json
import pandas as pd
from BaseProcessor import BaseProcessor
from util import kvstore_util
from util.rest_proxy import rest_proxy_from_searchinfo
from util.rest_url_util import make_splunk_url

logger = cexc.get_logger(__name__)
messages = cexc.get_messages_logger()

class KVStoreLookupProcessor(BaseProcessor):
    """The KVStore processor loads combined shared and user-specific entries from a KVStore collection ."""

    @staticmethod
    def json_dumps_unless_string(value):
        """
        Dump a value to JSON, unless it's already a string

        Args:
            value (str): the value to dump
        """
        if not isinstance(value, unicode):
            return json.dumps(value)
        else:
            return value

    @staticmethod
    def multivalue_encode(value):
        """
        Encode a value as part of a multivalue field expected by the Chunked External Command Protocol

        Args:
            value (str): the value to encode
        """
        return '$%s$' % value.replace('$', '$$')

    @classmethod
    def parse_reply(cls, reply):
        """
        Parse the reply of a KVStore REST query

        Args:
            reply: the results of a KVStore REST query, as produced by rest_proxy.make_rest_call
        """
        try:
            content = reply.get('content')
            error_type = reply.get('error_type')
            json_content = json.loads(content)

            if reply['success']:
                for i, json_content_row in enumerate(json_content):
                    encoded_multivalues = {} # stores encoded multivalued fields, if needed
                    for key, value in json_content_row.iteritems():
                        if isinstance(value, list):
                            value = map(cls.json_dumps_unless_string, value)

                            # if the value is a list, it needs to be multivalue encoded
                            json_content[i][key] = '\n'.join(value)
                            encoded_multivalues['__mv_' + key] = ';'.join(map(cls.multivalue_encode, value))
                        else:
                            json_content[i][key] = cls.json_dumps_unless_string(value)
                    json_content[i].update(encoded_multivalues)

                return json_content
            else:
                # trying to load a nonexistent collection helpfully returns a 500 (and not a 404)
                # so there's not much point bothering with fancy error handling
                error_text = json_content.get('messages')[0].get('text')
                if error_type is None:
                    raise RuntimeError(error_text)
                else:
                    raise RuntimeError(error_type + ', ' + error_text)
        except Exception as e:
            logger.debug(e.message)
            logger.debug(e)
            raise e

    @classmethod
    def load_collection(cls, collection_name, searchinfo):
        """
        Create the output table of KVStore entries.

        Args:
            collection_name (str): the name of the KVStore collection to load
            searchinfo (dict): information required for search
        """

        rest_proxy = rest_proxy_from_searchinfo(searchinfo)
        kvstore_user_reply = kvstore_util.load_collection_from_rest('user', collection_name, rest_proxy)
        kvstore_shared_reply = kvstore_util.load_collection_from_rest('app', collection_name, rest_proxy)

        formatted = cls.parse_reply(kvstore_user_reply) + cls.parse_reply(kvstore_shared_reply)

        return pd.DataFrame(formatted)

    @classmethod
    def load_experiment_history(cls, experiment_id, searchinfo):
        rest_proxy = rest_proxy_from_searchinfo(searchinfo)
        url_parts = ['mltk', 'experiments', experiment_id, 'history']
        url = make_splunk_url(rest_proxy, 'user', extra_url_parts=url_parts)
        reply = rest_proxy.make_rest_call('GET', url)

        return pd.DataFrame(cls.parse_reply(reply))

    def process(self):
        """List the KVStore rows."""
        collection_name = self.process_options['collection_name']
        experiment_id = self.process_options['experiment_id']

        if collection_name is not None:
            self.df = self.load_collection(collection_name, self.searchinfo)
        elif experiment_id is not None:
            self.df = self.load_experiment_history(experiment_id, self.searchinfo)
