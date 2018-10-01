#!/usr/bin/env python

import json

import codecs_manager


class MLSPLEncoder(json.JSONEncoder):
    def default(self, obj): # pylint: disable=E0202 ; pylint doesn't like overriding default for some reason
        codec = codecs_manager.get_codec_table().get((type(obj).__module__, type(obj).__name__), None)
        if codec is not None:
            return codec.encode(obj)

        try:
            return json.JSONEncoder.default(self, obj)
        except:
            raise TypeError("Not JSON serializable: %s.%s" % (type(obj).__module__, type(obj).__name__))


class MLSPLDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        super(MLSPLDecoder, self).__init__(*args, object_hook=self._object_hook, **kwargs)

    def _object_hook(self, obj):
        if isinstance(obj, dict) and '__mlspl_type' in obj:
            module_name, name = obj['__mlspl_type']
            codec = codecs_manager.get_codec_table().get((module_name, name), None)
            if codec:
                return codec.decode(obj)
            raise ValueError('No codec for record of type "%s.%s"' % (module_name, name))
        return obj
