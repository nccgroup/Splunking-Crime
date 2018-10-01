#!/usr/bin/env python

import os
import subprocess
import sys
import collections
import re


def getAppConf(confName, app_path=None):
    # using btool is more "correct" if things change in the future etc, but it's
    # super duper slow.
    # see the history of this file for a skeleton implementation of btool
    if app_path:
        default_path = os.path.join(app_path, "default", confName + ".conf")
        local_path = os.path.join(app_path, "local", confName + ".conf")
        combined_settings = {}
        for path in (default_path, local_path):
            if os.path.exists(path):
                stanzas = readConfFile(path)
                _multi_update(combined_settings, stanzas)
        return combined_settings
    else:
        raise NotImplementedError


def _multi_update(target, source):
    "Recursively updates multi-level dict target from multi-level dict source"
    for k, v in source.iteritems():
        if isinstance(v, collections.Mapping):
            returned_dict = _multi_update(target.get(k, {}), v)
            target[k] = returned_dict
        else:
            target[k] = source[k]
    return target


CONF_FILE_COMMENT_LINE_REGEX = re.compile(r"^\s*[#;]")


def readConfFile(path, ordered=False):
    """reads Sorkins .conf files into a dictionary of dictionaries

    N.B.:  To aid in ease-of-use with writeConfFile(), the implementation
           retains any stanza names, keys, or values that have been escaped
           in their escaped form.
    """
    if not len(path) > 0:
        return None

    settings = collections.OrderedDict() if ordered else dict()
    currStanza = None

    if not os.path.exists(path):
        # TODO audit consumers, then remove this file creation entirely, it's
        # deeply wrong.
        confdir = os.path.dirname(path)
        if not os.path.exists(confdir):
            os.makedirs(confdir)
        f = open(path, 'w')
    else:
        f = open(path, 'rb')
        lines = bom_aware_readlines(f, CONF_FILE_COMMENT_LINE_REGEX)
        settings = readConfLines(lines, ordered)

    f.close()
    return settings


def bom_aware_readlines(fileobj, do_not_fold_pattern=None):
    """Reads all lines from fileobj and returns them as a list.

    N.B.:  This function implicitly folds lines that end in a backslash with the
           line following, recursively, as long as the line does not match
           the regex do_not_fold_pattern.
    """
    lines = []
    while True:
        l = bom_aware_readline(fileobj, do_not_fold_pattern)
        if l:
            lines.append(l)
        else:
            break
    return lines


def bom_aware_readline(fileobj, do_not_fold_pattern=None):
    """Reads the next line from fileobj.

    N.B.:  This function implicitly folds lines that end in a backslash with the
           line following, recursively, as long as the line does not match
           the regex do_not_fold_pattern.
    """
    atstart = (fileobj.tell() == 0)
    line = ""
    while True:
        l = fileobj.readline()
        if atstart:
            if len(l) > 2 and ord(l[0]) == 239 and ord(l[1]) == 187 and ord(l[2]) == 191:
                # UTF-8 BOM detected: skip it over
                l = l[3:]
            atstart = False

        def fold_with_next_line(current_line):
            return ((not do_not_fold_pattern
                     or not do_not_fold_pattern.match(current_line))
                    and current_line.rstrip("\r\n").endswith("\\"))

        # if line should be folded, append \n, then to the top of the loop to
        # append the next line.
        if fold_with_next_line(l):
            # We purposefully retain the escaping backslash as then the result
            # can simply be rewritten out without needing to care about having
            # to reinstate any escaping.
            line += l.rstrip("\r\n")
            line += "\n"
        else:
            line += l
            break
    return line


def readConfLines(lines, ordered=False):
    """
    takes a list of lines in conf file format, and splits them into dictionary
    (of stanzas), each of which is a dictionary of key values.
    the passed list of strings can come either from the simple file open foo in
    readConfFile, or the snazzier output of popen("btool foo list")

    N.B.:  To aid in ease-of-use with writeConfFile(), the implementation
           retains any stanza names, keys, or values that have been escaped
           in their escaped form.
    """
    dict_type = collections.OrderedDict if ordered else dict
    currStanza = "default"
    settings = dict_type({currStanza: dict_type()})

    # line is of the form key = value where multi-line value is combined by '\n'
    for line in lines:
        l = line.strip()
        if l.startswith("#"): continue
        if l.startswith('['):
            stanza = l.lstrip('[')
            endLoc = stanza.rfind(']')
            if endLoc >= 0:
                stanza = stanza[:endLoc]
            if stanza not in settings:
                settings[stanza] = dict_type()
            currStanza = stanza
        else:
            # Key names may include embedded '=' chars as long as they are
            # escaped appropriately.
            equalsPos = l.find('=')
            while equalsPos != -1:
                backslashPos = equalsPos - 1
                backslashCount = 0
                # Iterate backwards from this '=' for as long as there are
                # backslashes.  If there are an odd number, then this '=' char
                # is considered escaped.
                while backslashPos > -1 and l[backslashPos] == '\\':
                    backslashPos -= 1
                    backslashCount += 1
                if backslashCount % 2 == 0:
                    break
                equalsPos = l.find('=', equalsPos + 1)
            # We ignore lines that contain no unescaped '=' chars.
            if equalsPos != -1:
                key = l[:equalsPos].strip()
                val = l[equalsPos + 1:].strip()
                if val and val[-1] == "\\":
                    # This could be a multi-line value and strip will get rid \n
                    # adding back \n to avoid conflating of the 2 settings:
                    # SPL-91600
                    val = "%s\n" % val
                settings[currStanza][key] = val
    return settings
