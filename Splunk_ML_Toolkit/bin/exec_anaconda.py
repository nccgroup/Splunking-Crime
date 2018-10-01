#!/usr/bin/env python
# Copyright (C) 2015-2016 Splunk Inc. All Rights Reserved.

import os
import platform
import stat
import sys
import subprocess
import time
import traceback

from util.base_util import get_apps_path


def exec_anaconda():
    """Re-execute the current Python script using the Anaconda Python
    interpreter included with Splunk_SA_Scientific_Python.

    After executing this function, you can safely import the Python
    libraries included in Splunk_SA_Scientific_Python (e.g. numpy).

    Canonical usage is to put the following at the *top* of your
    Python script (before any other imports):

       import exec_anaconda
       exec_anaconda.exec_anaconda()

       # Your other imports should now work.
       import numpy as np
       import pandas as pd
       ...
    """

    if 'Continuum' in sys.version:
        fix_sys_path()
        reload(os)
        reload(platform)
        reload(stat)
        reload(sys)
        reload(subprocess)
        return

    supported_systems = {
        ('Linux', 'i386'): 'linux_x86',
        ('Linux', 'x86_64'): 'linux_x86_64',
        ('Darwin', 'x86_64'): 'darwin_x86_64',
        ('Windows', 'AMD64'): 'windows_x86_64'
    }

    system = (platform.system(), platform.machine())
    if system not in supported_systems:
        raise Exception('Unsupported platform: %s %s' % (system))

    sa_scipy = 'Splunk_SA_Scientific_Python_%s' % (supported_systems[system])

    sa_path = os.path.join(get_apps_path(), sa_scipy)
    # sa_path = os.path.join(bundle_paths.get_base_path(), sa_scipy)
    if not os.path.isdir(sa_path):
        raise Exception('Failed to find Python for Scientific Computing Add-on (%s)' % sa_scipy)

    system_path = os.path.join(sa_path, 'bin', '%s' % (supported_systems[system]))

    if system[0] == 'Windows':
        python_path = os.path.join(system_path, 'python.exe')
        # MLA-564: Windows need the DLLs to be in the PATH
        dllpath = os.path.join(system_path, 'Library', 'bin')
        pathsep = os.pathsep if 'PATH' in os.environ else ''
        os.environ['PATH'] = os.environ.get('PATH', '') + pathsep + dllpath
    else:
        python_path = os.path.join(system_path, 'bin', 'python')

    # MLA-996: Unset PYTHONHOME
    os.environ.pop('PYTHONHOME', None)

    # Ensure that execute bit is set on <system_path>/bin/python
    if system[0] != 'Windows':
        mode = os.stat(python_path).st_mode
        os.chmod(python_path, mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    print >> sys.stderr, "INFO Running %s" % " ".join([python_path] + sys.argv)
    sys.stderr.flush()

    try:
        if system[0] == "Windows":
            os.environ['MKL_NUM_THREADS'] = '1'
            # os.exec* broken on Windows: http://bugs.python.org/issue19066
            subprocess.check_call([python_path] + sys.argv)
            os._exit(0)
        else:
            os.environ['VECLIB_MAXIMUM_THREADS'] = '1'
            os.environ['OPENBLAS_NUM_THREADS'] = '1'
            os.execl(python_path, python_path, *sys.argv)
    except Exception:
        traceback.print_exc(None, sys.stderr)
        sys.stderr.flush()
        time.sleep(.1)
        raise RuntimeError('Error encountered while loading Python for Scientific Computing, see search.log.')


def fix_sys_path():
    # Update sys.path to move Splunk's PYTHONPATH to the end. We want
    # to import Anaconda's built-ins before Splunk's.
    pp = os.environ.get('PYTHONPATH', None)
    if not pp:
        return
    for spp in pp.split(os.pathsep):
        try:
            sys.path.remove(spp)
            sys.path.append(spp)
        except Exception:
            pass

    # MLA-2136: update environment variable such that subprocesses
    # (from watchdog) will also have Anaconda's builtins available before
    # Splunk's builtins.
    if platform.system() == 'Windows':
        os.environ['PYTHONPATH'] = os.pathsep.join(sys.path)

def exec_anaconda_or_die():
    try:
        exec_anaconda()
    except Exception as e:
        import cexc
        cexc.abort(e)
        sys.exit(1)
