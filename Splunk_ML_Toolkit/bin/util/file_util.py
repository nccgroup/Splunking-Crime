import os
import sys


# Windows will mangle our line-endings unless we do this.
def fix_line_ending():
    if sys.platform == "win32":
        import os
        import msvcrt  # pylint: disable=import-error

        msvcrt.setmode(sys.stdout.fileno(),
                       os.O_BINARY)  # pylint: disable=E1103 ; the Windows version of os has O_BINARY
        msvcrt.setmode(sys.stderr.fileno(),
                       os.O_BINARY)  # pylint: disable=E1103 ; the Windows version of os has O_BINARY
        msvcrt.setmode(sys.stdin.fileno(),
                       os.O_BINARY)  # pylint: disable=E1103 ; the Windows version of os has O_BINARY


def file_exists(file_path):
    return os.path.isfile(file_path)
