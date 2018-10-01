#!/usr/bin/env python

import argparse

def parse_roles(file_path):
    """
    Parse the --roles argument out of the provided file.

    Args:
        file_path (str): The full path to the file to parse
    Returns:
        roles (list): Either a list of string roles, or an empty list if the file doesn't contain a --roles argument
    """

    with open(file_path, mode='r') as f:
        file_text = [line.strip() for line in f]
        parser = argparse.ArgumentParser()
        parser.add_argument('--roles')
        args = parser.parse_known_args(file_text)

        try:
            roles = args[0].roles.split(':')
        except AttributeError:
            roles = []

    return roles
