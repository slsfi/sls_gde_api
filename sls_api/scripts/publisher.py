import argparse
import logging
from lxml import etree
import os
from shutil import copy2 as copy_file
from sqlalchemy import Table
from sqlalchemy.sql import select

from sls_api.endpoints.generics import config, db_engine, metadata


def publish_publication(project, master_file_path, comment_file_path):
    """
    Given a project name, and paths to valid master and comment files from that project's root folder,
    Generates new est and com reading XML files and copies them to the correct folders for online viewing
    """
    file_root = config[project]["file_root"]
    est_root = os.path.join(file_root, "est")
    com_root = os.path.join(file_root, "com")
    # TODO actual masterfile processing to webfiles using XSLT
    # TODO com file generation with both masterfiles and comments database


def publish_publication_variant(project, master_file_path):
    """
    Given a project name and a path to a valid variant master file from that project's root folder,
    Generates a new variant reading XML file and copies it to the correct folder for online viewing
    """
    file_root = config[project]["file_root"]
    var_root = os.path.join(file_root, "var")
    # TODO actual masterfile processing to webfile using XSLT


def publish_publication_manuscript(project, master_file_path):
    """
    Given a project name and a path to a valid manuscript master file from that project's root folder,
    Generates a new manuscript reading XML file and copies it to the correct folder for online viewing
    """
    file_root = config[project]["file_root"]
    ms_root = os.path.join(file_root, "ms")
    # TODO actual masterfile processing to webfile using XSLT

