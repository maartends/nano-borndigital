#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  meemoo/helpers.py
#  
#  Copyleft 2020 meemoo
#  
#  @author: Maarten De Schrijver
#  

# System imports
import os
import logging
from io import BytesIO
from ftplib import FTP as BuiltinFTP
from urllib.parse import urlparse
# Third-party imports
from lxml import etree
# Local imports


# Get logger
log = logging.getLogger('nano-bd')

# Constants
BASE_DOMAIN = 'viaa.be'

def try_to_find_md5(object_metadata):
    """Simple convenience function that allows to be able to try different
    possible md5 field names (keys) and return it's value.

    Args:
        object_metadata (dict): The object metadata sub-section of the S3-event.

    Returns:
        str: The md5sum when found. An empty string ('') otherwise.
    """
    POSSIBLE_KEYS = ["x-md5sum-meta", "md5sum", "x-amz-meta-md5sum"]
    for key in POSSIBLE_KEYS:
        if key in object_metadata.keys():
            log.debug(f"md5sum located in metadata-field: '{key}'")
            return object_metadata[key]
    return ""

def get_from_event(event, name):
    """An opionated function which knows where certain fields should be located
    within the S3-event and can retrieve their values.
    While dict-keys are case-sensitive, keys for JSON objects should probably
    not be (hence `name.lower()`).

    Args:
        event (dict): The S3-event as a dict.
        name (str): The field-name that we want the value for.

    Returns:
        str: The fields value.
    """
    keys = ['bucket', 'object_key', 'host', 'tenant', 'md5']
    name = name.lower()
    assert name in keys, f'Unknown key: "{name}"'
    s3_sub = event['Records'][0]['s3']
    if name == 'bucket':
        return s3_sub['bucket']['name']
    elif name == 'object_key':
        return s3_sub['object']['key']
    elif name == 'host':
        host = '.'.join([
            s3_sub['bucket']['name'],
            s3_sub['domain']['name'],
            BASE_DOMAIN
        ])
        return host
    elif name =='tenant':
        return s3_sub['bucket']['metadata']['tenant']
    elif name == 'md5':
        return try_to_find_md5(s3_sub['object']['metadata'])


class SidecarBuilder(object):
    """SidecarBuilder constructs an XML sidecar compliant to the MediaHaven
    metadata model. The resulting XML (as a string) can obtained via a call to
    `to_string`.
    MediaHaven's documentation: https://mediahaven.atlassian.net/wiki/spaces/CS/pages/488964146/Metadata+Sidecar
    """
    ALLOWED_NODES = ['Dynamic', 'Technical']
    XML_ENCODING  = 'UTF-8'
    MHS_VERSION   = '19.4'
    MH_NAMESPACES = {
        "mhs": f"https://zeticon.mediahaven.com/metadata/{MHS_VERSION}/mhs/",
        "mh":  f"https://zeticon.mediahaven.com/metadata/{MHS_VERSION}/mh/"
    }
    #
    def __init__(self, ctx=None):
        self.sidecar    = None
        self.ctx        = ctx
    #
    def check_metadata_dict(self, metadata_dict) -> bool:
        # TODO: type annotation?
        """"""
        for k in metadata_dict:
            assert k in self.ALLOWED_NODES, f'Unknown sidecar node: "{k}"'
    #
    def build(self, metadata_dict) -> None:
        """"""
        self.check_metadata_dict(metadata_dict)
        # Create the root element: Sidecar
        root = etree.Element("{%s}%s" % (self.MH_NAMESPACES['mhs'], 'Sidecar'),
                             version=self.MHS_VERSION,
                             nsmap=self.MH_NAMESPACES)
        # Make a new document tree
        doc = etree.ElementTree(root)   # NEEDED?
        # Add the subelements
        for top in metadata_dict:
            # Can't we use f-strings? With curly braces?
            node = etree.SubElement(root, "{%s}%s" % (self.MH_NAMESPACES['mhs'], top))
            # TODO: the subnodes under 'Dynamic' should not be namespaced!
            for sub, val in metadata_dict[top].items():
                if top == 'Technical':
                    etree.SubElement(node, "{%s}%s" % (self.MH_NAMESPACES['mh'], sub)).text = val
                if top == 'Dynamic':
                    etree.SubElement(node, "%s" % sub).text = val

        self.sidecar = doc
    #
    def to_bytes(self, pretty=False) -> bytes:
        return etree.tostring(self.sidecar, pretty_print=pretty,
                           encoding=self.XML_ENCODING,
                           xml_declaration=True)
    #
    def to_string(self, pretty=False) -> str:
        return etree.tostring(self.sidecar, pretty_print=pretty,
                           encoding=self.XML_ENCODING,
                           xml_declaration=True).decode('utf-8')

class FTP(object):
    """Abstraction for FTP"""
    def __init__(self, host, ctx=None):
        self.ctx        = ctx
        self.host       = self.__set_host(host)
        self.conn       = self.__connect()
    #
    def __set_host(self, host):
        """"""
        parts = urlparse(host)
        log.debug(f'FTP: scheme={parts.scheme}, host={parts.netloc}')
        return parts.netloc

    def __connect(self):
        ftp_user = self.ctx.config['mediahaven']['ftp']['user']
        ftp_passwd = self.ctx.config['mediahaven']['ftp']['passwd']
        try:
            conn = BuiltinFTP(host=self.host, user=ftp_user, passwd=ftp_passwd)
        except Exception as e:
            log.error(e)
            raise
        else:
            log.debug(f'Succesfully established connection to {self.host}')
            return conn

    def put(self, content_bytes, destination_path, destination_filename):
        log.debug(f'Putting {destination_filename} to {destination_path} on {self.host}')
        with self.conn as conn:
            conn.cwd(destination_path)
            stor_cmd = f'STOR {destination_filename}'
            conn.storbinary(stor_cmd, BytesIO(content_bytes))



# vim modeline
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
