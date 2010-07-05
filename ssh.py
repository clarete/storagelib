# -*- Coding: utf-8; Mode: Python -*-
#
# ssh.py - A plugin to storagelib be able to store files through ssh
#
# Copyright (C) 2010  Lincoln de Sousa <lincoln@comum.org>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""SSH Storage

This module holds the implementation of a ssh storage that uses the
amazing paramiko library.
"""

import os
import paramiko
from storagelib import BaseStorage, Attr

class Storage(BaseStorage):
    """A ssh storage

    This is very closer to the other storages the only difference is
    that it uses the ssh protocol to copy files.
    """
    type_ = 'ssh'

    host = Attr()
    port = Attr(22)
    user = Attr()
    password = Attr()

    def __init__(self):
        super(Storage, self).__init__()
        # This var will hold a paramiko.SFTPClient instance
        self.client = None

    def setup(self):
        """Sets up everything needed to store a file through ssh
        """
        try:
            transport = paramiko.Transport((self.host, int(self.port)))
            transport.connect()
            transport.auth_password(self.user, self.password)
        except paramiko.SSHException:
            return False

        if transport.authenticated:
            self.client = transport.open_sftp_client()
            return True
        else:
            return False

    def store(self, finst):
        """Stores the file using the paramiko.SFTPClient object
        """
        name = self.get_name(finst)
        self.client.open(name, 'w').write(finst.read())

        # Closing ssh connection
        self.client.close()

        # Time to say to the user where's the uploaded file
        new_name = os.path.basename(name)
        if not self.base_uri.endswith('/'):
            self.base_uri += '/'
        return self.base_uri + new_name

