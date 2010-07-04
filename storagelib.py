# -*- Coding: utf-8; Mode: Python -*-
#
# videocdbrd - Executable that launches the video platform
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

"""A simple storage lib

This library aims to provide a simple interface that receives a file
(or its data), store it and then return the URI to reach the stored
resource.

It is written to be simple but extensible. It should be easy to add a
new type of storage just by writting a simple plugin.

Another thing we had as goal is to provide a way for the system
administrator to manager more than one repos with priorities and
weights.
"""

import sys
from random import randint
from ConfigParser import ConfigParser

_STORAGES = {}

class Attr(object):
    """A helper class to mark attributes of plugins.

    Instances of this class should be used to mark attributes that
    will be read from the general config file.
    """
    def __init__(self, default=None):
        self.default = default

class StorageMeta(type):
    """Storage metaclass

    This metaclass has two goals:
      * Register new types of storages.
      * List attributes of new types of storages to be able to use the
        main config file to store them.
    """
    def __new__(mcs, name, bases, attrs):
        klass = type.__new__(mcs, name, bases, attrs)
        _STORAGES[klass.type_] = klass

        klass.extra_attrs = []

        if name != 'BaseStorage':
            for key, val in attrs.items():
                if isinstance(val, Attr):
                    klass.extra_attrs.append(key)
                    setattr(klass, key, getattr(klass, key).default)
        return klass

class BaseStorage(object):
    """A storage representation.

    This class holds all basic (and required) attributes that a
    storage must have.
    """
    __metaclass__ = StorageMeta
    type_ = 'local'

    name = None
    dest = None
    base_uri = None
    name_policy = None
    structure = None
    priority = 0
    weight = 0

class SshStorage(BaseStorage):
    """A ssh storage

    This is very closer to the other storages the only difference is
    that it uses the ssh protocol to copy files.
    """
    type_ = 'ssh'

    host = Attr()
    user = Attr()

def cmp_storages(repo1, repo2):
    """A function used to do the first sort at the repos list putting
    all repositories with the lower priority first.
    """
    if repo1.priority == repo2.priority:
        return int(repo1.weight) - int(repo2.weight)
    else:
        return int(repo1.priority) - int(repo2.priority)

class StorageContext(object):
    """Context to manage storages

    This class looks for storages defined in a config file, sort them
    using the same algorithm for sorting SRV records defined in the
    RFC 2782.
    """
    def __init__(self, cfg):
        self.repo_list = []
        self.parse_cfg(cfg)
        self.sort_repos()

    def parse_cfg(self, cfg_file):
        """Parses the config file looking for repositories
        """
        cfg = ConfigParser()
        cfg.read([cfg_file])
        for i in cfg.sections():
            storage = _STORAGES[cfg.get(i, 'type')]()

            # reading attrs defined in BaseStorage
            storage.name = i
            storage.dest = cfg.get(i, 'dest')
            storage.base_uri = cfg.get(i, 'base_uri')
            storage.name_policy = cfg.get(i, 'name_policy')
            storage.structure = cfg.get(i, 'structure')
            if cfg.has_option(i, 'priority'):
                storage.priority = cfg.getint(i, 'priority')
            if cfg.has_option(i, 'weight'):
                storage.weight = cfg.getint(i, 'weight')

            # reading extra attrs, defined by each storage, like ssh
            for extra_attr in storage.extra_attrs:
                if cfg.has_option(i, extra_attr):
                    setattr(storage, extra_attr, cfg.get(i, extra_attr))
            self.repo_list.append(storage)

    def sort_repos(self):
        """Sorts repositories in order of precedence

        This sorts repositories using their `priority' and `weight'
        fields, just like RFC 2782 spec suggests to SRV targets.
        """
        self.repo_list.sort(cmp_storages)

        # let's copy the sorted list above
        unordered = self.repo_list[:]
        ordered = []

        # Now, let's find all repositories with the same priority and
        # use the weight field to order them.
        while unordered:
            priority = unordered[0].priority
            current_priority = []
            for repo in unordered:
                # Let's add the current repos to the current_priority
                # list. This will give us their count and will make it
                # easy to sum all their weights
                if repo.priority == priority:
                    current_priority.append(repo)

            # Now, that we have the list of repositories that are in
            # the priority we're dealing with, let's randomly select
            # their order givine precedence to the ones with higher
            # weight.
            sum_ = sum(x.weight for x in current_priority)
            count = len(current_priority)
            while count:
                rand_num = randint(0, sum_+1)
                for repo in unordered:
                    if repo.weight >= rand_num:
                        break
                    rand_num -= repo.weight
                ordered.append(repo)
                unordered.remove(repo)
                sum_ -= repo.weight
                count -= 1

if __name__ == '__main__':
    StorageContext(sys.argv[1])
