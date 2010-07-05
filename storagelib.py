# -*- Coding: utf-8; Mode: Python -*-
#
# storagelib.py - A simple and extensible storage library
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
import os
from datetime import datetime
from random import randint, choice
from ConfigParser import ConfigParser

_CHARS = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'

_STORAGES = {}

_NAME_POLICIES = {}

def register_storage_type(klass):
    """Register a storage class in the _STORAGES dictionary

    It also fills the `extra_attrs' attribute of the given klass to
    make it possible to load these parameters from the config file.
    """
    if not hasattr(klass, 'extra_attrs'):
        klass.extra_attrs = []
    for key in dir(klass):
        val = getattr(klass, key)
        if isinstance(val, Attr):
            klass.extra_attrs.append(key)
            setattr(klass, key, getattr(klass, key).default)
    _STORAGES[klass.type_] = klass

def np_random(path):
    """Creates a random name for a file being stored
    """
    npath = os.path.join(
        os.path.dirname(path),
        ''.join(choice(_CHARS) for x in range(10)))
    while os.path.exists(npath):
        npath = np_random(npath)
    return npath
_NAME_POLICIES['random'] = np_random

def np_preserve(path):
    """Tries to preserve the name of a file but when it already
    exists, we add the date
    """
    npath = path
    while os.path.exists(npath):
        npath = path + '.'
        npath += datetime.now().strftime('%Y%m%d-%H%M%S')
    return npath
_NAME_POLICIES['preserve'] = np_preserve

def np_preserve_ext(path):
    """Generates a random name but preserves the extension of the
    given file.
    """
    ext = os.path.splitext(path)[1]
    npath = os.path.join(
        os.path.dirname(path),
        ''.join(choice(_CHARS) for x in range(10)))
    npath += ext
    while os.path.exists(npath):
        npath = np_preserve_ext(npath)
    return npath
_NAME_POLICIES['preserve_ext'] = np_preserve_ext

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
        register_storage_type(klass)
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

    def get_name(self, finst):
        """Gets a name for the file being sored.

        The name is not actually created/choosen by this method. It
        only calls the proper name policy giving the original name as
        argument.
        """
        fname = getattr(finst, 'filename', finst.name)
        fname = os.path.basename(fname)
        fpath = os.path.join(self.dest, fname)
        npolicy = _NAME_POLICIES[self.name_policy]
        return npolicy(fpath)

    def setup(self):
        """Tries to setup everything needed to ensure that this
        storage is working. Returns True if everything is ok and False
        otherwise.
        """
        if not os.access(self.dest, os.W_OK):
            return False
        return True

    def store(self, finst):
        """Actually stores the file.
        """
        name = self.get_name(finst)
        open(name, 'w').write(finst.read())

        # Time to say to the user where's the uploaded file
        new_name = os.path.basename(name)
        if not self.base_uri.endswith('/'):
            self.base_uri += '/'
        return self.base_uri + new_name

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

        # Reading the Default section looking for the plugins entry
        # and loading all of them.
        if cfg.has_section('Default') and \
                cfg.has_option('Default', 'plugins'):
            plugins = cfg.get('Default', 'plugins').split(',')
            for i in plugins:
                module = __import__(i.strip(), globals(), fromlist='Storage')
                register_storage_type(module.Storage)

        for i in cfg.sections():
            # We can't handle the Default secion as a storage
            if i == 'Default':
                continue

            # The storage instance
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

    def store(self, finst):
        """Sort all storages again, look for the first working one and
        then stores the file.
        """
        self.sort_repos()
        for storage in self.repo_list:
            if not storage.setup():
                continue
            return storage.store(finst)

def store(finst):
    """Instantiates the `StorageContext' class and then calls its
    store method.

    The configuration file passed to StorageContext's constructor is
    the one found in the STORAGELIB_CONFIG_FILE environment var. If it
    is not set, an error is raised.
    """
    cfg = os.environ.get('STORAGELIB_CONFIG_FILE')
    if not cfg:
        raise Exception('STORAGELIB_CONFIG_FILE environment var not set')
    ctx = StorageContext(cfg)
    return ctx.store(finst)

def test():
    """Call the API with fake params to test
    """
    ctx = StorageContext(sys.argv[1])
    print ctx.store(file('/etc/resolv.conf'))

if __name__ == '__main__':
    test()
