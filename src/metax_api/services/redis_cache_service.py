# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging
from json import dump as dump_json, load as load_json
from pickle import dumps as pickle_dumps, loads as pickle_loads
from random import choice as random_choice
from typing import Any

import redis
from django.conf import settings as django_settings, settings
from redis.client import StrictRedis
from redis.exceptions import TimeoutError, ConnectionError
from redis.sentinel import MasterNotFoundError
from redis.sentinel import Sentinel

from metax_api.utils.utils import executing_test_case, executing_travis

_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug


class RedisClient(object):
    def __init__(self, db=0):
        if settings.REDIS_USE_PASSWORD is True:
            self.client = redis.Redis(
                password=settings.REDIS["PASSWORD"],
                retry_on_timeout=True,
                host=settings.REDIS["HOST"],
                port=settings.REDIS["PORT"],
            )
        else:
            self.client = redis.Redis(
                retry_on_timeout=True,
                host=settings.REDIS["HOST"],
                port=settings.REDIS["PORT"],
            )
        _logger.info(
            f"RedisClient created with host:{settings.REDIS['HOST']} port:{settings.REDIS['PORT']}"
        )

    def set(self, key, value, **kwargs):
        pickled_data = pickle_dumps(value)
        return self.client.set(key, pickled_data, **kwargs)

    def get_or_set(self, key, value, **kwargs):
        """
        Atomic set of value only if key did not exist yet.

        Returns True if set was successful, None if set failed (= value existed)

        https://redis.io/commands/setnx Not recommended any longer for distributed locks...
        https://redis.io/topics/distlock However this is also just a proposal and no official
        implementation exists yet
        """
        return self.set(key, value, nx=True, **kwargs)

    def get(self, key, master=False, **kwargs):
        value: Any
        try:
            value = self.client.get(key)
        except KeyError as e:
            _logger.error(f"Redis has no {key} as key: {e}")
        return pickle_loads(value) if value is not None else None

    def delete(self, *keys):
        self.client.delete(*keys)

    def get_master(self):
        """
        Expose the master node to permit any operation in redis-py
        """
        return self.client


class _RedisCacheService:
    def __init__(self, db=0):
        """
        db: database index to read/write to. available indexes 0-15.
        """
        if hasattr(django_settings, "REDIS"):
            settings = django_settings.REDIS
        else:
            raise Exception("Missing configuration from settings.py: REDIS")

        if not settings.get("SENTINEL", None):
            raise Exception("Missing configuration from settings for REDIS: SENTINEL")
        if not settings["SENTINEL"].get("HOSTS", None):
            raise Exception(
                "Missing configuration from settings for REDIS.SENTINEL: HOSTS"
            )
        if not settings["SENTINEL"].get("SERVICE", None):
            raise Exception(
                "Missing configuration from settings for REDIS.SENTINEL: SERVICE"
            )
        if not settings.get("TEST_DB", None):
            raise Exception("Missing configuration from settings for REDIS: TEST_DB")
        if len(settings["SENTINEL"]["HOSTS"]) < 3:
            raise Exception(
                "Invalid configuration in settings for REDIS.SENTINEL: HOSTS minimum number of hosts is 3"
            )

        if executing_test_case():
            db = settings["TEST_DB"]
        elif db == settings["TEST_DB"]:
            raise Exception(
                "Invalid db: db index %d is reserved for test suite execution." % db
            )

        self._redis_local = StrictRedis(
            host="localhost",
            port=settings["LOCALHOST_PORT"],
            password=settings["PASSWORD"],
            socket_timeout=settings.get("SOCKET_TIMEOUT", 0.1),
            db=db,
        )

        self._sentinel = Sentinel(
            settings["SENTINEL"]["HOSTS"],
            password=settings["PASSWORD"],
            socket_timeout=settings.get("SOCKET_TIMEOUT", 0.1),
            db=db,
        )

        self._service_name = settings["SENTINEL"]["SERVICE"]
        self._DEBUG = settings.get("DEBUG", False)
        self._node_count = self._count_nodes()

    def set(self, key, value, **kwargs):
        if self._DEBUG:
            d("cache: set()...")

        pickled_data = pickle_dumps(value)
        master = self._get_master()

        try:
            return master.set(key, pickled_data, **kwargs)
        except (TimeoutError, ConnectionError, MasterNotFoundError) as e:
            if self._DEBUG:
                d(
                    f"cache: master timed out or not found, or connection refused. \
                    No write instances available. error: {str(e)}"
                )
            # no master available
            return

    def get_or_set(self, key, value, **kwargs):
        """
        Atomic set of value only if key did not exist yet.

        Returns True if set was successful, None if set failed (= value existed)

        https://redis.io/commands/setnx Not recommended any longer for distributed locks...
        https://redis.io/topics/distlock However this is also just a proposal and no official
        implementation exists yet
        """
        return self.set(key, value, nx=True, **kwargs)

    def get(self, key, master=False, **kwargs):
        """
        Always try first from localhost, since when apps are running clustered,
        the 'randomization' already happened when the connection reached this server.
        If get from localhost fails, only then connect to sentinels, to randomly select
        some slave or master for reading. Fallback to master if other slave failed.

        Use of master can be forced by using the master_only flag in the constructor, or passing
        master=True to this method for a single get operation.
        """
        if self._DEBUG:
            d("cache: get()...")

        if master:
            return self._get_from_master(key, **kwargs)
        else:
            try:
                return self._get_from_local(key, **kwargs)
            except (TimeoutError, ConnectionError):
                # local is broken or busy, get randomly from other instances
                if self._slave_chosen():
                    try:
                        return self._get_from_slave(key, **kwargs)
                    except (TimeoutError, ConnectionError):
                        pass
                # lady luck chose master, or read from slave had an error
                return self._get_from_master(key, **kwargs)

    def delete(self, *keys):
        if self._DEBUG:
            d("cache: delete()...")

        master = self._get_master()

        try:
            master.delete(*keys)
        except (TimeoutError, ConnectionError, MasterNotFoundError) as e:
            if self._DEBUG:
                d(
                    f"cache: master timed out or not found, or connection refused. \
                    No write instances available. error: {str(e)}"
                )
            # no master available
            return

        if self._DEBUG:
            test = master.get(keys[0])
            if not test:
                d("cache: delete() successful")
            else:
                d("cache: delete() unsuccessful, could not delete data?")

    def _get_from_local(self, key, **kwargs):
        try:
            res = self._redis_local.get(key, **kwargs)
        except (TimeoutError, ConnectionError):
            if self._DEBUG:
                d(
                    "cache: _redis_local.get() timed out or connection refused, "
                    "trying from other slaves instead. fail-over in process?"
                )
            raise
        else:
            return pickle_loads(res) if res is not None else None

    def _get_from_slave(self, key, **kwargs):
        node = self._get_slave()
        try:
            res = node.get(key, **kwargs)
        except (TimeoutError, ConnectionError):
            if self._DEBUG:
                d(
                    "cache: slave.get() timed out or connection refused, "
                    "trying from master instead. fail-over in process?"
                )
            # fail-over propbably happened, and the old slave is now a master
            # (in case there was only one slave). try master instead
            raise
        else:
            return pickle_loads(res) if res is not None else None

    def _get_from_master(self, key, **kwargs):
        master = self._get_master()
        try:
            res = master.get(key, **kwargs)
        except (TimeoutError, MasterNotFoundError):
            if self._DEBUG:
                d(
                    "cache: master timed out also. no read instances available. returning None"
                )
            # uh oh, no master available either. either all redis instances have hit the bucket,
            # or there is a fail-over in process, and a new master will be in line in a moment
            return None
        return pickle_loads(res) if res is not None else None

    def get_master(self):
        """
        Expose the master node to permit any operation in redis-py
        """
        return self._get_master()

    def _slave_chosen(self):
        return random_choice(range(0, self._node_count)) != 0

    def _get_master(self):
        if self._DEBUG:
            d("cache: getting master")
        return self._sentinel.master_for(self._service_name, socket_timeout=0.1)

    def _get_slave(self):
        if self._DEBUG:
            d("cache: getting slave")
        return self._sentinel.slave_for(self._service_name, socket_timeout=0.1)

    def _count_nodes(self):
        return (
            len(self._sentinel.discover_slaves(self._service_name)) + 1
        )  # +1 is master


class _RedisCacheServiceDummy:

    """
    A dummy redis client that writes to a file on disk.
    """

    _storage_path = "/tmp/redis_dummy_storage"

    def __init__(self, *args, **kwargs):
        # d('Note: using dummy cache')
        pass

    def set(self, key, value, **kwargs):
        storage = self._get_storage()
        storage[key] = value
        self._save_storage(storage)

    def get(self, key, **kwargs):
        return self._get_storage().get(key, None)

    def get_or_set(self, key, value, **kwargs):
        if self.get(key):
            return False
        else:
            self.set(
                key,
                value,
            )
            return True

    def delete(self, key, **kwargs):
        storage = self._get_storage()
        storage.pop(key, False)
        self._save_storage(storage)

    def get_master(self):
        return self

    def flushdb(self):
        self._save_storage({})
        return True

    def _get_storage(self):
        try:
            with open(self._storage_path, "r") as f:
                return load_json(f)
        except IOError:
            self._save_storage({})
            try:
                with open(self._storage_path, "r") as f:
                    return load_json(f)
            except Exception as e:
                _logger.error(
                    "Could not open dummy cache file for reading at %s: %s"
                    % (self._storage_path, str(e))
                )
        except Exception as e:
            _logger.error(
                "Could not open dummy cache file for reading at %s: %s"
                % (self._storage_path, str(e))
            )

    def _save_storage(self, storage):
        try:
            with open(self._storage_path, "w") as f:
                dump_json(storage, f)
        except Exception as e:
            _logger.error(
                "Could not open dummy cache file for writing at %s: %s"
                % (self._storage_path, str(e))
            )


if executing_travis():
    RedisCacheService = RedisClient
else:
    RedisCacheService = RedisClient
