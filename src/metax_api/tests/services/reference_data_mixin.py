from unittest.mock import patch

from django.test import TestCase

from metax_api.services import ReferenceDataMixin as RDM
from metax_api.tests.utils import TestClassUtils
from metax_api.utils import (
    _RedisSentinelCache,
    _RedisSentinelCacheDummy,
    executing_travis,
    RedisSentinelCache,
    ReferenceDataLoader,
)

if executing_travis():
    _RedisCacheClass = _RedisSentinelCacheDummy
else:
    _RedisCacheClass = _RedisSentinelCache


class MockRedisSentinelCache(_RedisCacheClass):

    def __init__(self, return_data_after_retries=0, *args, **kwargs):
        self.call_count = 0
        self.return_data_after_retries = return_data_after_retries
        super(MockRedisSentinelCache, self).__init__(*args, **kwargs)

    def get(self, *args, **kwargs):
        """
        Fakes "cache is currently being populated by another process" by returning None
        until the request has retried return_data_after_retries times
        """
        self.call_count += 1
        if self.call_count <= self.return_data_after_retries - 1:
            return None
        return super(MockRedisSentinelCache, self).get(*args, **kwargs)


class ReferenceDataMixinTests(TestCase, TestClassUtils):

    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """

        # so that we dont have to wait all day for tests to execute
        RDM.REF_DATA_RELOAD_MAX_RETRIES = 2

        super(ReferenceDataMixinTests, cls).setUpClass()
        cls.cache = RedisSentinelCache()

    def setUp(self):
        self.cache.delete('reference_data')

    def tearDown(self):
        # re-populate cache with ref data to not disturb other test suites
        ReferenceDataLoader.populate_cache_reference_data(RedisSentinelCache())

    def test_reference_data_reload_ok(self):
        """
        Reference data is deleted from cache in setUp(), but the RDM should
        try to reload it from ES. No mocking, using original code.
        """
        RDM.get_reference_data(self.cache)
        self._assert_reference_data_ok()

    @patch('metax_api.utils.ReferenceDataLoader.populate_cache_reference_data')
    def test_reference_data_reload_in_progress(self, mock_populate_cache_reference_data):
        """
        Ensure the reference data fetch survives when another request has already started
        reloading the reference data. The method should retry for a few seconds, and finally succeed
        """
        return_data_after_retries = 1
        mock_populate_cache_reference_data.return_value = 'reload_started_by_other'
        self._populate_cache_reference_data()
        mock_cache = MockRedisSentinelCache(return_data_after_retries=return_data_after_retries)

        # the method being tested
        RDM.get_reference_data(mock_cache)

        self._assert_reference_data_ok()
        self.assertEqual(mock_cache.call_count, return_data_after_retries, 'ref data fetching should have retried a few times before succeeding')

    @patch('metax_api.utils.ReferenceDataLoader.populate_cache_reference_data')
    def test_reference_data_reload_in_progress_times_out(self, mock_populate_cache_reference_data):
        """
        Ensure the reference data fetch finally gives up when another request has already started
        reloading the reference data. The method should retry for a few seconds, and then give up
        """

        # since get_reference_data() retries MAX_RETRIES times, it should give up
        return_data_after_retries = 100

        mock_populate_cache_reference_data.return_value = 'reload_started_by_other'
        self._populate_cache_reference_data()
        mock_cache = MockRedisSentinelCache(return_data_after_retries=return_data_after_retries)

        try:
            # the method being tested
            RDM.get_reference_data(mock_cache)
        except Exception as e:
            self.assertEqual(e.__class__.__name__, 'Http503', 'ref data reload should raise Http503 when it gives up')

        self._assert_reference_data_ok()

    @patch('metax_api.utils.ReferenceDataLoader.populate_cache_reference_data')
    def test_reference_data_reload_failed(self, mock_populate_cache_reference_data):
        """
        Ensure 503 is raised when reload by the request failed
        """

        # since get_reference_data() retries MAX_RETRIES times, it should give up
        return_data_after_retries = 100

        # implies the request itself is reloading the data, and get_reference_data() will not retry
        # after first fail.
        mock_populate_cache_reference_data.return_value = None

        self._populate_cache_reference_data()
        mock_cache = MockRedisSentinelCache(return_data_after_retries=return_data_after_retries)

        try:
            # the method being tested
            RDM.get_reference_data(mock_cache)
        except Exception as e:
            self.assertEqual(e.__class__.__name__, 'Http503', 'ref data reload should raise Http503 when it gives up')

        self._assert_reference_data_ok()

    def _assert_reference_data_ok(self):
        self.assertEqual('reference_data' in self.cache.get('reference_data'), True)
        self.assertEqual('organization_data' in self.cache.get('reference_data'), True)

    def _populate_cache_reference_data(self):
        """
        The method ReferenceDataLoader.populate_cache_reference_data always returns a mock value
        in the tests. Instead, this method is executed to load something in the cache, which
        get_reference_data() will then try to return
        """
        self.cache.set('reference_data', {
            'reference_data': { 'language': ['stuff'] },
            'organization_data': { 'organization': ['stuff'] },
        })
