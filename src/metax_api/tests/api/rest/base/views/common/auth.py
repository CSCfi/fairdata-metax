from rest_framework import status

from metax_api.tests.api.rest.base.views.datasets.write import CatalogRecordApiWriteCommon


class ApiAccessAuthorization(CatalogRecordApiWriteCommon):

    """
    Test API-wide access restriction rules
    """

    def setUp(self):
        super(ApiAccessAuthorization, self).setUp()
        # test user api_auth_user has some custom api permissions set in settings.py
        self._use_http_authorization(username='api_auth_user')

    def test_write_access_ok(self):
        """
        User api_auth_user should have write and read access to datasets api.
        """
        response = self.client.get('/rest/datasets/1')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        cr = response.data
        cr['contract'] = 1

        response = self.client.put('/rest/datasets/1', cr, format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_write_access_error(self):
        """
        User api_auth_user should have read access to files api, but not write.
        """
        response = self.client.get('/rest/files/1')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        file = response.data
        file['file_format'] = 'text/html'

        response = self.client.put('/rest/files/1', file, format='json')
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_read_for_world_ok(self):
        """
        Reading datasets api should be permitted even without any authorization.
        """
        self.client._credentials = {}
        response = self.client.get('/rest/datasets/1')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
