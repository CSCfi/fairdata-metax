from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import Contract
from metax_api.tests.utils import test_data_file_path, TestClassUtils

class ContractApiReadTestV1(APITestCase, TestClassUtils):

    """
    Fields defined in ContractSerializer
    """
    file_field_names = (
        'id',
        'contract_json',
        'modified_by_user_id',
        'modified_by_api',
        'created_by_user_id',
        'created_by_api',
    )

    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        super(ContractApiReadTestV1, cls).setUpClass()

    def setUp(self):
        contract_from_test_data = self._get_object_from_test_data('contract', requested_index=0)
        self.pk = contract_from_test_data['id']

    def test_read_contract_list(self):
        response = self.client.get('/rest/datasets')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_read_contract_details_by_pk(self):
        response = self.client.get('/rest/contracts/%s' % self.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_read_contract_details_not_found(self):
        response = self.client.get('/rest/contracts/shouldnotexist')
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_model_fields_as_expected(self):
        response = self.client.get('/rest/contracts/%s' % self.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        actual_received_fields = [field for field in response.data.keys()]
        self._test_model_fields_as_expected(self.file_field_names, actual_received_fields)


class ContractApiWriteTestV1(APITestCase, TestClassUtils):

    def setUp(self):
        """
        Reloaded for every test case
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        contract_from_test_data = self._get_object_from_test_data('contract')
        self.pk = contract_from_test_data['id']

        """
        New data that is sent to the server for POST, PUT, PATCH requests. Modified
        slightly as approriate for different purposes
        """
        self.test_new_data = self._get_new_test_data()
        self.second_test_new_data = self._get_second_new_test_data()

    def test_update_contract(self):
        self.test_new_data['pk'] = self.pk
        response = self.client.put('/rest/contracts/%s' % self.pk, self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)
        self.assertEqual(len(response.data.keys()), 0, 'Returned dict should be empty')

    def test_update_contract_not_found(self):
        response = self.client.put('/rest/contracts/doesnotexist', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_delete_contract(self):
        url = '/rest/contracts/%s' % self.pk
        response = self.client.delete(url)
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

        try:
            deleted_contract = Contract.objects.get(pk=self.pk)
        except Contract.DoesNotExist:
            raise Exception('Deleted contract should not be deleted from the db, but marked as removed')

        self.assertEqual(deleted_contract.removed, True)
        related_catalog_records = deleted_contract.catalogrecord_set.all()
        for cr in related_catalog_records:
            self.assertEqual(cr.removed, True, 'Related CatalogRecord objects should be marked as removed')

    def _get_new_test_data(self):
        return {
            "contract_json": {
                "title": "Title of new contract",
                "identifier": "optional-identifier-new",
                "quota": 111204,
                "created": "2014-01-17T08:19:58Z",
                "modified": "2014-01-17T08:19:58Z",
                "description": "Description of unknown length",
                "contact": [{
                    "name": "Contact Name",
                    "phone": "+358501231234",
                    "email": "contact.email@csc.fi"
                }],
                "organization": {
                    "organization_identifier": "1234567abc",
                    "name": ["Mysterious organization"]
                },
                "service": [{
                    "identifier": "local:service:id",
                    "name": "Name of Service"
                }],
                "validity": {
                    "start_date": "2014-01-17T08:19:58Z"
                }
            }
        }

    def _get_second_new_test_data(self):
        return {
            "contract_json": {
                "title": "Title of second contract",
                "identifier": "optional-identifier-for-second",
                "quota": 111204,
                "created": "2014-01-17T08:19:58Z",
                "modified": "2014-01-17T08:19:58Z",
                "description": "Description of unknown length",
                "contact": [{
                    "name": "Contact Name",
                    "phone": "+358501231234",
                    "email": "contact.email@csc.fi"
                }],
                "organization": {
                    "organization_identifier": "1234567abc",
                    "name": ["Mysterious organization"]
                },
                "service": [{
                    "identifier": "local:service:id",
                    "name": "Name of Service"
                }],
                "validity": {
                    "start_date": "2014-01-17T08:19:58Z"
                }
            }
        }
