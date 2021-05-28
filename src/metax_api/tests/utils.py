# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT
import logging
import sys
from base64 import b64encode
from contextlib import contextmanager
from json import load as json_load
from os import path

import jwt
import responses
from django.conf import settings as django_settings
from rest_framework import status

logger = logging.getLogger(__name__)

datetime_format = "%Y-%m-%dT%H:%M:%S.%fZ"

# path to data used by automatic tests
test_data_file_path = "metax_api/tests/testdata/test_data.json"


def assert_catalog_record_is_open_access(cr):
    from metax_api.models.catalog_record import ACCESS_TYPES

    access_type = (
        cr["research_dataset"].get("access_rights", {}).get("access_type", {}).get("identifier", "")
    )
    assert access_type == ACCESS_TYPES["open"]


def assert_catalog_record_not_open_access(cr):
    from metax_api.models.catalog_record import ACCESS_TYPES

    access_type = (
        cr["research_dataset"].get("access_rights", {}).get("access_type", {}).get("identifier", "")
    )
    assert access_type != ACCESS_TYPES["open"]


def get_json_schema(model_name):
    with open(
        path.dirname(path.realpath(__file__))
        + "/../api/rest/base/schemas/%s_schema.json" % model_name
    ) as f:
        return json_load(f)


def get_test_oidc_token(new_proxy=False):
    # note: leaving new_proxy param in place for now to avoid conflicts.
    return {
        "sub": "randomstringhere",
        "displayName": "Teppo Testaaja",
        "eppn": "testuser@csc.fi",
        "iss": "https://fd-auth.csc.fi",
        "group_names": ["TAITO01:2002013", "IDA01:2001036"],
        "schacHomeOrganizationType": "urn:schac:homeOrganizationType:fi:other",
        "given_name": "Teppo",
        "nonce": "qUu_QUwfHaB92m3ng8PVZ3ycGXlecvMejgATXsuC9OM",
        "aud": "fairdata-metax",
        "uid": "testuser@csc.fi",
        "acr": "password",
        "auth_time": 1535535590,
        "name": "Teppo Testaaja",
        "schacHomeOrganization": "csc.fi",
        "exp": 1535539191,
        "iat": 1535535591,
        "family_name": "Testaaja",
        "email": "teppo.testaaja@csc.fi",
        "CSCUserName": "testuser",
        "CSCOrgNameFi": "IT Center for Science",
    }


def generate_test_identifier(itype, index, urn=True):
    """
    Generate urn-type identifier ending with uuid4

    :param index: positive integer for making test identifiers unique. Replaces characters from the end of uuid
    :param itype: to separate identifiers from each other on model level (e.g. is it a catalog record or data catalog)
    :param urn: use urn prefix or not
    :return: test identifier
    """

    postfix = str(index)
    # Use the same base identifier for the tests and vary by the identifier type and the incoming positive integer
    uuid = str(itype) + "955e904-e3dd-4d7e-99f1-3fed446f96d5"
    if urn:
        return "urn:nbn:fi:att:%s" % (uuid[: -len(postfix)] + postfix)
    return uuid[: -len(postfix)] + postfix


def generate_test_token(payload):
    """
    While the real algorithm used in the Fairdata auth component is RS256, HS256 is the only one
    supported in the PyJWT lib, and since we are mocking the responses anyway, it does not matter,
    as long as the token otherwise looks legit, can be parse etc.
    """
    try:
        return jwt.encode(payload, "secret", "HS256").decode("utf-8")
    except AttributeError as e:
        logger.error(e)
        return jwt.encode(payload, "secret", "HS256")


class TestClassUtils:

    """
    Test classes may (multi-)inherit this class in addition to APITestCase to use these helpers
    """

    # default api version is v1. v2 api tests will set it to v2
    api_version = "v1"

    def create_end_user_data_catalogs(self):
        from metax_api.models import DataCatalog
        from metax_api.utils import get_tz_aware_now_without_micros

        dc = DataCatalog.objects.get(pk=1)
        catalog_json = dc.catalog_json
        for identifier in django_settings.END_USER_ALLOWED_DATA_CATALOGS:
            catalog_json["identifier"] = identifier
            DataCatalog.objects.create(
                catalog_json=catalog_json,
                date_created=get_tz_aware_now_without_micros(),
                catalog_record_services_create="testuser,api_auth_user,metax",
                catalog_record_services_edit="testuser,api_auth_user,metax",
                catalog_record_services_read="testuser,api_auth_user,metax",
            )

    def create_legacy_data_catalogs(self):
        from metax_api.models import DataCatalog
        from metax_api.utils import get_tz_aware_now_without_micros

        dc = DataCatalog.objects.get(pk=1)
        catalog_json = dc.catalog_json
        for identifier in django_settings.LEGACY_CATALOGS:
            catalog_json["identifier"] = identifier
            catalog_json["dataset_versioning"] = False
            catalog_json["research_dataset_schema"] = "att"
            DataCatalog.objects.create(
                catalog_json=catalog_json,
                date_created=get_tz_aware_now_without_micros(),
                catalog_record_services_create="testuser,api_auth_user,metax",
                catalog_record_services_edit="testuser,api_auth_user,metax",
                catalog_record_services_read="testuser,api_auth_user,metax",
            )

    def _set_http_authorization(self, credentials_type):
        # Deactivate credentials
        if credentials_type == "no":
            self.client.credentials()
        elif credentials_type == "service":

            self._use_http_authorization(username="metax")
        elif credentials_type == "owner":
            self._use_http_authorization(method="bearer", token=self.token)
            self._mock_token_validation_succeeds()
        else:
            self.client.credentials()

    def _use_http_authorization(
        self,
        username="testuser",
        password=None,
        header_value=None,
        method="basic",
        token=None,
    ):
        """
        Include a HTTP Authorization header in api requests. By default, the test
        user specified in settings.py will be used. Different user credentials can be
        passed in the parameters.

        Parameter 'header_value' can be used to directly insert the header value, to for example
        test for malformed values, or other auth methods than BA.
        """
        if header_value:
            # used as is
            pass
        elif method == "basic":
            if username == "testuser":
                user = django_settings.API_TEST_USER
                username = user["username"]
                if not password:
                    # password can still be passed as a param, to test wrong password
                    password = user["password"]
            elif username == "metax":
                user = django_settings.API_METAX_USER
                username = user["username"]
                password = user["password"]
            elif username == "api_auth_user":
                user = django_settings.API_AUTH_TEST_USER
                username = user["username"]
                password = user["password"]
            else:
                if not password:
                    raise Exception("Missing parameter 'password' for HTTP Authorization header")

            header_value = "Basic %s" % b64encode(
                bytes("%s:%s" % (username, password), "utf-8")
            ).decode("utf-8")

        elif method == "bearer":
            assert token is not None, "token (dictionary) is required when using auth method bearer"
            header_value = "Bearer %s" % generate_test_token(token)

        self.client.credentials(HTTP_AUTHORIZATION=header_value)

    def _mock_token_validation_succeeds(self):
        """
        Since the End User authnz utilizes OIDC, and there is no legit local OIDC OP,
        responses from /secure/validate_token are mocked. The endpoint only returns
        200 OK for successful token validation, or 403 for failed validation.

        Use this method to simulate requests where token validation succeeds.
        """
        responses.add(responses.GET, django_settings.VALIDATE_TOKEN_URL, status=200)

    def _mock_token_validation_fails(self):
        """
        Since the End User authnz utilizes OIDC, and there is no legit local OIDC OP,
        responses from /secure/validate_token are mocked. The endpoint only returns
        200 OK for successful token validation, or 403 for failed validation.

        Use this method to simulate requests where token validation fails.
        """
        responses.add(responses.GET, django_settings.VALIDATE_TOKEN_URL, status=403)

    def _get_object_from_test_data(self, model_name, requested_index=0):
        with open(test_data_file_path) as test_data_file:
            test_data = json_load(test_data_file)

        model = "metax_api.%s" % model_name
        i = 0

        for row in test_data:
            if row["model"] == model:
                if i == requested_index:
                    obj = {
                        "id": row["pk"],
                    }
                    obj.update(row["fields"])
                    return obj
                else:
                    i += 1

        raise Exception(
            "Could not find model %s from test data with index == %d. "
            "Are you certain you generated rows for model %s in generate_test_data.py?"
            % (model_name, requested_index, model_name)
        )

    def _create_cr_for_owner(self, pk_for_template_cr, data):
        self.token = get_test_oidc_token()
        if "editor" in data:
            data.pop("editor", None)
        data["user_created"] = self.token["CSCUserName"]
        data["metadata_provider_user"] = self.token["CSCUserName"]
        data["metadata_provider_org"] = self.token["schacHomeOrganization"]
        data["metadata_owner_org"] = self.token["schacHomeOrganization"]
        data["data_catalog"]["identifier"] = django_settings.END_USER_ALLOWED_DATA_CATALOGS[0]

        data.pop("identifier", None)
        data["research_dataset"].pop("preferred_identifier", None)

        response = self.client.post(f"/rest/{self.api_version}/datasets", data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        return response.data["id"]

    def get_open_cr_with_files_and_dirs_from_api_with_file_details(
        self, set_owner=False, use_login_access_type=False
    ):
        from metax_api.models import CatalogRecord
        from metax_api.models.catalog_record import ACCESS_TYPES

        # Use http auth to get complete details of the catalog record
        self._use_http_authorization(username="metax")
        pk = 13

        if set_owner:
            response = self.client.get(
                f"/rest/{self.api_version}/datasets/{pk}?include_user_metadata"
            )
            pk = self._create_cr_for_owner(pk, response.data)

        CatalogRecord.objects.get(pk=pk).calculate_directory_byte_sizes_and_file_counts()

        if use_login_access_type:
            response_data = self.client.get(
                f"/rest/{self.api_version}/datasets/{pk}?include_user_metadata"
            ).data
            response_data["research_dataset"]["access_rights"]["access_type"][
                "identifier"
            ] = ACCESS_TYPES["login"]
            response = self.client.put(
                f"/rest/{self.api_version}/datasets/{pk}?include_user_metadata",
                response_data,
                format="json",
            )
            self.assertEqual(response.status_code, status.HTTP_200_OK)
            response = self.client.get(
                f"/rest/{self.api_version}/datasets/{pk}?include_user_metadata&file_details"
            )
            rd = response.data["research_dataset"]
        else:
            response = self.client.get(
                f"/rest/{self.api_version}/datasets/{pk}?include_user_metadata&file_details"
            )
            rd = response.data["research_dataset"]
            # Verify we are dealing with an open research dataset
            assert_catalog_record_is_open_access(response.data)

        # Verify we have both files and dirs in the catalog record
        self.assertTrue("files" in rd and len(rd["files"]) > 0)
        self.assertTrue("directories" in rd and len(rd["directories"]) > 0)

        # Empty credentials to not mess up the actual test
        self.client.credentials()

        return response.data

    def get_restricted_cr_with_files_and_dirs_from_api_with_file_details(self, set_owner=False):
        from metax_api.models import CatalogRecord
        from metax_api.models.catalog_record import ACCESS_TYPES

        # Use http auth to get complete details of the catalog record
        self._use_http_authorization("metax")
        pk = 13

        response = self.client.get(f"/rest/{self.api_version}/datasets/{pk}?include_user_metadata")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        data = response.data

        # Set access_type to restricted
        data["research_dataset"]["access_rights"]["access_type"]["identifier"] = ACCESS_TYPES[
            "restricted"
        ]

        if set_owner:
            pk = self._create_cr_for_owner(pk, data)
        else:
            response = self.client.put(
                f"/rest/{self.api_version}/datasets/{pk}?include_user_metadata",
                data,
                format="json",
            )
            self.assertEqual(response.status_code, status.HTTP_200_OK)

        CatalogRecord.objects.get(pk=pk).calculate_directory_byte_sizes_and_file_counts()
        response = self.client.get(
            f"/rest/{self.api_version}/datasets/{pk}?include_user_metadata&file_details"
        )

        # Verify we are dealing with restricted research dataset
        assert_catalog_record_not_open_access(response.data)
        rd = response.data["research_dataset"]

        # Verify we have both files and dirs in the catalog record
        self.assertTrue("files" in rd and len(rd["files"]) > 0)
        self.assertTrue("directories" in rd and len(rd["directories"]) > 0)

        # Empty credentials to not mess up the actual test
        self.client.credentials()

        return response.data

    def get_embargoed_cr_with_files_and_dirs_from_api_with_file_details(self, is_available):
        from metax_api.models import CatalogRecord
        from metax_api.models.catalog_record import ACCESS_TYPES

        # Use http auth to get complete details of the catalog record
        self._use_http_authorization("metax")
        pk = 13

        response = self.client.get(f"/rest/{self.api_version}/datasets/{pk}?include_user_metadata")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        data = response.data

        # Set access_type to embargo
        data["research_dataset"]["access_rights"]["access_type"]["identifier"] = ACCESS_TYPES[
            "embargo"
        ]

        if is_available:
            data["research_dataset"]["access_rights"]["available"] = "2000-01-01"
        else:
            data["research_dataset"]["access_rights"]["available"] = "3000-01-01"

        response = self.client.put(f"/rest/{self.api_version}/datasets/13", data, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        CatalogRecord.objects.get(pk=pk).calculate_directory_byte_sizes_and_file_counts()
        response = self.client.get(
            f"/rest/{self.api_version}/datasets/{pk}?include_user_metadata&file_details"
        )

        # Verify we are dealing with restricted research dataset
        assert_catalog_record_not_open_access(response.data)
        rd = response.data["research_dataset"]

        # Verify we have both files and dirs in the catalog record
        self.assertTrue("files" in rd and len(rd["files"]) > 0)
        self.assertTrue("directories" in rd and len(rd["directories"]) > 0)

        # Empty credentials to not mess up the actual test
        self.client.credentials()

        return response.data

    def _get_ida_dataset_without_files(self):
        data = self._get_object_from_test_data("catalogrecord", requested_index=0)

        data.pop("identifier", None)
        data["research_dataset"].pop("preferred_identifier", None)

        data.pop("files", None)
        data["research_dataset"].pop("files", None)
        data["research_dataset"]["total_files_byte_size"] = 0

        return data

    def _get_new_file_data(
        self,
        file_n,
        project=None,
        file_path=None,
        directory_path=None,
        open_access=False,
    ):
        from_test_data = self._get_object_from_test_data("file", requested_index=0)

        if not project:
            project = "research_project_112"
        if not directory_path:
            directory_path = "/prj_112_root/science_data_C/phase_2/2017/10/dir_" + file_n
        if not file_path:
            file_path = directory_path + "/file_" + file_n

        identifier = "urn:nbn:fi:100" + file_n

        from_test_data.update(
            {
                "file_name": "tiedosto_name_" + file_n,
                "file_path": file_path,
                "identifier": identifier,
                "parent_directory": 24,
                "project_identifier": project,
                "open_access": open_access,
            }
        )
        del from_test_data["id"]
        return from_test_data


@contextmanager
def streamhandler_to_console(lggr):
    # Use 'up to date' value of sys.stdout for StreamHandler,
    # as set by test runner.
    stream_handler = logging.StreamHandler(sys.stdout)
    lggr.addHandler(stream_handler)
    yield
    lggr.removeHandler(stream_handler)


def testcase_log_console(lggr):
    def testcase_decorator(func):
        def testcase_log_console(*args, **kwargs):
            with streamhandler_to_console(lggr):
                return func(*args, **kwargs)

        return testcase_log_console

    return testcase_decorator
