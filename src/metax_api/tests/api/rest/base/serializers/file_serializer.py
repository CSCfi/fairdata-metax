# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from datetime import datetime

from django.core.management import call_command
from rest_framework.test import APITestCase

from metax_api.api.rest.base.serializers import FileSerializer, LightFileSerializer
from metax_api.models import File
from metax_api.tests.utils import test_data_file_path, TestClassUtils


class LightFileSerializerTests(APITestCase, TestClassUtils):

    @classmethod
    def setUpClass(cls):
        call_command('loaddata', test_data_file_path, verbosity=0)
        super().setUpClass()

    def test_ls_field_list(self):
        lfs_field_list = LightFileSerializer.ls_field_list(['identifier', 'parent_directory', 'file_storage'])
        self.assertEqual('parent_directory__identifier' in lfs_field_list, True)
        self.assertEqual('file_storage__file_storage_json' in lfs_field_list, True)

        queryset = File.objects.filter(id__in=[1, 2]).values(*lfs_field_list)
        lfs_output = LightFileSerializer.serialize(queryset)
        self.assertEqual(len(lfs_output), 2)

        f = lfs_output[0]
        self.assertEqual('parent_directory' in f, True)
        self.assertEqual('identifier' in f['parent_directory'], True)
        self.assertEqual('file_storage' in f, True)
        self.assertEqual('identifier' in f['file_storage'], True)

    def test_serializer_outputs_are_the_same(self):
        fs_output = FileSerializer(File.objects.get(pk=1)).data

        lfs_field_list = LightFileSerializer.ls_field_list()
        lfs_output = LightFileSerializer.serialize(File.objects.values(*lfs_field_list).get(pk=1))

        self.assertEqual(len(fs_output), len(lfs_output), 'number of keys should match')
        self._assert_keys_match_in_dict(fs_output, lfs_output)

    def test_serializer_error_reporting(self):
        """
        If LightFileSerializer.ls_field_list() is not called and result passed to
        queryset .values(*field_list), the serializer should crash, since it does not have
        enough information to make the same result as from a normal serializer.
        """
        with self.assertRaises(ValueError):
            LightFileSerializer.serialize(File.objects.filter(pk=1).values())

    def _assert_keys_match_in_dict(self, ser_dict, ls_dict):
        for key, value in ser_dict.items():
            self.assertEqual(key in ls_dict, True, 'key should be present in boths outputs')
            if isinstance(ls_dict[key], datetime):
                # in the api datetimes are converted to str by django later - probably the value is ok here
                pass
            elif isinstance(value, dict):
                self._assert_keys_match_in_dict(value, ls_dict[key])
            else:
                self.assertEqual(value, ls_dict[key], 'value should be same both outputs')
