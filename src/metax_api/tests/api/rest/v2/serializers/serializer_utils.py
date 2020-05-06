# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from django.core.management import call_command
from rest_framework.serializers import ValidationError
from rest_framework.test import APITestCase

from metax_api.api.rest.base.serializers import validate_json
from metax_api.models import CatalogRecord
from metax_api.tests.utils import test_data_file_path, get_json_schema, TestClassUtils


schema = get_json_schema('ida_dataset')


class ValidateJsonTests(APITestCase, TestClassUtils):

    """
    Test some customizations of the json schema validator.
    """

    @classmethod
    def setUpClass(cls):
        call_command('loaddata', test_data_file_path, verbosity=0)
        super().setUpClass()

    def test_validate_date_field(self):
        """
        Fields marked in json schema as "@type":"http://www.w3.org/2001/XMLSchema#date"
        have the optional "format": "date" applied, and use a format checker during schema validation.
        Ensure it works.
        """
        rd = CatalogRecord.objects.values('research_dataset').get(pk=1)['research_dataset']

        rd['issued'] = '2018-09-29'
        try:
            validate_json(rd, schema)
        except ValidationError:
            self.fail('Validation raised ValidationError - should have validated ok')

        rd['issued'] = 'absolutely should fail'
        with self.assertRaises(ValidationError):
            validate_json(rd, schema)

        rd['issued'] = '2018-09-29T20:20:20+03:00'
        with self.assertRaises(ValidationError):
            validate_json(rd, schema)

        rd['issued'] = '2018-09-29T20:20:20'
        with self.assertRaises(ValidationError):
            validate_json(rd, schema)

        rd['issued'] = '2018-09-29 20:20:20'
        with self.assertRaises(ValidationError):
            validate_json(rd, schema)

    def test_validate_date_time_field(self):
        """
        Fields marked in json schema as "@type":"http://www.w3.org/2001/XMLSchema#dateTime"
        have the optional "format": "date-time" applied, and use a format checker during schema validation.
        Ensure it works.
        """
        rd = CatalogRecord.objects.values('research_dataset').get(pk=1)['research_dataset']

        rd['modified'] = '2018-09-29T20:20:20+03:00'
        try:
            validate_json(rd, schema)
        except ValidationError:
            self.fail('Validation raised ValidationError - should have validated ok')

        # note: jsonschema.FormatChecker accepts below also
        rd['modified'] = '2018-09-29T20:20:20.123456789+03:00'
        try:
            validate_json(rd, schema)
        except ValidationError:
            self.fail('Validation raised ValidationError - should have validated ok')

        rd['modified'] = 'absolutely should fail'
        with self.assertRaises(ValidationError):
            validate_json(rd, schema)

        rd['modified'] = '2018-09-29'
        with self.assertRaises(ValidationError):
            validate_json(rd, schema)

        rd['modified'] = '2018-09-29 20:20:20'
        with self.assertRaises(ValidationError):
            validate_json(rd, schema)

        rd['modified'] = '2018-09-29T20:20:20'
        with self.assertRaises(ValidationError):
            validate_json(rd, schema)

        rd['modified'] = '2018-09-29 20:20:20+03:00'
        with self.assertRaises(ValidationError):
            validate_json(rd, schema)
