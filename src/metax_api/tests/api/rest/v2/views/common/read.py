# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from rest_framework import status

from metax_api.tests.api.rest.base.views.datasets.read import CatalogRecordApiReadCommon


class ApiReadPaginationTests(CatalogRecordApiReadCommon):

    """
    pagination
    """

    def test_read_catalog_record_list_pagination_1(self):
        for param in ["pagination=true", "pagination", ""]:
            response = self.client.get("/rest/datasets?{}&limit=2&offset=0".format(param))
            self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
            self.assertEqual(
                len(response.data["results"]),
                2,
                "There should have been exactly two results",
            )
            self.assertEqual(
                response.data["results"][0]["id"],
                1,
                "Id of first result should have been 1",
            )