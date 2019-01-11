# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from json import load

from django.conf import settings as django_settings
from django.http import Http404
from rest_framework.decorators import list_route
from rest_framework.response import Response

from metax_api.exceptions import Http400
from metax_api.models import CatalogRecord
from metax_api.models.catalog_record import DataciteDOIUpdate
from metax_api.utils import generate_doi_identifier, is_metax_generated_doi_identifier
from .common_rpc import CommonRPC


class DatasetRPC(CommonRPC):

    @list_route(methods=['get'], url_path="get_minimal_dataset_template")
    def get_minimal_dataset_template(self, request):
        if request.query_params.get('type', None) not in ['service', 'enduser']:
            raise Http400({
                'detail': ['query param \'type\' missing or wrong. please specify ?type= as one of: service, enduser']
            })

        with open('metax_api/exampledata/dataset_minimal.json', 'rb') as f:
            example_ds = load(f)

        example_ds['data_catalog'] = django_settings.END_USER_ALLOWED_DATA_CATALOGS[0]

        if request.query_params['type'] == 'enduser':
            example_ds.pop('metadata_provider_org', None)
            example_ds.pop('metadata_provider_user', None)

        return Response(example_ds)

    @list_route(methods=['post'], url_path="set_preservation_identifier")
    def set_preservation_identifier(self, request):
        if not request.query_params.get('identifier', False):
            raise Http400({
                'detail': ['Query param \'identifier\' missing. Please specify ?identifier=<catalog record identifier>']
            })

        try:
            cr = CatalogRecord.objects.get(identifier=request.query_params['identifier'])
        except CatalogRecord.DoesNotExist:
            raise Http404

        if not cr.dataset_in_ida_data_catalog():
            raise Http400({
                'detail': ['Setting preservation identifier not allowed for a non-IDA catalog record']
            })

        if cr.preservation_identifier:
            # If cr preservation identifier already exists, make sure it also exists in Datacite
            DataciteDOIUpdate(cr, cr.preservation_identifier, 'update')()
        else:
            pref_id = cr.research_dataset['preferred_identifier']
            if is_metax_generated_doi_identifier(pref_id):
                # Copy metax generated doi identifier to preservation identifier. NOTE: This code should never be
                # reached since if pref id is a metax generated doi, it should have already been copied to preservation
                # identifier (ida dataset), but this is done just in case something has previously failed...
                cr.preservation_identifier = pref_id
                super(CatalogRecord, cr).save()
                cr.save()
                DataciteDOIUpdate(cr, cr.preservation_identifier, 'update')()
            else:
                # Generate a new DOI for the dataset. If pref id is a metax generated urn, use that urn's suffix as
                # the doi suffix. Otherwise generate completely new doi.
                if pref_id.startswith('urn:nbn:fi:att:'):
                    cr.preservation_identifier = generate_doi_identifier(pref_id[len('urn:nbn:fi:att:'):])
                else:
                    cr.preservation_identifier = generate_doi_identifier()

                super(CatalogRecord, cr).save()
                DataciteDOIUpdate(cr, cr.preservation_identifier, 'create')()

        return Response(cr.preservation_identifier)
