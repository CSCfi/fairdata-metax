# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from json import load
import logging

from django.conf import settings as django_settings
from django.http import Http404
from rest_framework import status
from rest_framework.decorators import list_route
from rest_framework.response import Response

from metax_api.api.rest.base.serializers import CatalogRecordSerializer
from metax_api.exceptions import Http400, Http403
from metax_api.models import CatalogRecord
from metax_api.models.catalog_record import DataciteDOIUpdate
from metax_api.api.rest.base.serializers import CatalogRecordSerializer
from metax_api.services.datacite_service import DataciteException, DataciteService, convert_cr_to_datacite_cr_json
from metax_api.utils import generate_doi_identifier, is_metax_generated_doi_identifier
from .common_rpc import CommonRPC

_logger = logging.getLogger(__name__)


class DatasetRPC(CommonRPC):

    serializer_class = CatalogRecordSerializer
    object = CatalogRecord

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

        if cr.preservation_identifier:
            # If cr preservation identifier already exists, make sure it also exists in Datacite
            DataciteDOIUpdate(cr, cr.preservation_identifier, 'update')()
        else:
            pref_id = cr.research_dataset['preferred_identifier']
            if is_metax_generated_doi_identifier(pref_id):
                # Copy metax generated doi identifier to preservation identifier. NOTE: This code should never be
                # reached since if pref id is a metax generated doi, it should have already been copied to preservation
                # identifier (ida dataset), but this is done just in case something has previously failed...
                _logger.warning("Reached a code block in dataset_rpc set_preservation_identifier method, which should"
                                " not be reached.")
                cr.preservation_identifier = pref_id
                action = 'update'
            else:
                # Generate a new DOI for the dataset. If pref id is a metax generated urn, use that urn's suffix as
                # the doi suffix. Otherwise generate completely new doi.
                if pref_id.startswith('urn:nbn:fi:att:'):
                    cr.preservation_identifier = generate_doi_identifier(pref_id[len('urn:nbn:fi:att:'):])
                else:
                    cr.preservation_identifier = generate_doi_identifier()
                action = 'create'

            self._save_and_publish_dataset(cr, action)

        return Response(cr.preservation_identifier)

    @list_route(methods=['post'], url_path="change_cumulative_state")
    def change_cumulative_state(self, request):
        identifier = request.query_params.get('identifier', False)
        state_value = request.query_params.get('cumulative_state', False)

        if not identifier:
            raise Http400('Query param \'identifier\' missing')
        if not state_value:
            raise Http400('Query param \'cumulative_state\' missing')

        try:
            cr = CatalogRecord.objects.get(identifier=identifier)
        except CatalogRecord.DoesNotExist:
            raise Http404

        if not cr.user_has_access(request):
            raise Http403('You do not have permissions to modify this dataset')

        cr.request = request

        if cr.change_cumulative_state(state_value):
            # new version is created
            return_status = status.HTTP_200_OK
            data = { 'new_version_created': self.get_serializer(cr).data['new_version_created'] }
        else:
            return_status = status.HTTP_204_NO_CONTENT
            data = None

        return Response(data=data, status=return_status)

    @list_route(methods=['post'], url_path="fix_deprecated")
    def fix_deprecated(self, request):
        if not request.query_params.get('identifier', False):
            raise Http400('Query param \'identifier\' missing. Please specify ?identifier=<catalog record identifier>')

        try:
            cr = CatalogRecord.objects.get(identifier=request.query_params['identifier'])
        except CatalogRecord.DoesNotExist:
            raise Http404

        if not cr.deprecated:
            raise Http400('Requested catalog record is not deprecated')

        cr.fix_deprecated()
        data = { 'new_version_created': self.get_serializer(cr).data['new_version_created'] }

        return Response(data=data, status=status.HTTP_200_OK)

    def _save_and_publish_dataset(self, cr, action):
        try:
            DataciteService().get_validated_datacite_json(
                convert_cr_to_datacite_cr_json(cr), True)
        except DataciteException as e:
            raise Http400(str(e))

        super(CatalogRecord, cr).save(update_fields=['preservation_identifier'])
        DataciteDOIUpdate(cr, cr.preservation_identifier, action)()
