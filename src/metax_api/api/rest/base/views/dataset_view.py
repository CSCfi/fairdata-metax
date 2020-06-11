# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from json import dump, load
import logging

from django.http import Http404
from rest_framework import status
from rest_framework.decorators import action
from rest_framework.response import Response
import yaml

from metax_api.exceptions import Http403, Http400
from metax_api.models import CatalogRecord, Common, DataCatalog, File, Directory
from metax_api.renderers import XMLRenderer
from metax_api.services import CatalogRecordService as CRS, CommonService as CS, RabbitMQService as rabbitmq
from .common_view import CommonViewSet
from ..serializers import CatalogRecordSerializer, LightFileSerializer

_logger = logging.getLogger(__name__)


class DatasetViewSet(CommonViewSet):

    serializer_class = CatalogRecordSerializer
    object = CatalogRecord
    select_related = ['data_catalog', 'contract']

    lookup_field = 'pk'

    def __init__(self, *args, **kwargs):
        # As opposed to other views, do not set json schema here
        # It is done in the serializer
        super(DatasetViewSet, self).__init__(*args, **kwargs)

    def get_object(self):
        try:
            return super(DatasetViewSet, self).get_object()
        except Http404:
            if CRS.is_primary_key(self.kwargs.get(self.lookup_field, False)):
                # fail on pk search is clear...
                raise

        return self._search_using_dataset_identifiers()

    def retrieve(self, request, *args, **kwargs):
        from metax_api.services.datacite_service import DataciteException
        self.queryset_search_params = {}
        CS.set_if_modified_since_filter(self.request, self.queryset_search_params)

        res = super(DatasetViewSet, self).retrieve(request, *args, **kwargs)

        if 'dataset_format' in request.query_params:
            try:
                res.data = CRS.transform_datasets_to_format(
                    res.data, request.query_params['dataset_format'], request=request)
            except DataciteException as e:
                raise Http400(str(e))
            request.accepted_renderer = XMLRenderer()

        return res

    def _retrieve_by_preferred_identifier(self, request, *args, **kwargs):
        lookup_value = request.query_params['preferred_identifier']
        self.kwargs[self.lookup_field] = lookup_value
        self.request.GET._mutable = True
        self.request.query_params['no_pagination'] = 'true'
        self.request.GET._mutable = False # hehe
        return self.retrieve(request, *args, **kwargs)

    def list(self, request, *args, **kwargs):
        # best to specify a variable for parameters intended for filtering purposes in get_queryset(),
        # because other api's may use query parameters of the same name, which can
        # mess up filtering if get_queryset() uses request.query_parameters directly.
        self.queryset_search_params = CRS.get_queryset_search_params(request)

        if 'preferred_identifier' in request.query_params:
            return self._retrieve_by_preferred_identifier(request, *args, **kwargs)

        # actually a nested url /datasets/id/metadata_versions/id. this is probably a very screwed up way to do this...
        if 'identifier' in kwargs and 'metadata_version_identifier' in kwargs:
            return self._metadata_version_get(request, *args, **kwargs)

        return super(DatasetViewSet, self).list(request, *args, **kwargs)

    def _metadata_version_get(self, request, *args, **kwargs):
        """
        Get single research_dataset version.
        """
        assert 'identifier' in kwargs
        assert 'metadata_version_identifier' in kwargs

        # get_object() expects the following...
        self.kwargs[self.lookup_field] = kwargs['identifier']

        cr = self.get_object()
        search_params = { 'catalog_record_id': cr.id }

        if CRS.is_primary_key(kwargs['metadata_version_identifier']):
            search_params['id'] = kwargs['metadata_version_identifier']
        else:
            search_params['metadata_version_identifier'] = kwargs['metadata_version_identifier']

        try:
            research_dataset = cr.research_dataset_versions.get(**search_params).research_dataset
        except:
            raise Http404

        if not cr.user_is_privileged(request):
            # normally when retrieving a record and its research_dataset field,
            # the request goes through the CatalogRecordSerializer, where sensitive
            # fields are automatically stripped. This is a case where it's not
            # possible to use the serializer, since an older metadata version of a ds
            # is not stored as part of the cr, but in the table ResearchDatasetVersion.
            # therefore, perform this checking and stripping separately here.
            research_dataset = CRS.check_and_remove_metadata_based_on_access_type(
                CRS.remove_contact_info_metadata(research_dataset))

        return Response(data=research_dataset, status=status.HTTP_200_OK)

    @action(detail=True, methods=['get'], name="metadata_versions")
    def metadata_versions_list(self, request, pk=None):
        """
        List all research_dataset version associated with this dataset.
        """
        cr = self.get_object()
        entries = cr.get_metadata_version_listing()
        return Response(data=entries, status=status.HTTP_200_OK)

    @action(detail=True, methods=['get'], name="files")
    def files_list(self, request, pk=None):
        """
        Get files associated to this dataset. Can be used to retrieve a list of only
        deleted files by providing the query parameter removed_files=true.
        """
        params = {}
        manager = 'objects'
        # TODO: This applies only to IDA files, not remote resources.
        # TODO: Should this apply also to remote resources?
        cr = self.get_object()

        if not cr.authorized_to_see_catalog_record_files(request):
            raise Http403({
                'detail': ['You do not have permission to see this information because the dataset access type '
                           'is not open and you are not the owner of the catalog record.']
            })

        if CS.get_boolean_query_param(request, 'removed_files'):
            params['removed'] = True
            manager = 'objects_unfiltered'

        file_fields = []
        if 'file_fields' in request.query_params:
            file_fields = request.query_params['file_fields'].split(',')

        file_fields = LightFileSerializer.ls_field_list(file_fields)
        queryset = cr.files(manager=manager).filter(**params).values(*file_fields)
        files = LightFileSerializer.serialize(queryset)

        return Response(data=files, status=status.HTTP_200_OK)

    @action(detail=False, methods=['get'], name="identifiers")
    def get_all_identifiers(self, request):
        self.queryset_search_params = CRS.get_queryset_search_params(request)
        q = self.get_queryset().values('identifier')
        identifiers = [item['identifier'] for item in q]
        return Response(identifiers)

    @action(detail=False, methods=['get'], name="metadata_version_identifiers")
    def get_all_metadata_version_identifiers(self, request):
        # todo probably remove at some point
        self.queryset_search_params = CRS.get_queryset_search_params(request)
        q = self.get_queryset().values('research_dataset')
        identifiers = [item['research_dataset']['metadata_version_identifier'] for item in q]
        return Response(identifiers)

    @action(detail=False, methods=['get'], name="unique_preferred_identifiers")
    def get_all_unique_preferred_identifiers(self, request):
        self.queryset_search_params = CRS.get_queryset_search_params(request)

        if CS.get_boolean_query_param(request, 'latest'):
            queryset = self.get_queryset().filter(next_dataset_version_id=None).values('research_dataset')
        else:
            queryset = self.get_queryset().values('research_dataset')

        unique_pref_ids = list(set(item['research_dataset']['preferred_identifier'] for item in queryset))
        return Response(unique_pref_ids)

    def _search_using_dataset_identifiers(self):
        """
        Search by lookup value from metadata_version_identifier and preferred_identifier fields. preferred_identifier
        searched only with GET requests. If query contains parameter 'preferred_identifier', do not lookup
        using identifier or metadata_version_identifier
        """
        lookup_value = self.kwargs.get(self.lookup_field, False)

        if 'preferred_identifier' in self.request.query_params and self.request.method == 'GET':
            # search by preferred_identifier only for GET requests, while preferring:
            # - hits from att catalogs (assumed to be first created. improve logic if situation changes)
            # - first created (the first harvested occurrence, probably)
            # note: cant use get_object(), because get_object() will throw an error if there are multiple results
            obj = self.get_queryset().filter(research_dataset__contains={'preferred_identifier': lookup_value}) \
                .order_by('data_catalog_id', 'date_created').first()
            if obj:
                self.check_object_permissions(self.request, obj)
                return obj
            raise Http404

        try:
            # todo probably remove this at some point. for now, doesnt do harm and does not instantly break
            # services using this...
            return super(DatasetViewSet, self).get_object(
                search_params={ 'research_dataset__contains': {'metadata_version_identifier': lookup_value} })
        except Http404:
            pass

        return super(DatasetViewSet, self).get_object(search_params={ 'identifier': lookup_value })

    @action(detail=True, methods=['get'], name="redis")
    def redis_test(self, request, pk=None): # pragma: no cover
        if request.user.username != 'metax':
            raise Http403()
        try:
            cached = self.cache.get('cr-1211%s' % pk)
        except:
            _logger.debug('redis: could not connect during read')
            cached = None
            raise

        if cached:
            _logger.debug('found in cache, returning')
            return Response(data=cached, status=status.HTTP_200_OK)

        data = self.get_serializer(CatalogRecord.objects.get(pk=1)).data

        try:
            self.cache.set('cr-1211%s' % pk, data)
        except:
            _logger.debug('redis: could not connect during write')
            raise

        return Response(data=data, status=status.HTTP_200_OK)

    @action(detail=True, methods=['get'], name="rabbitmq")
    def rabbitmq_test(self, request, pk=None): # pragma: no cover
        if request.user.username != 'metax':
            raise Http403()
        rabbitmq.publish({ 'msg': 'hello create'}, routing_key='create', exchange='datasets')
        rabbitmq.publish({ 'msg': 'hello update'}, routing_key='update', exchange='datasets')
        return Response(data={}, status=status.HTTP_200_OK)

    @action(detail=False, methods=['get'], name="update_cr_total_files_byte_sizes")  # pragma: no cover
    def update_cr_total_files_byte_sizes(self, request):
        """
        Meant only for updating test data having wrong total ida byte size

        :param request:
        :return:
        """
        if request.user.username != 'metax':
            raise Http403()
        # Get all IDs for ida data catalogs
        ida_catalog_ids = []
        for dc in DataCatalog.objects.filter(catalog_json__contains={'research_dataset_schema': 'ida'}):
            ida_catalog_ids.append(dc.id)

        # Update IDA CR total_files_byte_size field value without creating a new version
        # Skip CatalogRecord save since it prohibits changing the value of total_files_byte_size
        for cr in CatalogRecord.objects.filter(data_catalog_id__in=ida_catalog_ids):
            cr.research_dataset['total_files_byte_size'] = sum(f.byte_size for f in cr.files.all())
            cr.preserve_version = True
            super(Common, cr).save()

        return Response(data={}, status=status.HTTP_200_OK)

    @action(detail=False, methods=['get'], name="update_cr_directory_browsing_data")  # pragma: no cover
    def update_cr_directory_browsing_data(self, request):
        """
        Meant only for updating test data: Updates cr field _directory_data with cr specific
        directory data used during file browsing.

        :param request:
        :return:
        """
        if request.user.username != 'metax':
            raise Http403()

        if 'id' in request.query_params:
            # in order to update one record only, use query param ?id=integer. useful for testcases
            records = CatalogRecord.objects.filter(pk=request.query_params['id']).only('id')
        else:
            records = CatalogRecord.objects.filter(data_catalog__catalog_json__research_dataset_schema='ida') \
                .only('id')

        from time import time

        for cr in records:
            start = time()
            cr.calculate_directory_byte_sizes_and_file_counts()
            end = time()
            file_count = cr.files.all().count()
            dir_count = cr.files.all().distinct('parent_directory_id').count()
            _logger.info('record %d took %.2f seconds. record has %d files in approximately %d directories.' %
                (cr.id, end - start, file_count, dir_count))

        return Response(data={}, status=status.HTTP_200_OK)

    @action(detail=False, methods=['post'], name="flush_password")
    def flush_password(self, request): # pragma: no cover
        """
        Set a password for flush api
        """
        if request.user.username == 'metax':
            with open('/home/metax-user/flush_password', 'w') as f:
                dump(request.data, f)
        else:
            raise Http403
        _logger.debug('FLUSH password set')
        return Response(data=None, status=status.HTTP_204_NO_CONTENT)

    @action(detail=False, methods=['post'], name="flush")
    def flush_records(self, request): # pragma: no cover
        """
        Delete all catalog records and files. Requires a password
        """
        with open('/home/metax-user/app_config') as app_config:
            app_config_dict = yaml.load(app_config, Loader=yaml.FullLoader)
            for host in app_config_dict['ALLOWED_HOSTS']:
                if 'metax.csc.local' in host or 'metax-test' in host or 'metax-stable' in host:

                    if 'password' in request.data:
                        with open('/home/metax-user/flush_password', 'rb') as f:
                            if request.data['password'] == load(f)['password']:
                                break
                    raise Http403
            else:
                raise Http403

        for f in File.objects_unfiltered.all():
            super(Common, f).delete()

        for dr in Directory.objects_unfiltered.all():
            super(Common, dr).delete()

        for f in CatalogRecord.objects_unfiltered.all():
            super(Common, f).delete()

        _logger.debug('FLUSH called by %s' % request.user.username)

        return Response(data=None, status=status.HTTP_204_NO_CONTENT)
