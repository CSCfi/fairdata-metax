from json import dump, load
import urllib.parse
import logging

from django.http import Http404
from rest_framework import status
from rest_framework.decorators import detail_route, list_route
from rest_framework.response import Response
import yaml

from metax_api.exceptions import Http403
from metax_api.models import CatalogRecord, Common, DataCatalog, File, Directory
from metax_api.renderers import XMLRenderer
from metax_api.services import CatalogRecordService as CRS, CommonService as CS
from metax_api.utils import RabbitMQ
from .common_view import CommonViewSet
from ..serializers import CatalogRecordSerializer, FileSerializer

_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug


class DatasetViewSet(CommonViewSet):

    authentication_classes = ()
    permission_classes = ()

    # note: override get_queryset() to get more control
    queryset = CatalogRecord.objects.select_related('data_catalog', 'contract').all()
    queryset_unfiltered = CatalogRecord.objects_unfiltered.select_related('data_catalog', 'contract').all()

    serializer_class = CatalogRecordSerializer
    object = CatalogRecord

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

    def get_queryset(self):

        additional_filters = {}

        if hasattr(self, 'queryset_search_params'):
            additional_filters.update(**self.queryset_search_params)

        CS.set_if_modified_since_filter(self.request, additional_filters)

        return super(DatasetViewSet, self).get_queryset().filter(**additional_filters)

    def retrieve(self, request, *args, **kwargs):
        self.queryset_search_params = {}
        CS.set_if_modified_since_filter(self.request, self.queryset_search_params)

        res = super(DatasetViewSet, self).retrieve(request, *args, **kwargs)

        if 'dataset_format' in request.query_params:
            res.data = CRS.transform_datasets_to_format(res.data, request.query_params['dataset_format'])
            request.accepted_renderer = XMLRenderer()
        elif 'file_details' in request.query_params:
            CRS.populate_file_details(res.data)

        return res

    def _retrieve_by_preferred_identifier(self, request, *args, **kwargs):
        lookup_value = urllib.parse.unquote(request.query_params['preferred_identifier'])
        self.request.GET._mutable = True
        self.request.query_params['no_pagination'] = 'true'
        self.request.GET._mutable = False # hehe

        # search by preferred_identifier only for GET requests, while preferring:
        # - hits from att catalogs (assumed to be first created. improve logic if situation changes)
        # - first created (the first harvested occurrence, probably)
        # note: cant use get_object(), because get_object() will throw an error if there are multiple results
        if self.request.method == 'GET':
            obj = self.get_queryset().filter(research_dataset__contains={'preferred_identifier': lookup_value}) \
                .order_by('data_catalog_id', 'date_created').first()
            if obj:
                serializer = self.get_serializer(obj)
                return Response(data=serializer.data, status=status.HTTP_200_OK)
        raise Http404

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

    def update(self, request, *args, **kwargs):
        res = super(DatasetViewSet, self).update(request, *args, **kwargs)
        CRS.publish_updated_datasets(res)
        return res

    def update_bulk(self, request, *args, **kwargs):
        res = super(DatasetViewSet, self).update_bulk(request, *args, **kwargs)
        CRS.publish_updated_datasets(res)
        return res

    def partial_update(self, request, *args, **kwargs):
        res = super(DatasetViewSet, self).partial_update(request, *args, **kwargs)
        CRS.publish_updated_datasets(res)
        return res

    def partial_update_bulk(self, request, *args, **kwargs):
        res = super(DatasetViewSet, self).partial_update_bulk(request, *args, **kwargs)
        CRS.publish_updated_datasets(res)
        return res

    def destroy(self, request, *args, **kwargs):
        res = super(DatasetViewSet, self).destroy(request, *args, **kwargs)
        if res.status_code == status.HTTP_204_NO_CONTENT:
            removed_object = self._get_removed_dataset()
            rabbitmq = RabbitMQ()
            rabbitmq.publish({ 'identifier': removed_object.identifier },
                routing_key='delete', exchange='datasets')
        return res

    def create(self, request, *args, **kwargs):
        res = super(DatasetViewSet, self).create(request, *args, **kwargs)

        if res.status_code == status.HTTP_201_CREATED:
            if 'success' in res.data:
                # was bulk create
                message = [ r['object'] for r in res.data['success'] ]
            else:
                message = res.data
            rabbitmq = RabbitMQ()
            rabbitmq.publish(message, routing_key='create', exchange='datasets')

        return res

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

        return Response(data=research_dataset, status=status.HTTP_200_OK)

    @detail_route(methods=['get'], url_path="metadata_versions")
    def metadata_versions_list(self, request, pk=None):
        """
        List all research_dataset version associated with this dataset.
        """
        cr = self.get_object()
        entries = cr.get_metadata_version_listing()
        return Response(data=entries, status=status.HTTP_200_OK)

    @detail_route(methods=['get'], url_path="files")
    def files_get(self, request, pk=None):
        """
        Get files associated to this dataset. Can be used to retrieve a list of only
        deleted files by providing the query parameter removed_files=true.
        """
        params = {}
        manager = 'objects'
        # TODO: This applies only to IDA files, not remote resources.
        # TODO: Should this apply also to remote resources?
        catalog_record = self.get_object()

        if CS.get_boolean_query_param(request, 'removed_files'):
            params['removed'] = True
            manager = 'objects_unfiltered'

        files = [ FileSerializer(f).data for f in catalog_record.files(manager=manager).filter(**params) ]

        return Response(data=files, status=status.HTTP_200_OK)

    @detail_route(methods=['post'], url_path="proposetopas")
    def propose_to_pas(self, request, pk=None):
        CRS.propose_to_pas(request, self.get_object())
        return Response(data={}, status=status.HTTP_204_NO_CONTENT)

    @list_route(methods=['get'], url_path="identifiers")
    def get_all_identifiers(self, request):
        self.queryset_search_params = CRS.get_queryset_search_params(request)
        q = self.get_queryset().values('identifier')
        identifiers = [item['identifier'] for item in q]
        return Response(identifiers)

    @list_route(methods=['get'], url_path="metadata_version_identifiers")
    def get_all_metadata_version_identifiers(self, request):
        # todo probably remove at some point
        self.queryset_search_params = CRS.get_queryset_search_params(request)
        q = self.get_queryset().values('research_dataset')
        identifiers = [item['research_dataset']['metadata_version_identifier'] for item in q]
        return Response(identifiers)

    @list_route(methods=['get'], url_path="unique_preferred_identifiers")
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
        try:
            # todo probably remove this at some point. for now, doesnt do harm and does not instantly break
            # services using this...
            return super(DatasetViewSet, self).get_object(
                search_params={ 'research_dataset__contains': {'metadata_version_identifier': lookup_value} })
        except Http404:
            pass

        return super(DatasetViewSet, self).get_object(search_params={ 'identifier': lookup_value })

    def _get_removed_dataset(self):
        """
        Get dataset from self.object.objects_unfiltered so that removed objects can be found
        """
        lookup_value = self.kwargs.get('pk')
        different_fields = [
            {},
            { 'search_params': { 'identifier': lookup_value } },
            { 'search_params': { 'research_dataset__contains': {'metadata_version_identifier': lookup_value }} },
            { 'search_params': { 'research_dataset__contains': {'preferred_identifier': lookup_value }} },
            { 'search_params': { 'research_dataset__contains':
                                {'other_identifier': [{'local_identifier': lookup_value }]}}},
        ]
        for params in different_fields:
            try:
                return self.get_removed_object(**params)
            except Http404:
                pass
        raise Http404

    @detail_route(methods=['get'], url_path="redis")
    def redis_test(self, request, pk=None): # pragma: no cover
        try:
            cached = self.cache.get('cr-1211%s' % pk)
        except:
            d('redis: could not connect during read')
            cached = None
            raise

        if cached:
            d('found in cache, returning')
            return Response(data=cached, status=status.HTTP_200_OK)

        data = self.get_serializer(CatalogRecord.objects.get(pk=1)).data

        try:
            self.cache.set('cr-1211%s' % pk, data)
        except:
            d('redis: could not connect during write')
            raise

        return Response(data=data, status=status.HTTP_200_OK)

    @detail_route(methods=['get'], url_path="rabbitmq")
    def rabbitmq_test(self, request, pk=None): # pragma: no cover
        rabbitmq = RabbitMQ()
        rabbitmq.publish({ 'msg': 'hello create'}, routing_key='create', exchange='datasets')
        rabbitmq.publish({ 'msg': 'hello update'}, routing_key='update', exchange='datasets')
        return Response(data={}, status=status.HTTP_200_OK)

    @list_route(methods=['get'], url_path="update_cr_total_ida_byte_sizes")  # pragma: no cover
    def update_cr_total_ida_byte_sizes(self, request):
        """
        Meant only for updating test data having wrong total ida byte size

        :param request:
        :return:
        """
        # Get all IDs for ida data catalogs
        ida_catalog_ids = []
        for dc in DataCatalog.objects.filter(catalog_json__contains={'research_dataset_schema': 'ida'}):
            ida_catalog_ids.append(dc.id)

        # Update IDA CR total_ida_byte_size field value without creating a new version
        # Skip CatalogRecord save since it prohibits changing the value of total_ida_byte_size
        for cr in CatalogRecord.objects.filter(data_catalog_id__in=ida_catalog_ids):
            cr.research_dataset['total_ida_byte_size'] = sum(f.byte_size for f in cr.files.all())
            cr.preserve_version = True
            super(Common, cr).save()

        return Response(data={}, status=status.HTTP_200_OK)

    @list_route(methods=['post'], url_path="flush_password")
    def flush_password(self, request):
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

    @list_route(methods=['post'], url_path="flush")
    def flush_records(self, request):
        """
        Delete all catalog records and files. Requires a password
        """
        with open('/home/metax-user/app_config') as app_config:
            app_config_dict = yaml.load(app_config)
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
