import logging
from json import load as json_load

from django.utils import timezone
from rest_framework import status
from rest_framework.serializers import ValidationError

from metax_api.exceptions import Http400, Http412
from metax_api.utils import parse_timestamp_string_to_tz_aware_datetime, get_tz_aware_now_without_micros

_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug


class CommonService():

    @staticmethod
    def is_primary_key(received_lookup_value):
        if not received_lookup_value:
            return False
        try:
            int(received_lookup_value)
        except ValueError:
            return False
        else:
            return True

    @classmethod
    def create_bulk(cls, request, serializer_class, **kwargs):
        """
        Note: BOTH single and list create

        Create objects to database from a list of dicts or a single dict, and return a list
        of created objects or a single object.

        params:
        request: the http request object
        serializer_class: does the actual saving, knows what kind of object is in question
        """
        common_info = cls.update_common_info(request, return_only=True)

        results = None

        if isinstance(request.data, list):

            if len(request.data) == 0:
                raise ValidationError(['the received object list is empty'])

            # dont fail the entire request if only some inserts fail.
            # successfully created rows are added to 'successful', and
            # failed inserts are added to 'failed', with a related error message.
            results = { 'success': [], 'failed': []}

            cls._create_bulk(common_info, request.data, results, serializer_class, **kwargs)

            if results['success']:
                # if even one insert was successful, general status of the request is success
                http_status = status.HTTP_201_CREATED
            else:
                # only if all inserts have failed, return a general failure for the whole request
                http_status = status.HTTP_400_BAD_REQUEST

        else:
            results, http_status = cls._create_single(common_info, request.data, serializer_class, **kwargs)

        return results, http_status

    @classmethod
    def _create_single(cls, common_info, initial_data, serializer_class, **kwargs):
        """
        Extracted into its own method so it may be inherited.
        """
        serializer = serializer_class(data=initial_data, **kwargs)
        serializer.is_valid(raise_exception=True)
        serializer.save(**common_info)
        return serializer.data, status.HTTP_201_CREATED

    @classmethod
    def _create_bulk(cls, common_info, initial_data_list, results, serializer_class, **kwargs):
        """
        The actual part where the list is iterated and objects validated, and created.
        """
        for row in initial_data_list:

            serializer = serializer_class(data=row, **kwargs)

            try:
                serializer.is_valid(raise_exception=True)
            except Exception as e:
                cls._append_error(results, serializer, e)
            else:
                serializer.save(**common_info)
                results['success'].append({ 'object': serializer.data })

    @staticmethod
    def get_json_schema(schema_folder_path, model_name, data_catalog_prefix=False):
        """
        Get the json schema file for model model_name.
        schema_folder_path is a the path to the folder where schemas are located.
        It can be given due to different api versions have different schema paths.
        For datasets, a data catalog prefix can be given, in which case it will
        be the prefix for the schema file name.
        """
        schema_name = ''

        if model_name == 'dataset':
            if data_catalog_prefix:
                schema_name = data_catalog_prefix
            else:
                schema_name = 'att'

            schema_name += '_'

        schema_name += '%s_schema.json' % model_name

        try:
            with open('%s/%s' % (schema_folder_path, schema_name), encoding='utf-8') as f:
                return json_load(f)
        except IOError as e:
            if model_name != 'dataset':
                # only datasets have a default schema
                raise
            _logger.warning(e)
            with open('%s/att_dataset_schema.json' % schema_folder_path, encoding='utf-8') as f:
                return json_load(f)

    @classmethod
    def update_bulk(cls, request, model_obj, serializer_class, **kwargs):
        """
        Note: ONLY list update (PUT and PATCH). Single update uses std rest_framework process.

        Update objects to database from a list of dicts. Bulk update operation requires
        that the payload dict contains a field that can be used to identify the target row
        that is being updated, since a PUT or PATCH to i.e. /datasets or /files will not have
        an identifier in the url.

        If the header If-Unmodified-Since is set, the field date_modified from the received
        data row will be compared to same field in the instance being updated, to see if the
        resource has been modified in the meantime. Only the presence of the header is checked
        in bulk update, its value does not matter. For PATCH, the presence of the field
        date_modified is an extra requirement in the received data, and an error will be
        returned if it is missing, when the header is set.

        params:
        request: the http request object
        model_obj: the model, used to search the instance being updated
        serializer_class: does the actual saving

        """
        if not isinstance(request.data, list):
            raise ValidationError('request.data is not a list')

        common_info = cls.update_common_info(request, return_only=True)
        results = { 'success': [], 'failed': []}

        for row in request.data:

            instance = cls._get_object_for_update(model_obj, row, results,
                                                  cls._request_has_header(request, 'HTTP_IF_UNMODIFIED_SINCE'))

            if not instance:
                continue

            # note: kwargs will contain kw 'partial', when executed from PATCH
            serializer = serializer_class(instance, data=row, **kwargs)

            try:
                serializer.is_valid(raise_exception=True)
            except Exception as e:
                cls._append_error(results, serializer, e)
            else:
                serializer.save(**common_info)
                results['success'].append({ 'object': serializer.data })

        # if even one operation was successful, general status of the request is success
        if len(results.get('success', [])) > 0:
            http_status = status.HTTP_200_OK
        else:
            http_status = status.HTTP_400_BAD_REQUEST

        return results, http_status

    @staticmethod
    def update_common_info(request, return_only=False):
        """
        Update fields common for all tables and most actions:
        - last modified timestamp and service name
        - created on timestamp and service name

        For cases where request data is actually xml, or bulk update/create, it is useful to
        return the common info, so that its info can be used manually, instead of updating
        request.data here automatically. For that purpose, use the return_only flag.
        """
        service_name = request.user.username or None

        if not service_name: # pragma: no cover
            # should never happen: update_common_info is executed only on update operations,
            # which requires authorization, which should put the username into the request obj.
            ValidationError({
                'detail': 'request.user.username not set; unknown service. '
                'how did you get here without passing authorization...?'
            })

        method = request.stream and request.stream.method or False
        current_time = get_tz_aware_now_without_micros()
        common_info = {}

        if method in ('PUT', 'PATCH', 'DELETE'):
            common_info.update({
                'service_modified': service_name,
                'date_modified': current_time
            })
        elif method == 'POST':
            common_info.update({
                'service_created': service_name,
                'date_created': current_time,
            })
        else:
            pass

        if return_only:
            return common_info
        else:
            request.data.update(common_info)

    @staticmethod
    def _append_error(results, serializer, error):
        """
        Handle the error and append it to the results list in list create or update operations
        Sometimes the error is not a field validation error, but an actual programming error
        resulting in a crash, in which case serializer.errors is not accessible. The error
        is returned as str(error) to from the api anyway to make it easier to spot, but it
        is still a crash, and should be fixed.
        """
        try:
            results['failed'].append({ 'object': serializer.initial_data, 'errors': serializer.errors })
        except AssertionError:
            _logger.exception(
                'Looks like serializer.is_valid() tripped - could not access serializer.errors. '
                'Returning str(e) instead. THIS SHOULD BE FIXED. YES, IM TALKING TO YOU'
            )
            # note that all cases where this happens should be fixed - this is a programming error.
            # str(e) might show dicts or lists as strings, which would look silly to receiving
            # humans
            results['failed'].append({ 'object': serializer.initial_data, 'errors': str(error) })

    @staticmethod
    def _get_object_for_update(model_obj, row, results, check_unmodified_since):
        """
        Find the target object being updated using a row from the request payload.

        parameters:
        model_obj: the model object used to search the db
        row: the payload from the request
        results: the result-list that will be returned from the api
        check_unmodified_since: retrieved object should compare its date_modified timestamp
            to the corresponding field in the received row. this simulates the use of the
            if-unmodified-since header that is used for single updates.
        """
        instance = None
        try:
            instance = model_obj.objects.get(using_dict=row)
        except model_obj.DoesNotExist:
            results['failed'].append({ 'object': row, 'errors': { 'detail': ['object not found'] }})
        except ValidationError as e:
            results['failed'].append({ 'object': row, 'errors': { 'detail': e.detail } })

        if instance and check_unmodified_since:
            if 'date_modified' not in row:
                results['failed'].append({
                    'object': row,
                    'errors': {
                        'detail': ['Field date_modified is required when the header If-Unmodified-Since is set']
                    }
                })
            elif instance.modified_since(row['date_modified']):
                results['failed'].append({ 'object': row, 'errors': { 'detail': ['Resource has been modified'] } })
            else:
                # good case - all is good
                pass

        return instance

    @classmethod
    def _validate_and_get_if_unmodified_since_header_as_tz_aware_datetime(cls, request):
        try:
            return cls._validate_http_date_header(request, 'HTTP_IF_UNMODIFIED_SINCE')
        except:
            raise Http400('Bad If-Unmodified-Since header')

    @classmethod
    def validate_and_get_if_modified_since_header_as_tz_aware_datetime(cls, request):
        try:
            return cls._validate_http_date_header(request, 'HTTP_IF_MODIFIED_SINCE')
        except:
            raise Http400('Bad If-Modified-Since header')

    @staticmethod
    def _validate_http_date_header(request, header_name):
        timestamp = request.META.get(header_name, '')
        # According to RFC 7232, Http date should always be expressed in 'GMT'. Forcing its use makes this explicit
        if not timestamp.endswith('GMT'):
            raise Exception
        return parse_timestamp_string_to_tz_aware_datetime(timestamp)

    @staticmethod
    def _request_has_header(request, header_name):
        return header_name in request.META

    @staticmethod
    def _request_is_write_operation(request):
        return request.method in ('POST', 'PUT', 'PATCH', 'DELETE')

    @classmethod
    def check_if_unmodified_since(cls, request, obj):
        if cls._request_is_write_operation(request) and \
                cls._request_has_header(request, 'HTTP_IF_UNMODIFIED_SINCE'):

            header_timestamp = cls._validate_and_get_if_unmodified_since_header_as_tz_aware_datetime(request)
            if obj.modified_since(header_timestamp):
                raise Http412('Resource has been modified since {0} (timezone: {1})'.format(
                    str(header_timestamp), timezone.get_default_timezone_name()))

    @classmethod
    def set_if_modified_since_filter(cls, request, filter_obj):
        """
        Evaluate If-Modified-Since http header only on read operations.
        Filter items whose date_modified field timestamp value is greater than the header value.
        This method updates given filter object.

        :param request:
        :param filter_obj
        :return:
        """
        if not cls._request_is_write_operation(request) and cls._request_has_header(request, 'HTTP_IF_MODIFIED_SINCE'):
            filter_obj.update({
                'date_modified__gt': cls.validate_and_get_if_modified_since_header_as_tz_aware_datetime(request)
            })
