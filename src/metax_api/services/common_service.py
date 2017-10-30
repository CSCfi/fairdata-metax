from datetime import datetime
from json import load as json_load

from rest_framework import status
from rest_framework.serializers import ValidationError

import logging
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
        common_info = { 'created_by_api': datetime.now() }

        results = None

        if isinstance(request.data, list):

            # dont fail the entire request if only some inserts fail.
            # successfully created rows are added to 'successful', and
            # failed inserts are added to 'failed', with a related error message.
            results = { 'success': [], 'failed': []}

            for row in request.data:

                serializer = serializer_class(data=row, **kwargs)

                try:
                    serializer.is_valid(raise_exception=True)
                except Exception as e:
                    cls._append_error(results, serializer, e)
                else:
                    serializer.save(**common_info)
                    results['success'].append({ 'object': serializer.data })

            if results['success']:
                # if even one insert was successful, general status of the request is success
                http_status = status.HTTP_201_CREATED
            else:
                # only if all inserts have failed, return a general failure for the whole request
                http_status = status.HTTP_400_BAD_REQUEST

        else:
            serializer = serializer_class(data=request.data, **kwargs)
            serializer.is_valid(raise_exception=True)
            serializer.save(**common_info)
            http_status = status.HTTP_201_CREATED
            results = serializer.data

        return results, http_status

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

        If the header If-Unmodified-Since is set, the field modified_by_api from the received
        data row will be compared to same field in the instance being updated, to see if the
        resource has been modified in the meantime. Only the presence of the header is checked
        in bulk update, its value does not matter. For PATCH, the presence of the field
        modified_by_api is an extra requirement in the received data, and an error will be
        returned if it is missing, when the header is set.

        params:
        request: the http request object
        model_obj: the model, used to search the instance being updated
        serializer_class: does the actual saving

        """
        if not isinstance(request.data, list):
            raise ValidationError('request.data is not a list')

        check_modified_since = kwargs['context']['view']._request_has_header('If-Unmodified-Since')
        common_info = { 'modified_by_api': datetime.now() }
        results = { 'success': [], 'failed': []}

        for row in request.data:

            instance = cls._get_object_for_update(model_obj, row, results, check_modified_since)

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

        http_status = cls._get_http_status_for_result(results, kwargs.get('partial', False))

        return results, http_status

    @staticmethod
    def update_common_info(request, return_only=False):
        """
        Update fields common for all tables and most actions:
        - last modified timestamp and user
        - created on timestamp and user

        For cases where request data is actually xml, it is useful to return the common info,
        so that its info can be used manually, instead of updating request.data here automatically.
        For that purpese, use the return_only flag.
        """
        user_id = request.user.id or None

        if not user_id:
            _logger.warning("User id not set; unknown user")

        method = request.stream and request.stream.method or False
        current_time = datetime.now()
        common_info = {}

        if method in ('PUT', 'PATCH', 'DELETE'):
            common_info.update({
                'modified_by_user_id': user_id,
                'modified_by_api': current_time
            })
        elif method == 'POST':
            common_info.update({
                'created_by_user_id': user_id,
                'created_by_api': current_time,
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
    def _get_http_status_for_result(results, partial_update):
        if results['success']:
            # if even one operation was successful, general status of the request is success
            if partial_update:
                # PATCH will contain full updated object
                return status.HTTP_200_OK
            else:
                # PUT
                if results['failed']:
                    # some were ok, but since some failed, cant return 204 no_content
                    return status.HTTP_200_OK
                else:
                    # to stay consistent with a single PUT operation, fully successful update
                    # will return no data to the client
                    return status.HTTP_204_NO_CONTENT
        else:
            # only if all rows have failed, return a general failure for the whole request
            return status.HTTP_400_BAD_REQUEST

    @staticmethod
    def _get_object_for_update(model_obj, row, results, check_modified_since):
        """
        Find the target object being updated using a row from the request payload.

        parameters:
        model_obj: the model object used to search the db
        row: the payload from the request
        results: the result-list that will be returned from the api
        check_modified_since: retrieved object should compare its modified_by_api timestamp
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

        if instance and check_modified_since:
            if 'modified_by_api' not in row:
                results['failed'].append({
                    'object': row,
                    'errors': {
                        'detail': ['Field modified_by_api is required when the header If-Unmodified-Since is set']
                    }
                })
            elif instance.modified_since(row['modified_by_api']):
                results['failed'].append({ 'object': row, 'errors': { 'detail': ['Resource has been modified'] } })
            else:
                # good case - all is good
                pass

        return instance
