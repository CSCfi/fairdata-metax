from datetime import datetime
from json import load as json_load
from os import path
import logging

from rest_framework import status
from rest_framework.serializers import ValidationError

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
        cls.update_common_info(request)

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
                except Exception:
                    results['failed'].append({ 'object': serializer.data, 'errors': serializer.errors })
                else:
                    serializer.save()
                    results['success'].append({ 'object': serializer.data, 'errors': serializer.errors })

            if results['success']:
                # if even one insert was successful, general status of the request is success
                http_status = status.HTTP_201_CREATED
            else:
                # only if all inserts have failed, return a general failure for the whole request
                http_status = status.HTTP_400_BAD_REQUEST

        else:
            serializer = serializer_class(data=request.data, **kwargs)
            serializer.is_valid(raise_exception=True)
            serializer.save()
            http_status = status.HTTP_201_CREATED
            results = serializer.data

        return results, http_status

    @staticmethod
    def get_json_schema(view_file_location, model_name):
        """
        Get the json schema file for model model_name.
        view_file is a __file__ variable
        """
        with open(path.dirname(view_file_location) + '/../schemas/json_schema_%s.json' % model_name, encoding='utf-8') as f:
            return json_load(f)

    @classmethod
    def update_bulk(cls, request, model_obj, serializer_class, **kwargs):
        """
        Note: ONLY list update

        Update objects to database from a list of dicts. Bulk update operations requires
        that the payload dict contains a field that can be used to identify the target row
        that is being updated, since a PUT or PATCH to i.e. /datasets or /files will not have
        an identifier in the url. PUT is going to have these identifying fields anyway, so
        this is only relevant to PATCH.

        params:
        request: the http request object
        model_obj: the model, used to search the instance being updated
        serializer_class: does the actual saving

        """
        if not isinstance(request.data, list):
            raise ValidationError('request.data is not a list')

        cls.update_common_info(request)

        results = { 'success': [], 'failed': []}

        for row in request.data:

            instance = cls._get_object_for_update(model_obj, row, results)

            if not instance:
                continue

            # note: kwargs will contain kw 'partial', when executed from PATCH
            serializer = serializer_class(instance, data=row, **kwargs)

            try:
                serializer.is_valid(raise_exception=True)
            except Exception:
                results['failed'].append({ 'object': serializer.data, 'errors': serializer.errors })
            else:
                serializer.save()
                results['success'].append({ 'object': serializer.data, 'errors': serializer.errors })

        http_status = cls._get_http_status_for_result(results, kwargs.get('partial', False))

        return results, http_status

    @staticmethod
    def update_common_info(request):
        """
        Update fields common for all tables and most actions:
        - last modified timestamp and user
        - created on timestamp and user
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

        if common_info:

            if isinstance(request.data, list):
                for row in request.data:
                    row.update(common_info)
            else:
                request.data.update(common_info)

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
    def _get_object_for_update(model_obj, row, results):
        """
        Find the target object being updated using a row from the request payload
        """
        try:
            return model_obj.objects.get(using_dict=row)
        except model_obj.DoesNotExist:
            results['failed'].append({ 'object': row, 'errors': { 'detail': ['object not found'] }})
        except ValidationError as e:
            results['failed'].append({ 'object': row, 'errors': { 'detail': e.detail } })
        return None
