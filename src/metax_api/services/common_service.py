# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging
from json import load as json_load
from typing import List

from django.db.models import Q
from django.utils import timezone
from rest_framework import status
from rest_framework.request import Request
from rest_framework.serializers import ValidationError

from metax_api.exceptions import Http400, Http412
from metax_api.models import CatalogRecord as cr, File
from metax_api.utils import (
    get_tz_aware_now_without_micros,
    parse_timestamp_string_to_tz_aware_datetime,
)

_logger = logging.getLogger(__name__)


class CommonService:
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

    @staticmethod
    def get_boolean_query_param(request, param_name):
        """
        Helper method to also check for values, instead of only presence of a boolean parameter,
        such as ?recursive=true/false instead of only ?recursive, which can only evaluate to true.
        """
        if isinstance(request, Request):
            # DRF request
            value = request.query_params.get(param_name, None)
        else:
            # if method is called before a view's dispatch() method, the only request available
            # is a low level WSGIRequest.
            for query_param in (val for val in request.environ["QUERY_STRING"].split("&")):
                try:
                    param, val = query_param.split("=")
                except ValueError:
                    # probably error was 'cant unpack tuple, not enough values' -> was a
                    # param without value specified, such as ?recursive, instead of ?recursive=true.
                    # if flag is specified without value, default value is to be considered True.
                    param, val = query_param, "true"

                if param == param_name:
                    value = val
                    break
            else:
                return False

        if value in ("", "true"):
            # flag was specified without value (?recursive), or with value (?recursive=true)
            return True
        elif value in (None, "false"):
            # flag was not present, or its value was ?recursive=false
            return False
        else:
            raise ValidationError(
                {param_name: ["boolean value must be true or false. received value was %s" % value]}
            )

    @staticmethod
    def get_list_query_param(request, param_name):
        """
        Retrieve an optional comma-separated list of string values from a query param.

        Note: Returns set, not a list!

        Note: In case method is needed before dispatch() is called, see what is done in
        method get_boolean_query_param().
        """
        value = request.query_params.get(param_name, None)

        if value is None:
            return None
        elif value in ("", ","):
            return set()

        values_set = set(v.strip() for v in value.split(","))

        if values_set:
            return values_set

        return set()

    @staticmethod
    def has_research_agent_query_params(request):
        """
        Defines if request has query parameters for creator, curator, publisher or rights_holder.
        Queries are for example 'creator_person' or 'publisher_organization'
        Returns boolean
        """
        fields = ["creator", "curator", "publisher", "rights_holder"]
        types = ["organization", "person"]
        for field in fields:
            if any(request.query_params.get(f"{field}_{type}") for type in types):
                return True

        return False

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
        kwargs["context"]["request"] = request

        results = None

        if not request.data:
            raise Http400("Request body is required")

        if isinstance(request.data, list):

            if len(request.data) == 0:
                raise ValidationError(["the received object list is empty"])

            # dont fail the entire request if only some inserts fail.
            # successfully created rows are added to 'successful', and
            # failed inserts are added to 'failed', with a related error message.
            results = {"success": [], "failed": []}

            cls._create_bulk(common_info, request.data, results, serializer_class, **kwargs)

            if results["success"]:
                # if even one insert was successful, general status of the request is success
                http_status = status.HTTP_201_CREATED
            else:
                # only if all inserts have failed, return a general failure for the whole request
                http_status = status.HTTP_400_BAD_REQUEST

        else:
            results, http_status = cls._create_single(
                common_info, request.data, serializer_class, **kwargs
            )

        if "failed" in results:
            cls._check_and_raise_atomic_error(request, results)

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
                results["success"].append({"object": serializer.data})

    @staticmethod
    def get_json_schema(schema_folder_path, model_name, data_catalog_prefix=False):
        """
        Get the json schema file for model model_name.
        schema_folder_path is a the path to the folder where schemas are located.
        It can be given due to different api versions have different schema paths.
        For datasets, a data catalog prefix can be given, in which case it will
        be the prefix for the schema file name.
        """
        schema_name = ""

        if model_name == "dataset":
            if data_catalog_prefix:
                schema_name = data_catalog_prefix
            else:
                schema_name = "ida"

            schema_name += "_"

        schema_name += "%s_schema.json" % model_name

        try:
            with open("%s/%s" % (schema_folder_path, schema_name), encoding="utf-8") as f:
                return json_load(f)
        except IOError as e:
            if model_name != "dataset":
                # only datasets have a default schema
                raise
            _logger.warning(e)
            with open("%s/ida_dataset_schema.json" % schema_folder_path, encoding="utf-8") as f:
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
            raise ValidationError({"detail": ["request.data is not a list"]})

        common_info = cls.update_common_info(request, return_only=True)
        results = {"success": [], "failed": []}

        for row in request.data:

            instance = cls._get_object_for_update(
                request,
                model_obj,
                row,
                results,
                cls._request_has_header(request, "HTTP_IF_UNMODIFIED_SINCE"),
            )

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
                results["success"].append({"object": serializer.data})

        # if even one operation was successful, general status of the request is success
        if len(results.get("success", [])) > 0:
            http_status = status.HTTP_200_OK
        else:
            http_status = status.HTTP_400_BAD_REQUEST

        if "failed" in results:
            cls._check_and_raise_atomic_error(request, results)

        return results, http_status

    @staticmethod
    def update_common_info(request, return_only=False):
        """
        Update fields common for all tables and most actions:
        - last modified timestamp and service/user name
        - created on timestamp and service/user name

        For cases where request data is actually xml, or bulk update/create, it is useful to
        return the common info, so that its info can be used manually, instead of updating
        request.data here automatically. For that purpose, use the return_only flag.
        """
        if not request.user.username:  # pragma: no cover
            # should never happen: update_common_info is executed only on update operations,
            # which requires authorization, which should put the username into the request obj.
            ValidationError(
                {
                    "detail": "request.user.username not set; unknown service or user. "
                    "how did you get here without passing authorization...?"
                }
            )

        method = request.stream and request.stream.method or False
        current_time = get_tz_aware_now_without_micros()
        common_info = {}

        if method in ("PUT", "PATCH", "DELETE"):
            common_info["date_modified"] = current_time
            if request.user.is_service:
                common_info["service_modified"] = request.user.username
            else:
                common_info["user_modified"] = request.user.username
                common_info["service_modified"] = None
        elif method == "POST":
            common_info["date_created"] = current_time
            if request.user.is_service:
                common_info["service_created"] = request.user.username
            else:
                common_info["user_created"] = request.user.username
        else:
            pass

        if return_only:
            return common_info
        else:
            request.data.update(common_info)

    @staticmethod
    def _check_and_raise_atomic_error(request, results):
        if "success" in results and not len(results["success"]):
            # everything failed anyway, so return normal route even if atomic was used
            return
        if len(results.get("failed", [])) > 0 and request.query_params.get("atomic", None) in (
            "",
            "true",
        ):
            raise ValidationError(
                {
                    "success": [],
                    "failed": results["failed"],
                    "detail": [
                        "request was failed due to parameter atomic=true. all changes were rolled back. "
                        'actual failed rows are listed in the field "failed".'
                    ],
                }
            )

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
            results["failed"].append(
                {"object": serializer.initial_data, "errors": serializer.errors}
            )
        except AssertionError:
            _logger.exception(
                "Looks like serializer.is_valid() tripped - could not access serializer.errors. "
                "Returning str(e) instead. THIS SHOULD BE FIXED. YES, IM TALKING TO YOU"
            )
            # note that all cases where this happens should be fixed - this is a programming error.
            # str(e) might show dicts or lists as strings, which would look silly to receiving
            # humans
            results["failed"].append({"object": serializer.initial_data, "errors": str(error)})

    @staticmethod
    def _get_object_for_update(request, model_obj, row, results, check_unmodified_since):
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
            results["failed"].append({"object": row, "errors": {"detail": ["object not found"]}})
        except ValidationError as e:
            results["failed"].append({"object": row, "errors": {"detail": e.detail}})

        if instance and not instance.user_has_access(request):

            # dont reveal anything from the actual instance
            ret = {}

            if "id" in row:
                ret["id"] = row["id"]
            if "identifier" in row:
                ret["identifier"] = row["identifier"]

            results["failed"].append(
                {
                    "object": ret,
                    "errors": {"detail": ["You are not permitted to access this resource."]},
                }
            )

            instance = None

        if instance and check_unmodified_since:
            if "date_modified" not in row:
                results["failed"].append(
                    {
                        "object": row,
                        "errors": {
                            "detail": [
                                "Field date_modified is required when the header If-Unmodified-Since is set"
                            ]
                        },
                    }
                )
            elif instance.modified_since(row["date_modified"]):
                results["failed"].append(
                    {
                        "object": row,
                        "errors": {"detail": ["Resource has been modified"]},
                    }
                )
            else:
                # good case - all is good
                pass

        return instance

    @classmethod
    def _validate_and_get_if_unmodified_since_header_as_tz_aware_datetime(cls, request):
        try:
            return cls._validate_http_date_header(request, "HTTP_IF_UNMODIFIED_SINCE")
        except:
            raise Http400("Bad If-Unmodified-Since header")

    @classmethod
    def validate_and_get_if_modified_since_header_as_tz_aware_datetime(cls, request):
        try:
            return cls._validate_http_date_header(request, "HTTP_IF_MODIFIED_SINCE")
        except:
            raise Http400("Bad If-Modified-Since header")

    @staticmethod
    def _validate_http_date_header(request, header_name):
        timestamp = request.META.get(header_name, "")
        # According to RFC 7232, Http date should always be expressed in 'GMT'. Forcing its use makes this explicit
        if not timestamp.endswith("GMT"):
            raise Exception
        return parse_timestamp_string_to_tz_aware_datetime(timestamp)

    @staticmethod
    def _request_has_header(request, header_name):
        return header_name in request.META

    @staticmethod
    def _request_is_write_operation(request):
        return request.method in ("POST", "PUT", "PATCH", "DELETE")

    @staticmethod
    def request_is_create_operation(request):
        """
        This is fast'n dirty method for correctly choosing the json schema for dataset
        validation. the if the path contains files, it means that the user is adding
        files to the dataset thus, not creating the dataset. This might not work for other
        datatypes out of the box.
        """
        return request.method in ("POST") and "files" not in request.path

    @classmethod
    def check_if_unmodified_since(cls, request, obj):
        if cls._request_is_write_operation(request) and cls._request_has_header(
            request, "HTTP_IF_UNMODIFIED_SINCE"
        ):

            header_timestamp = (
                cls._validate_and_get_if_unmodified_since_header_as_tz_aware_datetime(request)
            )
            if obj.modified_since(header_timestamp):
                raise Http412(
                    "Resource has been modified since {0} (timezone: {1})".format(
                        str(header_timestamp), timezone.get_default_timezone_name()
                    )
                )

    @classmethod
    def set_if_modified_since_filter(cls, request, filter_obj):
        """
        Evaluate If-Modified-Since http header only on read operations.
        Filter items whose date_modified field timestamp value is greater than the header value,
        or if date_modified is missing, compares with field date_created instead.
        This method updates given filter object.

        :param request:
        :param filter_obj
        :return:
        """
        if not cls._request_is_write_operation(request) and cls._request_has_header(
            request, "HTTP_IF_MODIFIED_SINCE"
        ):

            ts = cls.validate_and_get_if_modified_since_header_as_tz_aware_datetime(request)

            flter = Q(date_modified__gt=ts) | (Q(date_modified=None) & Q(date_created__gt=ts))

            if "q_filters" in filter_obj:
                filter_obj["q_filters"].append(flter)
            else:
                filter_obj["q_filters"] = [flter]

    @staticmethod
    def identifiers_to_ids(identifiers: List[any], params=None):
        """
        In case identifiers are identifiers (strings), which they probably are in real use,
        do a query to get a list of pk's instead, since they will be used quite a few times.
        """
        if not isinstance(identifiers, list):
            raise Http400("Received identifiers is not a list")
        elif not identifiers:
            _logger.info("Received empty list of identifiers. Aborting")
            raise Http400("Received empty list of identifiers")
        elif all(isinstance(x, int) for x in identifiers):
            return identifiers

        if params in ["files", "noparams"]:
            identifiers = [
                id
                for id in File.objects.filter(identifier__in=identifiers).values_list(
                    "id", flat=True
                )
            ]
        else:
            identifiers = [
                id
                for id in cr.objects.filter(identifier__in=identifiers).values_list("id", flat=True)
            ]

        return identifiers
