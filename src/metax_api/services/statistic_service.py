# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging
from collections import OrderedDict
from django.conf import settings
from django.db import connection
from django.db.models import Count, Sum
from django.db.models.functions import Coalesce

from metax_api.exceptions import Http400
from metax_api.models import CatalogRecord, DataCatalog, File, ProjectStatistics, OrganizationStatistics

_logger = logging.getLogger(__name__)


class StatisticService:
    @staticmethod
    def _get_access_types():
        sql_distinct_access_types = """
            select distinct(research_dataset->'access_rights'->'access_type'->>'identifier')
            from metax_api_catalogrecord
        """

        with connection.cursor() as cr:
            cr.execute(sql_distinct_access_types)
            access_types = [row[0] for row in cr.fetchall()]

        return access_types

    @classmethod
    def count_datasets(
        cls,
        access_type=None,
        data_catalog=None,
        deprecated=None,
        from_date=None,
        harvested=None,
        latest=True,
        legacy=None,
        metadata_owner_org=None,
        metadata_provider_org=None,
        metadata_provider_user=None,
        preservation_state=None,
        removed=None,
        to_date=None,
    ):
        """
        Get simple total record count and total byte size according to given filters.
        """
        _logger.info("Retrieving total count and byte size...")

        sql = """
            SELECT
                count(cr.id) AS count,
                COALESCE(SUM(COALESCE((research_dataset->>'total_files_byte_size')::bigint, 0)), 0) AS ida_byte_size
            from metax_api_catalogrecord as cr
            join metax_api_datacatalog as dc on dc.id = cr.data_catalog_id
            where 1=1
            %s
        """
        where_args = []
        sql_args = []

        where_args.append("and state = 'published'")

        if from_date:
            where_args.append("and cr.date_created >= %s::date")
            sql_args.append(from_date)

        if to_date:
            where_args.append("and cr.date_created <= %s::date")
            sql_args.append(to_date)

        if access_type:
            where_args.append(
                "and research_dataset->'access_rights'->'access_type'->>'identifier' = %s"
            )
            sql_args.append(access_type)

        if data_catalog:
            try:
                DataCatalog.objects.values("id").get(pk=int(data_catalog))
                where_args.append("and dc.id = %s")
            except DataCatalog.DoesNotExist:
                raise Http400({"detail": ["Data catalog identifier %s not found" % data_catalog]})
            except ValueError:
                try:
                    DataCatalog.objects.values("id").get(catalog_json__identifier=data_catalog)
                    where_args.append("and dc.catalog_json->>'identifier' = %s")
                except DataCatalog.DoesNotExist:
                    raise Http400(
                        {"detail": ["Data catalog identifier %s not found" % data_catalog]}
                    )
            sql_args.append(data_catalog)

        if deprecated is not None:
            where_args.append("and deprecated = %s")
            sql_args.append(deprecated)

        if harvested:
            where_args.append("and dc.catalog_json->>'harvested' = %s")
            sql_args.append(harvested)

        if latest:
            where_args.append("and next_dataset_version_id is null")

        if metadata_owner_org:
            where_args.append("and metadata_owner_org = %s")
            sql_args.append(metadata_owner_org)

        if metadata_provider_org:
            where_args.append("and metadata_provider_org = %s")
            sql_args.append(metadata_provider_org)

        if metadata_provider_user:
            where_args.append("and metadata_provider_user = %s")
            sql_args.append(metadata_provider_user)

        if preservation_state:
            where_args.append("and preservation_state = %s")
            sql_args.append(preservation_state)

        if removed is not None:
            where_args.append("and cr.removed = %s")
            sql_args.append(removed)

        if legacy is not None:
            where_args.append(
                ''.join(["and", " " if legacy else " NOT ", "dc.catalog_json->>'identifier'", " = ", "any(%s)"])
            )
            sql_args.append(settings.LEGACY_CATALOGS)

        sql = sql % "\n".join(where_args)

        with connection.cursor() as cr:
            cr.execute(sql, sql_args)
            try:
                results = [
                    dict(zip([col[0] for col in cr.description], row)) for row in cr.fetchall()
                ][0]
            except IndexError:
                results = {}

        _logger.info("Done retrieving total count and byte size")

        return results

    @classmethod
    def total_datasets(cls, from_date, to_date, latest=True, legacy=None, removed=None):
        """
        Retrieve dataset count and byte size per month and monthly cumulative from all datasets.
        """
        _logger.info("Retrieving total count and byte sizes for all datasets...")

        sql_all_datasets = """
            WITH cte AS (
                SELECT
                    date_trunc('month', cr.date_created) AS mon,
                    SUM(COALESCE((cr.research_dataset->>'total_files_byte_size')::bigint, 0)) AS mon_ida_byte_size
                FROM metax_api_catalogrecord cr
                JOIN metax_api_datacatalog AS dc on dc.id = cr.data_catalog_id
                WHERE 1=1
                and state = 'published'
                OPTIONAL_WHERE_FILTERS
                GROUP BY mon
            )
            SELECT
                to_char(mon, 'YYYY-MM') as month,
                COALESCE(count, 0) as count,
                COALESCE(SUM(cr.count) OVER (ORDER BY mon), 0) AS count_cumulative,
                COALESCE(SUM(c.mon_ida_byte_size), 0) AS ida_byte_size,
                COALESCE(SUM(c.mon_ida_byte_size) OVER (ORDER BY mon), 0) AS ida_byte_size_cumulative
            FROM generate_series(%s::date, %s::date, interval '1 month') AS mon
            LEFT JOIN cte c USING (mon)
            LEFT JOIN (
                SELECT
                    count(cr.id) AS count,
                    date_trunc('month', cr.date_created) AS mon
                FROM metax_api_catalogrecord AS cr
                JOIN metax_api_datacatalog AS dc ON dc.id = cr.data_catalog_id
                WHERE 1=1
                and state = 'published'
                OPTIONAL_WHERE_FILTERS
                GROUP BY mon
            ) cr USING (mon)
            GROUP BY mon, c.mon_ida_byte_size, count
            ORDER BY mon;
        """

        filter_sql = []
        filter_args = []

        if latest:
            filter_sql.append("and next_dataset_version_id is null")

        if removed is not None:
            filter_sql.append("and cr.removed = %s")
            filter_args.append(removed)

        if legacy is not None:
            filter_sql.append(
                ''.join(["and", " " if legacy else " NOT ", "dc.catalog_json->>'identifier'", " = ", "any(%s)"])
            )
            filter_args.append(settings.LEGACY_CATALOGS)

        sql_all_datasets = sql_all_datasets.replace("OPTIONAL_WHERE_FILTERS", "\n".join(filter_sql))

        sql_args = filter_args + [from_date + "-01", to_date + "-01"] + filter_args

        with connection.cursor() as cr:
            cr.execute(sql_all_datasets, sql_args)
            results = [dict(zip([col[0] for col in cr.description], row)) for row in cr.fetchall()]

        _logger.info("Done retrieving total count and byte sizes")

        return results

    @classmethod
    def total_data_catalog_datasets(cls, from_date, to_date, data_catalog=None):
        """
        Retrieve dataset count and byte size per month and monthly cumulative for all catalogs,
        or a given catalog, grouped by access_type.
        """
        if data_catalog:
            try:
                dc_params = {"pk": int(data_catalog)}
            except ValueError:
                dc_params = {"catalog_json__identifier": data_catalog}

            try:
                dc = DataCatalog.objects.filter(**dc_params).values()
            except:
                raise Http400({"detail": ["Data catalog identifier %s not found" % data_catalog]})

            _logger.info(
                "Retrieving total count and byte sizes for datasets in catalog: %s"
                % dc["catalog_json"]["identifier"]
            )
            catalogs = [dc]
        else:
            _logger.info("Retrieving total count and byte sizes for datasets in all catalogs")
            catalogs = DataCatalog.objects.all().values()

        access_types = cls._get_access_types()

        results = {}
        for dc in catalogs:
            results[dc["catalog_json"]["identifier"]] = cls._total_data_catalog_datasets(
                from_date, to_date, access_types, dc["id"]
            )

        _logger.info("Done retrieving total count and byte sizes")

        return results

    @classmethod
    def _total_data_catalog_datasets(cls, from_date, to_date, access_types, dc_id):
        sql = """
            WITH cte AS (
                SELECT
                    date_trunc('month', cr.date_created) AS mon,
                    SUM(COALESCE((cr.research_dataset->>'total_files_byte_size')::bigint, 0)) AS mon_ida_byte_size
                FROM metax_api_catalogrecord cr
                JOIN metax_api_datacatalog as dc on dc.id = cr.data_catalog_id
                where dc.id = %s
                and state = 'published'
                and cr.research_dataset->'access_rights'->'access_type'->>'identifier' = %s
                GROUP BY mon
            )
            SELECT
                to_char(mon, 'YYYY-MM') as month,
                COALESCE(count, 0) as count,
                COALESCE(SUM(cr.count) over (partition by to_char(mon, 'YYYY') ORDER BY mon), 0)
                    AS count_cumulative,
                COALESCE(SUM(c.mon_ida_byte_size), 0) AS ida_byte_size,
                COALESCE(SUM(c.mon_ida_byte_size) over (partition by to_char(mon, 'YYYY') ORDER BY mon), 0)
                    AS ida_byte_size_cumulative
            FROM generate_series(%s::date, %s::date, interval '1 month') AS mon
            LEFT JOIN cte c USING (mon)
            LEFT JOIN (
                SELECT
                    count(cr.id) AS count,
                    date_trunc('month', cr.date_created) AS mon,
                    reverse(
                        split_part(reverse(cr.research_dataset->'access_rights'->'access_type'->>'identifier'), '/', 1)
                        ) as access_type
                FROM metax_api_catalogrecord AS cr
                JOIN metax_api_datacatalog as dc on dc.id = cr.data_catalog_id
                where dc.id = %s
                and state = 'published'
                and cr.research_dataset->'access_rights'->'access_type'->>'identifier' = %s
                GROUP BY mon, access_type
            ) cr USING (mon)
            GROUP BY mon, c.mon_ida_byte_size, count, access_type
            ORDER BY mon;
        """

        # group results by access_type
        grouped = {}

        with connection.cursor() as cr:
            for access_type in access_types:
                cr.execute(sql, [dc_id, access_type, from_date, to_date, dc_id, access_type])
                results = [
                    dict(zip([col[0] for col in cr.description], row)) for row in cr.fetchall()
                ]
                grouped[access_type.split("/")[-1]] = results

        total = []

        # sum totals for this data catalog
        for group in grouped.values():
            for i, stats in enumerate(group):
                try:
                    last = total[i]
                except IndexError:
                    total.append(stats)
                else:
                    last["count"] += stats["count"]
                    last["count_cumulative"] += stats["count_cumulative"]
                    last["ida_byte_size"] += stats["ida_byte_size"]
                    last["ida_byte_size_cumulative"] += stats["ida_byte_size_cumulative"]

        grouped["total"] = total

        _logger.info("Done retrieving total count and byte sizes")

        return grouped

    @classmethod
    def total_organization_datasets(cls, from_date, to_date, metadata_owner_org=None, latest=True, legacy=None, removed=None):
        """
        Retrieve dataset count and byte size per month and monthly cumulative for all organizations,
        or a given single organization, grouped by catalog.
        """
        if metadata_owner_org:
            _logger.info(
                "Retrieving total count and byte sizes for datasets for organization: %s"
                % metadata_owner_org
            )
            metadata_owner_orgs = [metadata_owner_org]
        else:
            _logger.info("Retrieving total count and byte sizes for datasets for all organizations")
            metadata_owner_orgs = (
                CatalogRecord.objects.values_list("metadata_owner_org", flat=True)
                .order_by("metadata_owner_org")
                .distinct("metadata_owner_org")
            )

        results = {}
        for org in metadata_owner_orgs:
            results[org] = cls._total_organization_datasets(from_date, to_date, org, latest, legacy, removed)

        return results

    @classmethod
    def _total_organization_datasets(cls, from_date, to_date, metadata_owner_org, latest, legacy, removed):
        sql = """
            WITH cte AS (
                SELECT
                    date_trunc('month', cr.date_created) AS mon,
                    SUM(COALESCE((cr.research_dataset->>'total_files_byte_size')::bigint, 0)) AS mon_ida_byte_size
                FROM metax_api_catalogrecord cr
                JOIN metax_api_datacatalog as dc on dc.id = cr.data_catalog_id
                where dc.id = %s
                and state = 'published'
                and cr.metadata_owner_org = %s
                OPTIONAL_WHERE_FILTERS
                GROUP BY mon
            )
            SELECT
                to_char(mon, 'YYYY-MM') as month,
                COALESCE(count, 0) as count,
                COALESCE(SUM(cr.count) over (partition by to_char(mon, 'YYYY') ORDER BY mon), 0)
                    AS count_cumulative,
                COALESCE(SUM(c.mon_ida_byte_size), 0) AS ida_byte_size,
                COALESCE(SUM(c.mon_ida_byte_size) over (partition by to_char(mon, 'YYYY') ORDER BY mon), 0)
                    AS ida_byte_size_cumulative
            FROM generate_series(%s::date, %s::date, interval '1 month') AS mon
            LEFT JOIN cte c USING (mon)
            LEFT JOIN (
                SELECT
                    count(cr.id) AS count,
                    date_trunc('month', cr.date_created) AS mon
                FROM metax_api_catalogrecord AS cr
                JOIN metax_api_datacatalog as dc on dc.id = cr.data_catalog_id
                where dc.id = %s
                and state = 'published'
                and cr.metadata_owner_org = %s
                OPTIONAL_WHERE_FILTERS
                GROUP BY mon
            ) cr USING (mon)
            GROUP BY mon, c.mon_ida_byte_size, count
            ORDER BY mon;
        """

        filter_sql = []
        filter_args = []

        if latest:
            filter_sql.append("and next_dataset_version_id is null")

        if removed is not None:
            filter_sql.append("and cr.removed = %s")
            filter_args.append(removed)

        if legacy is not None:
            filter_sql.append(
                "".join(
                    [
                        "AND ",
                        "" if legacy else "NOT ",
                        "dc.catalog_json->>'identifier' = ANY(%s)",
                    ]
                )
            )
            filter_args.append(settings.LEGACY_CATALOGS)

        sql = sql.replace("OPTIONAL_WHERE_FILTERS", "\n".join(filter_sql))

        catalogs = DataCatalog.objects.filter(
            catalog_json__research_dataset_schema__in=["ida", "att"]
        ).values()

        # group results by catalogs
        grouped = {}

        with connection.cursor() as cr:
            for dc in catalogs:
                sql_args = [dc["id"], metadata_owner_org] + filter_args + [from_date, to_date, dc["id"], metadata_owner_org] + filter_args
                cr.execute(sql, sql_args)
                results = [
                    dict(zip([col[0] for col in cr.description], row)) for row in cr.fetchall()
                ]
                grouped[dc["catalog_json"]["identifier"]] = results

        total = []

        # sum totals for this organization
        for group in grouped.values():
            for i, stats in enumerate(group):
                try:
                    last = total[i]
                except IndexError:
                    total.append(stats)
                else:
                    last["count"] += stats["count"]
                    last["count_cumulative"] += stats["count_cumulative"]
                    last["ida_byte_size"] += stats["ida_byte_size"]
                    last["ida_byte_size_cumulative"] += stats["ida_byte_size_cumulative"]

        grouped["total"] = total

        _logger.info("Done retrieving total count and byte sizes")

        return grouped

    @classmethod
    def total_harvested_datasets(cls, from_date, to_date):
        """
        For harvested datasets, retrieve dataset count per month and monthly cumulative,
        and grouped by access_type.
        """
        _logger.info("Retrieving total counts for harvested datasets")

        sql = """
            SELECT
                to_char(mon, 'YYYY-MM') as month,
                COALESCE(count, 0) as count,
                COALESCE(SUM(cr.count) over (partition by to_char(mon, 'YYYY') ORDER BY mon), 0) AS count_cumulative
            FROM generate_series(%s::date, %s::date, interval '1 month') AS mon
            LEFT JOIN (
                SELECT
                    count(cr.id) AS count,
                    date_trunc('month', cr.date_created) AS mon,
                    reverse(
                        split_part(reverse(cr.research_dataset->'access_rights'->'access_type'->>'identifier'), '/', 1)
                        ) as access_type
                FROM metax_api_catalogrecord AS cr
                JOIN metax_api_datacatalog as dc on dc.id = cr.data_catalog_id
                where (dc.catalog_json->>'harvested')::boolean = true
                and cr.research_dataset->'access_rights'->'access_type'->>'identifier' = %s
                GROUP BY mon, access_type
            ) cr USING (mon)
            GROUP BY mon, count, access_type
            ORDER BY mon;
        """

        access_types = cls._get_access_types()

        # group by access_type
        grouped = {}

        with connection.cursor() as cr:
            for access_type in access_types:
                cr.execute(sql, [from_date, to_date, access_type])
                results = [
                    dict(zip([col[0] for col in cr.description], row)) for row in cr.fetchall()
                ]
                grouped[access_type.split("/")[-1]] = results

        total = []

        # sum totals for this data catalog
        for group in grouped.values():
            for i, stats in enumerate(group):
                try:
                    last = total[i]
                except IndexError:
                    total.append(stats)
                else:
                    last["count"] += stats["count"]
                    last["count_cumulative"] += stats["count_cumulative"]

        grouped["total"] = total

        _logger.info("Done retrieving total counts")

        return grouped

    @classmethod
    def deprecated_datasets_cumulative(cls, from_date=None, to_date=None):
        """
        Retrieve dataset count per month and monthly cumulative for deprecated datasets.
        """
        _logger.info("Retrieving total counts for deprecated datasets")

        sql = """
            SELECT
                to_char(mon, 'YYYY-MM') as month,
                COALESCE(count, 0) as count,
                COALESCE(SUM(cr.count) over (partition by to_char(mon, 'YYYY') ORDER BY mon), 0) AS count_cumulative
            FROM generate_series(%s::date, %s::date, interval '1 month') AS mon
            LEFT JOIN (
                SELECT
                    count(cr.id) AS count,
                    date_trunc('month', cr.date_created) AS mon
                FROM metax_api_catalogrecord AS cr
                where deprecated is true
                GROUP BY mon
            ) cr USING (mon)
            GROUP BY mon, count
            ORDER BY mon;
        """

        with connection.cursor() as cr:
            cr.execute(sql, [from_date, to_date])
            results = [dict(zip([col[0] for col in cr.description], row)) for row in cr.fetchall()]

        _logger.info("Done retrieving total deprecated counts")

        return results

    @classmethod
    def total_end_user_datasets(cls, from_date, to_date):
        """
        Retrieve dataset count per month and monthly cumulative for datasets
        which have been created by end users using End User API.
        """
        _logger.info("Retrieving total counts for datasets created using End User API")

        sql = """
            SELECT
                to_char(mon, 'YYYY-MM') as month,
                COALESCE(count, 0) as count,
                COALESCE(SUM(cr.count) over (partition by to_char(mon, 'YYYY') ORDER BY mon), 0) AS count_cumulative
            FROM generate_series(%s::date, %s::date, interval '1 month') AS mon
            LEFT JOIN (
                SELECT
                    count(cr.id) AS count,
                    date_trunc('month', cr.date_created) AS mon
                FROM metax_api_catalogrecord AS cr
                where service_created is null
                and state = 'published'
                GROUP BY mon
            ) cr USING (mon)
            GROUP BY mon, count
            ORDER BY mon;
        """

        with connection.cursor() as cr:
            cr.execute(sql, [from_date, to_date])
            results = [dict(zip([col[0] for col in cr.description], row)) for row in cr.fetchall()]

        _logger.info("Done retrieving total counts")

        return results

    @classmethod
    def unused_files(cls):
        _logger.info("Retrieving total counts of files which are not part of any datasets...")

        sql_get_unused_files_by_project = """
            select count(f.id) as count, project_identifier
            from metax_api_file as f
            where not exists (
                select
                from metax_api_catalogrecord_files
                where file_id = f.id
            )
            group by project_identifier;
        """
        with connection.cursor() as cr:
            cr.execute(sql_get_unused_files_by_project)
            file_stats = [
                dict(zip([col[0] for col in cr.description], row)) for row in cr.fetchall()
            ]

        _logger.info("Done retrieving total counts")

        return file_stats

    @classmethod
    def count_files(cls, projects, removed=None, include_pids=False):
        kwargs = OrderedDict()
        file_query = File.objects_unfiltered.all()

        kwargs['project_identifier__in'] = projects
        kwargs['record__state'] = "published"
        if removed is not None:
            kwargs['removed'] = False if removed == 'false' else True
        # "record" is defined for CatalogRecord to enable lookups from Files to CatalogRecord
        file_query = file_query.filter(**kwargs) \
                               .values("id", "byte_size") \
                               .distinct()

        # Coalesce is required to provides default value
        if include_pids:
            return file_query.aggregate(count=Count("id"), byte_size=Coalesce(Sum("byte_size"), 0)), list(file_query.values_list('identifier', flat=True))
        return file_query.aggregate(count=Count("id"), byte_size=Coalesce(Sum("byte_size"), 0))

    @classmethod
    def projects_summary(cls, projects):
        _logger.info(f"projects_summary: {projects}")
        stats_query = ProjectStatistics.objects.all()
        if not projects is None:
            stats_query = stats_query.filter(project_identifier__in=projects)
        _logger.info(f"stats_query: {stats_query.values()}")
        summary = stats_query.values()
        _logger.info(f"summary: {summary}")
        if len(summary) == 0:
            summary = f"No projects found with project_identifier: {projects}"
        return summary

    @classmethod
    def organizations_summary(cls, organizations):
        _logger.info(f"organizations_summary: {organizations}")
        stats_query = OrganizationStatistics.objects.all()
        if not organizations is None:
            stats_query = stats_query.filter(organization__in=organizations)
        _logger.info(f"stats_query: {stats_query.values()}")
        summary = stats_query.values()
        _logger.info(f"summary: {summary}")
        if len(summary) == 0:
            summary = f"No organizations found with organization_identifier: {organizations}"
        return summary

