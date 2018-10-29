# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import datetime

from django.utils import timezone
from django.conf import settings
from oaipmh import common
from oaipmh.common import ResumptionOAIPMH
from oaipmh.error import IdDoesNotExistError, BadArgumentError, NoRecordsMatchError

from metax_api.models.catalog_record import CatalogRecord, DataCatalog
from metax_api.services import CatalogRecordService as CRS

SYKE_URL_PREFIX_TEMPLATE = 'http://metatieto.ymparisto.fi:8080/geoportal/catalog/search/resource/details.page?uuid=%s'
DATACATALOGS_SET = 'datacatalogs'
DATASETS_SET = 'datasets'
OAI_DC_MDPREFIX = 'oai_dc'
OAI_DATACITE_MDPREFIX = 'oai_datacite'
OAI_DC_URNRESOLVER_MDPREFIX = 'oai_dc_urnresolver'


class MetaxOAIServer(ResumptionOAIPMH):

    def _is_valid_set(self, set, metadataPrefix):
        if not set:
            return True
        if set == DATACATALOGS_SET:
            if metadataPrefix == OAI_DC_URNRESOLVER_MDPREFIX:
                raise BadArgumentError('{0} metadataPrefix not implemented for datacatalogs'
                                       .format(OAI_DC_URNRESOLVER_MDPREFIX))
            else:
                return True
        if set in settings.OAI['SET_MAPPINGS']:
            if set != DATASETS_SET and metadataPrefix == OAI_DC_URNRESOLVER_MDPREFIX:
                raise BadArgumentError('When using metadataPrefix {0}, set value must be either {1} or {2}'
                                       .format(OAI_DC_URNRESOLVER_MDPREFIX, DATACATALOGS_SET, DATASETS_SET))
            else:
                return True
        return False

    def _get_default_set_filter(self):
        # there are not that many sets yet, so just using list even though
        # there will be duplicates
        catalog_urns = []
        for k, v in settings.OAI['SET_MAPPINGS'].items():
            catalog_urns.extend(v)
        return catalog_urns

    def _get_filtered_records(self, metadataPrefix, set, cursor, batch_size, from_=None, until=None):
        if not self._is_valid_set(set, metadataPrefix):
            raise BadArgumentError('Invalid set value')

        proxy = CatalogRecord
        if set == DATACATALOGS_SET:
            proxy = DataCatalog

        if metadataPrefix == OAI_DC_URNRESOLVER_MDPREFIX:
            # Use unfiltered objects for fetching catalog records for urn resolver, since otherwise deleted objects
            # won't appear in the result. Get only active objects.
            query_set = proxy.objects_unfiltered.filter(active=True)
        else:
            # For NON urn resolver, only get non-deleted active objects
            query_set = proxy.objects.all()

        if from_ and until:
            query_set = proxy.objects.filter(date_modified__gte=from_, date_modified__lte=until)
        elif from_:
            query_set = proxy.objects.filter(date_modified__gte=from_)
        elif until:
            query_set = proxy.objects.filter(date_modified__lte=until)

        if metadataPrefix == OAI_DC_URNRESOLVER_MDPREFIX:
            pass
        else:
            if set:
                if set == DATACATALOGS_SET:
                    pass
                else:
                    query_set = query_set.filter(
                        data_catalog__catalog_json__identifier__in=settings.OAI['SET_MAPPINGS'][set])
            else:
                query_set = query_set.filter(data_catalog__catalog_json__identifier__in=self._get_default_set_filter())
        cursor_end = cursor + batch_size if cursor + batch_size < len(query_set) else len(query_set)
        return query_set[cursor:cursor_end]

    def _handle_syke_urnresolver_metadata(self, record):
        identifiers = []
        preferred_identifier = record.research_dataset.get('preferred_identifier')
        identifiers.append(preferred_identifier)
        for id_obj in record.research_dataset.get('other_identifier', []):
            if id_obj.get('notation', '').startswith('{'):
                uuid = id_obj['notation']
                identifiers.append(SYKE_URL_PREFIX_TEMPLATE % uuid)
        return identifiers

    def _get_oai_dc_urnresolver_metadata(self, record):
        """
        Preferred identifier should be a csc or att urn and is added only for non-harvested catalog records.
        Other identifiers are checked for all catalog records and identifier is added if it is a kata-identifier.
        Special handling for SYKE catalog's catalog records.
        """
        identifiers = []
        if isinstance(record, CatalogRecord):
            data_catalog = record.data_catalog.catalog_json.get('identifier')

            if data_catalog == 'urn:nbn:fi:att:data-catalog-harvest-syke':
                identifiers = self._handle_syke_urnresolver_metadata(record)
            elif record.data_catalog.catalog_json['identifier'] not in settings.LEGACY_CATALOGS:
                identifiers.append(settings.OAI['ETSIN_URL_TEMPLATE'] % record.identifier)

                if not record.catalog_is_harvested():
                    preferred_identifier = record.research_dataset.get('preferred_identifier')
                    if preferred_identifier.startswith('urn:nbn:fi:att:') or \
                            preferred_identifier.startswith('urn:nbn:fi:csc'):
                        identifiers.append(preferred_identifier)

                for id_obj in record.research_dataset.get('other_identifier', []):
                    if id_obj.get('notation', '').startswith('urn:nbn:fi:csc-kata'):
                        other_urn = id_obj['notation']
                        identifiers.append(other_urn)

        # If there is only one identifier it probably means there is no identifier that should be resolved
        if len(identifiers) < 2:
            return None

        meta = {
            'identifier':  identifiers
        }
        return meta

    def _get_oaic_dc_value(self, value, lang=None):
        valueDict = {}
        valueDict['value'] = value
        if lang:
            valueDict['lang'] = lang
        return valueDict

    def _get_oai_dc_metadata(self, record, json):
        identifier = []
        if 'preferred_identifier' in json:
            identifier.append(self._get_oaic_dc_value(json.get('preferred_identifier')))
        if 'identifier' in json:
            identifier.append(self._get_oaic_dc_value(json.get('identifier')))

        title = []
        title_data = json.get('title', {})
        for key, value in title_data.items():
            title.append(self._get_oaic_dc_value(value, key))

        creator = []
        creator_data = json.get('creator', [])
        for value in creator_data:
            if 'name' in value:
                creator.append(self._get_oaic_dc_value(value.get('name')))

        subject = []
        subject_data = json.get('keyword', [])
        for value in subject_data:
            subject.append(self._get_oaic_dc_value(value))
        subject_data = json.get('field_of_science', [])
        for value in subject_data:
            for key, value2 in value.get('pref_label', {}).items():
                subject.append(self._get_oaic_dc_value(value2, key))
        subject_data = json.get('theme', [])
        for value in subject_data:
            for key, value2 in value.get('pref_label', {}).items():
                subject.append(self._get_oaic_dc_value(value2, key))

        desc = []
        desc_data = json.get('description', {}).get('name', {})
        for key, value in desc_data.items():
                desc.append(self._get_oaic_dc_value(value, key))

        publisher = []
        publisher_data = json.get('publisher', {})
        for key, value in publisher_data.get('name', {}).items():
            publisher.append(self._get_oaic_dc_value(value, key))

        contributor = []
        contributor_data = json.get('contributor', [])
        for value in contributor_data:
            if 'name' in value:
                contributor.append(self._get_oaic_dc_value(value.get('name')))

        date = self._get_oaic_dc_value(str(record.date_created))

        language = []
        language_data = json.get('language', [])
        for value in language_data:
            if 'identifier' in value:
                language.append(self._get_oaic_dc_value(value['identifier']))

        relation = []
        relation_data = json.get('relation', [])
        for value in relation_data:
            if 'identifier'in value.get('entity', {}):
                relation.append(self._get_oaic_dc_value(value['entity']['identifier']))

        coverage = []
        coverage_data = json.get('spatial', [])
        for value in coverage_data:
            if 'geographic_name' in value:
                coverage.append(self._get_oaic_dc_value(value['geographic_name']))

        rights = []
        rights_data = json.get('access_rights', {})
        rights_desc = rights_data.get('description', {}).get('name', {})
        for key, value in rights_desc.items():
            rights.append(self._get_oaic_dc_value(value, key))

        for value in rights_data.get('license', []):
            if 'identifier' in value:
                rights.append(self._get_oaic_dc_value(value['identifier']))

        if isinstance(record, CatalogRecord):
            m_type = 'Dataset'
        elif isinstance(record, DataCatalog):
            m_type = 'Datacatalog'
        else:
            m_type = 'N/A'

        meta = {
            'identifier':  identifier,
            'title': title,
            'creator': creator,
            'subject': subject,
            'description': desc,
            'publisher': publisher,
            'contributor': contributor,
            'date': [date],
            'type': [self._get_oaic_dc_value(m_type)],
            'language': language,
            'relation': relation,
            'coverage': coverage,
            'rights': rights
        }
        return meta

    def _get_oai_datacite_metadata(self, json):
        datacite_xml = CRS.transform_datasets_to_format(
            {'research_dataset': json}, 'datacite', False
        )
        meta = {
            'datacentreSymbol': 'Metax',
            'schemaVersion': '4.1',
            'payload': datacite_xml
        }
        return meta

    def _get_metadata_for_record(self, record, metadataPrefix):
        if isinstance(record, CatalogRecord):
            if metadataPrefix != OAI_DC_URNRESOLVER_MDPREFIX:
                json = CRS.check_and_remove_metadata_based_on_access_type(
                    CRS.remove_contact_info_metadata(record.research_dataset))
        elif isinstance(record, DataCatalog):
            if metadataPrefix != OAI_DC_MDPREFIX:
                raise BadArgumentError('Invalid metadataPrefix value. '
                                       'DataCatalogs can only be harvested using oai_dc format.')
            json = record.catalog_json
        else:
            json = {}

        meta = {}
        if metadataPrefix == OAI_DC_MDPREFIX:
            meta = self._get_oai_dc_metadata(record, json)
        elif metadataPrefix == OAI_DATACITE_MDPREFIX:
            meta = self._get_oai_datacite_metadata(json)
        elif metadataPrefix == OAI_DC_URNRESOLVER_MDPREFIX:
            meta = self._get_oai_dc_urnresolver_metadata(record)
            # If record did not have any identifiers to be resolved, return None
            if meta is None:
                return None
        return self._fix_metadata(meta)

    def _get_header_timestamp(self, record):
        if record.date_modified:
            timestamp = record.date_modified
        else:
            timestamp = record.date_created
        return timezone.make_naive(timestamp)

    def _get_oai_item(self, identifier, record, metadata_prefix):
        metadata = self._get_metadata_for_record(record, metadata_prefix)
        if metadata is None:
            return None

        item = (common.Header('', identifier, self._get_header_timestamp(record), ['metax'], False),
                common.Metadata('', metadata), None)
        return item

    def _get_oai_catalog_item(self, identifier, record, metadata_prefix):
        metadata = self._get_metadata_for_record(record, metadata_prefix)
        item = (common.Header('', identifier, self._get_header_timestamp(record), ['metax'], False),
                common.Metadata('', metadata), None)
        return item

    def _fix_metadata(self, meta):
        metadata = {}
        # Fixes the bug on having a large dataset being scrambled to individual
        # letters
        for key, value in meta.items():
            if not isinstance(value, list):
                metadata[str(key)] = [value]
            else:
                metadata[str(key)] = value
        return metadata

    def _get_record_identifier(self, record, set):
        if set == DATACATALOGS_SET:
            return record.catalog_json['identifier']
        else:
            return record.identifier

    def identify(self):
        """Implement OAI-PMH verb Identify ."""
        first = CatalogRecord.objects.filter(
            data_catalog__catalog_json__identifier__in=self._get_default_set_filter()
        ).order_by(
            'date_created'
        ).values_list('date_created', flat=True).first()
        if first:
            first = timezone.make_naive(first)
        else:
            first = datetime.datetime.now()

        return common.Identify(
            repositoryName=settings.OAI['REPOSITORY_NAME'],
            baseURL=settings.OAI['BASE_URL'],
            protocolVersion="2.0",
            adminEmails=[settings.OAI['ADMIN_EMAIL']],
            earliestDatestamp=first,
            deletedRecord='no',
            granularity='YYYY-MM-DDThh:mm:ssZ',
            compression=['identity'])

    def listMetadataFormats(self, identifier=None):
        """Implement OAI-PMH verb listMetadataFormats ."""
        return [(OAI_DC_MDPREFIX,
                 'http://www.openarchives.org/OAI/2.0/oai_dc.xsd',
                 'http://www.openarchives.org/OAI/2.0/oai_dc/'),
                (OAI_DATACITE_MDPREFIX,
                 'https://schema.datacite.org/meta/kernel-4.1/metadata.xsd',
                 'https://schema.datacite.org/meta/kernel-4.1/'),
                (OAI_DC_URNRESOLVER_MDPREFIX,
                 'http://www.openarchives.org/OAI/2.0/oai_dc.xsd',
                 'http://www.openarchives.org/OAI/2.0/oai_dc/')
                ]

    def listSets(self, cursor=None, batch_size=None):
        """Implement OAI-PMH verb ListSets."""
        data = [(DATACATALOGS_SET, DATACATALOGS_SET, '')]
        for set_key in settings.OAI['SET_MAPPINGS'].keys():
            data.append((set_key, set_key, ''))
        return data

    def listIdentifiers(self, metadataPrefix=None, set=None, cursor=None,
                        from_=None, until=None, batch_size=None):
        """Implement OAI-PMH verb listIdentifiers."""
        records = self._get_filtered_records(metadataPrefix, set, cursor, batch_size, from_, until)
        data = []
        for record in records:
            identifier = self._get_record_identifier(record, set)
            data.append(common.Header('', identifier, self._get_header_timestamp(record), ['metax'], False))
        return data

    def listRecords(self, metadataPrefix=None, set=None, cursor=None, from_=None,
                    until=None, batch_size=None):
        """Implement OAI-PMH verb ListRecords."""
        data = []
        records = self._get_filtered_records(metadataPrefix, set, cursor, batch_size, from_, until)
        for record in records:
            identifier = self._get_record_identifier(record, set)
            if set == DATACATALOGS_SET:
                data.append(self._get_oai_catalog_item(identifier, record, metadataPrefix))
            else:
                oai_item = self._get_oai_item(identifier, record, metadataPrefix)
                if oai_item is not None:
                    data.append(oai_item)
        return data

    def getRecord(self, metadataPrefix, identifier):
        """Implement OAI-PMH verb GetRecord."""
        try:
            if metadataPrefix == OAI_DC_URNRESOLVER_MDPREFIX:
                record = CatalogRecord.objects_unfiltered.get(identifier__exact=identifier)
            else:
                record = CatalogRecord.objects.get(identifier__exact=identifier)
        except CatalogRecord.DoesNotExist:
            try:
                if metadataPrefix == OAI_DC_URNRESOLVER_MDPREFIX:
                    record = DataCatalog.objects_unfiltered.get(catalog_json__identifier__exact=identifier)
                else:
                    record = DataCatalog.objects.get(catalog_json__identifier__exact=identifier)
            except DataCatalog.DoesNotExist:
                raise IdDoesNotExistError("No record with id %s available." % identifier)

        metadata = self._get_metadata_for_record(record, metadataPrefix)
        if metadata is None:
            raise NoRecordsMatchError

        return (common.Header('', identifier, self._get_header_timestamp(record), ['metax'], False),
                common.Metadata('', metadata), None)
