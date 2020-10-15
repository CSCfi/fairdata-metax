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
from oaipmh.error import IdDoesNotExistError, BadArgumentError, NoRecordsMatchError, CannotDisseminateFormatError

from metax_api.models.catalog_record import CatalogRecord, DataCatalog
from metax_api.services import CatalogRecordService as CRS
from metax_api.services.datacite_service import DataciteException, convert_cr_to_datacite_cr_json

# SYKE_IDENTIFIER_PREFIX
SYKE_URL_PREFIX_TEMPLATE = 'https://metadata.ymparisto.fi/dataset/%s'
DATACATALOGS_SET = 'datacatalogs'
DATASETS_SET = 'datasets'
OAI_DC_MDPREFIX = 'oai_dc'
OAI_DATACITE_MDPREFIX = 'oai_datacite'
OAI_FAIRDATA_DATACITE_MDPREFIX = 'oai_fairdata_datacite'
OAI_DC_URNRESOLVER_MDPREFIX = 'oai_dc_urnresolver'


class MetaxOAIServer(ResumptionOAIPMH):

    def _validate_mdprefix_and_set(self, metadataPrefix, set=None):
        if not set:
            pass
        elif set == DATACATALOGS_SET:
            if metadataPrefix != OAI_DC_MDPREFIX:
                raise BadArgumentError('Invalid metadataPrefix value. Data catalogs can only be harvested using '
                                       '{0} format.'.format(OAI_DC_MDPREFIX))
        elif set in settings.OAI['SET_MAPPINGS']:
            if set != DATASETS_SET and metadataPrefix == OAI_DC_URNRESOLVER_MDPREFIX:
                raise BadArgumentError('When using metadataPrefix {0}, set value must be either {1} or {2}'
                                       .format(OAI_DC_URNRESOLVER_MDPREFIX, DATACATALOGS_SET, DATASETS_SET))
        else:
            raise BadArgumentError('Invalid set value')

    @staticmethod
    def _get_default_set_filter():
        # there are not that many sets yet, so just using list even though
        # there will be duplicates
        catalog_urns = []
        for k, v in settings.OAI['SET_MAPPINGS'].items():
            catalog_urns.extend(v)
        return catalog_urns

    def _get_urnresolver_record_data(self, set, cursor, batch_size, from_=None, until=None):
        proxy = CatalogRecord
        if set == DATACATALOGS_SET:
            proxy = DataCatalog

        # Use unfiltered objects for fetching catalog records for urn resolver, since otherwise deleted objects
        # won't appear in the result. Get only active objects.
        records = proxy.objects_unfiltered.filter(active=True)

        if from_ and until:
            records = records.filter(date_modified__gte=from_, date_modified__lte=until)
        elif from_:
            records = records.filter(date_modified__gte=from_)
        elif until:
            records = records.filter(date_modified__lte=until)

        # Fetch only needed values as dict to increase performance.
        records = records.values(
            'identifier',
            'date_created',
            'date_modified',
            'data_catalog__catalog_json',
            'research_dataset')

        data = []
        for record in records:
            metadatas = self._get_oai_dc_urnresolver_metadatas_for_record(record)
            for md in metadatas:
                item = (common.Header('', self._get_record_identifier(record, set),
                                      self._get_header_timestamp(record), ['metax'], False),
                        common.Metadata('', md), None)
                data.append(item)

        cursor_end = cursor + batch_size if cursor + batch_size < len(data) else len(data)
        return data[cursor:cursor_end]

    def _get_filtered_records_data(self, verb, metadata_prefix, set, cursor, batch_size, from_=None, until=None):
        proxy = CatalogRecord
        if set == DATACATALOGS_SET:
            proxy = DataCatalog

        # For NON urn resolver, only get non-deleted active objects
        query_set = proxy.objects.all()

        if from_ and until:
            query_set = proxy.objects.filter(date_modified__gte=from_, date_modified__lte=until)
        elif from_:
            query_set = proxy.objects.filter(date_modified__gte=from_)
        elif until:
            query_set = proxy.objects.filter(date_modified__lte=until)

        if set:
            if set == DATACATALOGS_SET:
                pass
            else:
                query_set = query_set.filter(
                    data_catalog__catalog_json__identifier__in=settings.OAI['SET_MAPPINGS'][set])
        else:
            query_set = query_set.filter(data_catalog__catalog_json__identifier__in=self._get_default_set_filter())
            query_set = query_set.filter(state='published')

        data = []
        for record in query_set:
            if verb == 'ListRecords':
                try:
                    oai_item = self._get_oai_item(self._get_record_identifier(record, set), record, metadata_prefix)
                    data.append(oai_item)
                except CannotDisseminateFormatError as e:
                    if metadata_prefix == OAI_FAIRDATA_DATACITE_MDPREFIX or metadata_prefix == OAI_DATACITE_MDPREFIX:
                        pass
                    else:
                        raise e
            elif verb == 'ListIdentifiers':
                identifier = self._get_record_identifier(record, set)
                data.append(common.Header('', identifier, self._get_header_timestamp(record), ['metax'], False))
            else:
                raise Exception("OAI-PMH bad code error")

        cursor_end = cursor + batch_size if cursor + batch_size < len(data) else len(data)
        return data[cursor:cursor_end]

    def _handle_syke_urnresolver_metadata(self, record):
        identifiers = []
        preferred_identifier = record.research_dataset.get('preferred_identifier')
        identifiers.append(preferred_identifier)
        for id_obj in record.research_dataset.get('other_identifier', []):
            if id_obj.get('notation', '').startswith('{'):
                uuid = id_obj['notation']
                identifiers.append(SYKE_URL_PREFIX_TEMPLATE % uuid)
                break
        return identifiers

    def _get_oai_dc_urnresolver_metadatas_for_record(self, record):
        """
        Preferred identifier should be a csc or att urn and is added only for non-harvested catalog records.
        Other identifiers are checked for all catalog records and identifier is added if it is a kata-identifier.
        Special handling for SYKE catalog's catalog records.

        Returns a list of metadata objects, since one record can have more than one resolvable identifier. The caller
        of this method should make these objects as separate oai-pmh records.
        """
        metadatas = []

        if isinstance(record, dict):
            pref_id = record['research_dataset'].get('preferred_identifier')
            dc_id = record['data_catalog__catalog_json'].get('identifier')
            is_harvested = record['data_catalog__catalog_json'].get('harvested', False) is True
            if record['research_dataset'].get('other_identifier') is not None:
                other_ids = record['research_dataset'].get('other_identifier')
            else:
                other_ids = []

            if dc_id == 'urn:nbn:fi:att:data-catalog-harvest-syke':
                for id_obj in other_ids:
                    if id_obj.get('notation', '').startswith('{'):
                        metadatas.append({'identifier': [SYKE_URL_PREFIX_TEMPLATE % id_obj['notation'], pref_id]})
                        break

            elif dc_id not in settings.LEGACY_CATALOGS:
                resolution_url = settings.OAI['ETSIN_URL_TEMPLATE'] % record['identifier']
                if not is_harvested and (pref_id.startswith('urn:nbn:fi:att:') or
                                         pref_id.startswith('urn:nbn:fi:csc')):
                    metadatas.append({'identifier': [resolution_url, pref_id]})

                for id_obj in other_ids:
                    if id_obj.get('notation', '').startswith('urn:nbn:fi:csc-kata'):
                        metadatas.append({'identifier': [resolution_url, id_obj['notation']]})

        return metadatas

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
                if isinstance(value['name'], dict):
                    for key, val in value['name'].items():
                        creator.append(self._get_oaic_dc_value(val, key))
                else:
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
        desc_data = json.get('description', None)
        if desc_data is not None:
            if isinstance(desc_data, dict):
                for key, value in desc_data.items():
                    desc.append(self._get_oaic_dc_value(value, key))
            else:
                desc.append(desc_data)

        publisher = []
        publisher_data = json.get('publisher', {})
        for key, value in publisher_data.get('name', {}).items():
            publisher.append(self._get_oaic_dc_value(value, key))

        contributor = []
        contributor_data = json.get('contributor', [])
        for value in contributor_data:
            if 'name' in value:
                if isinstance(value['name'], dict):
                    for key, val in value['name'].items():
                        contributor.append(self._get_oaic_dc_value(val, key))
                else:
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
            if 'identifier' in value.get('entity', {}):
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

    def _get_oai_datacite_metadata(self, cr, datacite_type):
        cr_json = convert_cr_to_datacite_cr_json(cr)
        try:
            datacite_xml = CRS.transform_datasets_to_format(cr_json, datacite_type, False)
        except DataciteException as e:
            raise CannotDisseminateFormatError(str(e))

        meta = {
            'datacentreSymbol': 'Metax',
            'schemaVersion': '4.1',
            'payload': datacite_xml
        }
        return meta

    def _get_metadata_for_record(self, record, metadataPrefix):
        meta = {}
        if isinstance(record, CatalogRecord):
            json = record.research_dataset
        elif isinstance(record, DataCatalog):
            json = record.catalog_json
        else:
            json = {}

        if metadataPrefix == OAI_DC_MDPREFIX:
            meta = self._get_oai_dc_metadata(record, json)
        elif metadataPrefix == OAI_FAIRDATA_DATACITE_MDPREFIX:
            meta = self._get_oai_datacite_metadata(record, 'fairdata_datacite')
        elif metadataPrefix == OAI_DATACITE_MDPREFIX:
            meta = self._get_oai_datacite_metadata(record, 'datacite')

        return self._fix_metadata(meta)

    def _get_header_timestamp(self, record):
        """
        Can handle record as json or object.
        """
        if isinstance(record, dict):
            modified = record.get('date_modified', None)
            timestamp = modified if modified is not None else record['date_created']
        else:
            timestamp = record.date_modified if record.date_modified else record.date_created
        return timezone.make_naive(timestamp)

    def _get_oai_item(self, identifier, record, metadata_prefix):
        item = (common.Header('', identifier, self._get_header_timestamp(record), ['metax'], False),
                common.Metadata('', self._get_metadata_for_record(record, metadata_prefix)), None)
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
        """
        Can handle record as json or object.
        """
        if set == DATACATALOGS_SET:
            return record['catalog_json__identifier'] if isinstance(record, dict) else record.catalog_json['identifier']
        else:
            return record['identifier'] if isinstance(record, dict) else record.identifier

# OAI-PMH VERBS

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
                (OAI_FAIRDATA_DATACITE_MDPREFIX,
                 'https://schema.datacite.org/meta/kernel-4.1/metadata.xsd',
                 'https://schema.datacite.org/meta/kernel-4.1/'),
                (OAI_DATACITE_MDPREFIX,
                 'https://schema.datacite.org/meta/kernel-4.1/metadata.xsd',
                 'https://schema.datacite.org/meta/kernel-4.1/'),
                (OAI_DC_URNRESOLVER_MDPREFIX,
                 '',
                 '')
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
        if metadataPrefix == OAI_DC_URNRESOLVER_MDPREFIX:
            raise BadArgumentError('Invalid metadataPrefix value. It can be only used with ListRecords verb')

        self._validate_mdprefix_and_set(metadataPrefix, set)
        return self._get_filtered_records_data('ListIdentifiers', metadataPrefix, set, cursor, batch_size, from_, until)

    def listRecords(self, metadataPrefix=None, set=None, cursor=None, from_=None,
                    until=None, batch_size=None):
        """Implement OAI-PMH verb ListRecords."""
        self._validate_mdprefix_and_set(metadataPrefix, set)
        data = []
        if metadataPrefix == OAI_DC_URNRESOLVER_MDPREFIX:
            data = self._get_urnresolver_record_data(set, cursor, batch_size, from_, until)
        else:
            data = self._get_filtered_records_data('ListRecords', metadataPrefix, set, cursor, batch_size, from_, until)
        return data

    def getRecord(self, metadataPrefix, identifier):
        """Implement OAI-PMH verb GetRecord."""
        try:
            if metadataPrefix == OAI_DC_URNRESOLVER_MDPREFIX:
                raise BadArgumentError('Invalid metadataPrefix value. It can be only used with ListRecords verb')
            record = CatalogRecord.objects.get(identifier__exact=identifier)
            if record.state == 'draft':
                raise IdDoesNotExistError("No record with identifier %s is available." % identifier)
        except CatalogRecord.DoesNotExist:
            try:
                record = DataCatalog.objects.get(catalog_json__identifier__exact=identifier)
                if record and metadataPrefix != OAI_DC_MDPREFIX:
                    raise BadArgumentError('Invalid metadataPrefix value. Data catalogs can only be harvested using '
                                           '{0} format.'.format(OAI_DC_MDPREFIX))
            except DataCatalog.DoesNotExist:
                raise IdDoesNotExistError("No record with identifier %s is available." % identifier)

        metadata = self._get_metadata_for_record(record, metadataPrefix)

        if metadata is None:
            raise NoRecordsMatchError

        return (common.Header('', identifier, self._get_header_timestamp(record), ['metax'], False),
            common.Metadata('', metadata), None)