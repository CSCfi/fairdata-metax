import datetime

from django.utils import timezone
from django.conf import settings
from oaipmh import common
from oaipmh.common import ResumptionOAIPMH
from oaipmh.error import IdDoesNotExistError
from oaipmh.error import BadArgumentError

from metax_api.models.catalog_record import CatalogRecord, DataCatalog
from metax_api.services import CatalogRecordService as CRS

syke_url_prefix_template = 'http://metatieto.ymparisto.fi:8080/geoportal/catalog/search/resource/details.page?uuid=%s'


class MetaxOAIServer(ResumptionOAIPMH):

    def _is_valid_set(self, set):
        if not set or set in ['urnresolver', 'datacatalogs'] or set in settings.OAI['SET_MAPPINGS']:
            return True
        return False

    def _get_default_set_filter(self):
        # there are not that many sets yet, so just using list even though
        # there will be duplicates
        catalog_urns = []
        for k, v in settings.OAI['SET_MAPPINGS'].items():
            catalog_urns.extend(v)
        return catalog_urns

    def _get_filtered_records(self, set, cursor, batch_size, from_=None, until=None):
        if not self._is_valid_set(set):
            raise BadArgumentError('invalid set value')

        proxy = CatalogRecord
        if set == 'datacatalogs':
            proxy = DataCatalog

        query_set = proxy.objects.all()
        if from_ and until:
            query_set = proxy.objects.filter(date_modified__gte=from_, date_modified__lte=until)
        elif from_:
            query_set = proxy.objects.filter(date_modified__gte=from_)
        elif until:
            query_set = proxy.objects.filter(date_modified__lte=until)

        if set:
            if set in ['urnresolver', 'datacatalogs']:
                pass
            else:
                query_set = query_set.filter(
                    data_catalog__catalog_json__identifier__in=settings.OAI['SET_MAPPINGS'][set])
        else:
            query_set = query_set.filter(data_catalog__catalog_json__identifier__in=self._get_default_set_filter())
        return query_set[cursor:batch_size]

    def _handle_syke_urnresolver_metadata(self, record):
        identifiers = []
        preferred_identifier = record.research_dataset.get('preferred_identifier')
        identifiers.append(preferred_identifier)
        for id_obj in record.research_dataset.get('other_identifier', []):
            if id_obj.get('notation', '').startswith('{'):
                uuid = id_obj['notation']
                identifiers.append(syke_url_prefix_template % uuid)
        return identifiers

    def _get_oai_dc_urnresolver_metadata(self, record):
        """
        Preferred identifier is added only for ida and att catalog records
        other identifiers are added for all.

        Special handling for SYKE catalog.
        """

        identifiers = []

        data_catalog = record.data_catalog.catalog_json.get('identifier')
        if data_catalog == 'urn:nbn:fi:att:data-catalog-harvest-syke':
            identifiers = self._handle_syke_urnresolver_metadata(record)

        else:
            identifiers.append(settings.OAI['ETSIN_URL_TEMPLATE'] % record.identifier)

            # assuming ida and att catalogs are not harvested
            if not record.catalog_is_harvested():
                preferred_identifier = record.research_dataset.get('preferred_identifier')
                identifiers.append(preferred_identifier)
            for id_obj in record.research_dataset.get('other_identifier', []):
                if id_obj.get('notation', '').startswith('urn:nbn:fi:csc-kata'):
                    other_urn = id_obj['notation']
                    identifiers.append(other_urn)

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

    def _get_oai_dc_metadata(self, record, json, type):
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
        desc_data = json.get('description', [])
        for value in desc_data:
            for key, value2 in value.items():
                desc.append(self._get_oaic_dc_value(value2, key))

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
        for value in rights_data.get('description', []):
            for key, value2 in value.items():
                rights.append(self._get_oaic_dc_value(value2, key))
        for value in rights_data.get('license', []):
            if 'identifier' in value:
                rights.append(self._get_oaic_dc_value(value['identifier']))

        types = []
        types.append(self._get_oaic_dc_value(type))

        meta = {
            'identifier':  identifier,
            'title': title,
            'creator': creator,
            'subject': subject,
            'description': desc,
            'publisher': publisher,
            'contributor': contributor,
            'date': [date],
            'type': types,
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

    def _get_metadata_for_record(self, record, json, type, metadata_prefix):
        if type == 'Datacatalog' and metadata_prefix != 'oai_dc':
            raise BadArgumentError('Invalid set value. DataCatalogs can only be harvested using oai_dc format.')

        meta = {}
        json = CRS.strip_catalog_record(json)

        if metadata_prefix == 'oai_dc':
            meta = self._get_oai_dc_metadata(record, json, type)
        elif metadata_prefix == 'oai_datacite':
            meta = self._get_oai_datacite_metadata(json)
        elif metadata_prefix == 'oai_dc_urnresolver':
            # This is a special case. Only identifier values are retrieved from the record,
            # so strip_catalog_record is not applicable here.
            meta = self._get_oai_dc_urnresolver_metadata(record)
        return self._fix_metadata(meta)

    def _get_header_timestamp(self, record):
        timestamp = None
        if record.date_modified:
            timestamp = record.date_modified
        else:
            timestamp = record.date_created
        return timezone.make_naive(timestamp)

    def _get_oai_item(self, identifier, record, metadata_prefix):
        metadata = self._get_metadata_for_record(record, record.research_dataset, 'Dataset', metadata_prefix)
        item = (common.Header('', identifier, self._get_header_timestamp(record), ['metax'], False),
                common.Metadata('', metadata), None)
        return item

    def _get_oai_catalog_item(self, identifier, record, metadata_prefix):
        metadata = self._get_metadata_for_record(record, record.catalog_json, 'Datacatalog', metadata_prefix)
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
        return [('oai_dc',
                 'http://www.openarchives.org/OAI/2.0/oai_dc.xsd',
                 'http://www.openarchives.org/OAI/2.0/oai_dc/'),
                ('oai_datacite',
                 'https://schema.datacite.org/meta/kernel-4.1/metadata.xsd',
                 'https://schema.datacite.org/meta/kernel-4.1/'),
                ('oai_dc_urnresolver',
                 'http://www.openarchives.org/OAI/2.0/oai_dc.xsd',
                 'http://www.openarchives.org/OAI/2.0/oai_dc/')
                ]

    def listSets(self, cursor=None, batch_size=None):
        """Implement OAI-PMH verb ListSets."""
        data = [('datacatalogs', 'datacatalog', '')]
        for set_key in settings.OAI['SET_MAPPINGS'].keys():
            data.append((set_key, set_key, ''))
        return data

    def _get_record_identifier(self, record, set):
        if set == 'datacatalogs':
            return record.catalog_json['identifier']
        else:
            return record.identifier

    def listIdentifiers(self, metadataPrefix=None, set=None, cursor=None,
                        from_=None, until=None, batch_size=None):
        """Implement OAI-PMH verb listIdentifiers."""
        records = self._get_filtered_records(set, cursor, batch_size, from_, until)
        data = []
        for record in records:
            identifier = self._get_record_identifier(record, set)
            data.append(common.Header('', identifier, self._get_header_timestamp(record), ['metax'], False))
        return data

    def listRecords(self, metadataPrefix=None, set=None, cursor=None, from_=None,
                    until=None, batch_size=None):
        """Implement OAI-PMH verb ListRecords."""
        data = []
        records = self._get_filtered_records(set, cursor, batch_size, from_, until)
        for record in records:
            identifier = self._get_record_identifier(record, set)
            if set == 'datacatalogs':
                data.append(self._get_oai_catalog_item(identifier, record, metadataPrefix))
            else:
                data.append(self._get_oai_item(identifier, record, metadataPrefix))
        return data

    def getRecord(self, metadataPrefix, identifier):
        """Implement OAI-PMH verb GetRecord."""
        try:
            record = CatalogRecord.objects.get(identifier__exact=identifier)
            json = record.research_dataset
            type = 'Dataset'
        except CatalogRecord.DoesNotExist:
            try:
                record = DataCatalog.objects.get(catalog_json__identifier__exact=identifier)
                json = record.catalog_json
                type = 'Datacatalog'
            except DataCatalog.DoesNotExist:
                raise IdDoesNotExistError("No record with id %s available." % identifier)

        metadata = self._get_metadata_for_record(record,  json, type, metadataPrefix)
        return (common.Header('', identifier, self._get_header_timestamp(record), ['metax'], False),
                common.Metadata('', metadata), None)
