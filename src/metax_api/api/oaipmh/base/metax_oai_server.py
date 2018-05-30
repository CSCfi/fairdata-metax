import datetime

from django.utils import timezone
from django.conf import settings
from oaipmh import common
from oaipmh.common import ResumptionOAIPMH
from oaipmh.error import IdDoesNotExistError
from oaipmh.error import BadArgumentError

from metax_api.models.catalog_record import CatalogRecord
from metax_api.services import CatalogRecordService as CRS

syke_url_prefix_template = 'http://metatieto.ymparisto.fi:8080/geoportal/catalog/search/resource/details.page?uuid=%s'


class MetaxOAIServer(ResumptionOAIPMH):

    def _is_valid_set(self, set):
        if not set or set == 'urnresolver' or set in settings.OAI['SET_MAPPINGS']:
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

        query_set = CatalogRecord.objects.all()
        if from_ and until:
            query_set = CatalogRecord.objects.filter(date_modified__gte=from_, date_modified__lte=until)
        elif from_:
            query_set = CatalogRecord.objects.filter(date_modified__gte=from_)
        elif until:
            query_set = CatalogRecord.objects.filter(date_modified__lte=until)

        if set:
            if set == 'urnresolver':
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

    def _get_oai_dc_metadata(self, record):
        identifier = self._get_oaic_dc_value(record.research_dataset.get('preferred_identifier'))

        title = []
        title_data = record.research_dataset.get('title', {})
        for key, value in title_data.items():
            title.append(self._get_oaic_dc_value(value, key))

        creator = []
        creator_data = record.research_dataset.get('creator', [])
        for value in creator_data:
            if 'name' in value:
                creator.append(self._get_oaic_dc_value(value.get('name')))

        subject = []
        subject_data = record.research_dataset.get('keyword', [])
        for value in subject_data:
            subject.append(self._get_oaic_dc_value(value))
        subject_data = record.research_dataset.get('field_of_science', [])
        for value in subject_data:
            for key, value2 in value.get('pref_label', {}).items():
                subject.append(self._get_oaic_dc_value(value2, key))
        subject_data = record.research_dataset.get('theme', [])
        for value in subject_data:
            for key, value2 in value.get('pref_label', {}).items():
                subject.append(self._get_oaic_dc_value(value2, key))

        desc = []
        desc_data = record.research_dataset.get('description', [])
        for value in desc_data:
            for key, value2 in value.items():
                desc.append(self._get_oaic_dc_value(value2, key))

        publisher = []
        publisher_data = record.research_dataset.get('publisher', {})
        for key, value in publisher_data.get('name', {}).items():
            publisher.append(self._get_oaic_dc_value(value))

        contributor = []
        contributor_data = record.research_dataset.get('contributor', [])
        for value in contributor_data:
            if 'name' in value:
                contributor.append(self._get_oaic_dc_value(value.get('name')))

        date = self._get_oaic_dc_value(str(record.date_created))

        language = []
        language_data = record.research_dataset.get('language', [])
        for value in language_data:
            for key, value2 in value.items():
                language.append(self._get_oaic_dc_value(value2))

        relation = []
        relation_data = record.research_dataset.get('relation', [])
        for value in relation_data:
            if 'identifier'in value.get('entity', {}):
                relation.append(self._get_oaic_dc_value(value['entity']['identifier']))

        coverage = []
        coverage_data = record.research_dataset.get('spatial', [])
        for value in coverage_data:
            if 'geographic_name' in value:
                coverage.append(self._get_oaic_dc_value(value['geographic_name']))

        rights = []
        rights_data = record.research_dataset.get('access_rights', {})
        for value in rights_data.get('description', []):
            for key, value2 in value.items():
                rights.append(self._get_oaic_dc_value(value2, key))
        for value in rights_data.get('license', []):
            if 'identifier' in value:
                rights.append(self._get_oaic_dc_value(value['identifier']))

        types = []
        types.append(self._get_oaic_dc_value('Dataset'))

        meta = {
            'identifier':  [identifier],
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

    def _get_oai_datacite_metadata(self, record):
        datacite_xml = CRS.transform_datasets_to_format(
            {'research_dataset': record.research_dataset}, 'datacite', False
        )
        meta = {
            'datacentreSymbol': 'Metax',
            'schemaVersion': '4.1',
            'payload': datacite_xml
        }
        return meta

    def _get_metadata_for_record(self, record, metadata_prefix):
        meta = {}

        # strip sensitive fields from research_dataset. note: the modified research_dataset
        # is placed back into the record's research_dataset -field. meaning, an accidental call
        # of record.save() would overwrite the original data
        # record.research_dataset = CRS.strip_catalog_record(record.research_dataset)

        if metadata_prefix == 'oai_dc':
            meta = self._get_oai_dc_metadata(record)
        elif metadata_prefix == 'oai_datacite':
            meta = self._get_oai_datacite_metadata(record)
        elif metadata_prefix == 'oai_dc_urnresolver':
            meta = self._get_oai_dc_urnresolver_metadata(record)
        return self._fix_metadata(meta)

    def _get_header_timestamp(self, record):
        timestamp = None
        if record.date_modified:
            timestamp = record.date_modified
        else:
            timestamp = record.date_created
        return timezone.make_naive(timestamp)

    def _get_oai_item(self, record, metadata_prefix):
        identifier = record.identifier
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
        data = []
        for set_key in settings.OAI['SET_MAPPINGS'].keys():
            data.append((set_key, set_key, ''))
        return data

    def listIdentifiers(self, metadataPrefix=None, set=None, cursor=None,
                        from_=None, until=None, batch_size=None):
        """Implement OAI-PMH verb listIdentifiers."""
        records = self._get_filtered_records(set, cursor, batch_size, from_, until)
        data = []
        for record in records:
            identifier = record.research_dataset.get('preferred_identifier')
            data.append(common.Header('', identifier, self._get_header_timestamp(record), ['metax'], False))
        return data

    def listRecords(self, metadataPrefix=None, set=None, cursor=None, from_=None,
                    until=None, batch_size=None):
        """Implement OAI-PMH verb ListRecords."""
        data = []
        records = self._get_filtered_records(set, cursor, batch_size, from_, until)
        for record in records:
            data.append(self._get_oai_item(record, metadataPrefix))
        return data

    def getRecord(self, metadataPrefix, identifier):
        """Implement OAI-PMH verb GetRecord."""
        try:
            record = CatalogRecord.objects.get(
                # data_catalog__catalog_json__identifier__in=self._get_default_set_filter(),
                identifier__exact=identifier
            )
        except CatalogRecord.DoesNotExist:
            raise IdDoesNotExistError("No dataset with id %s available through the OAI-PMH interface." % identifier)
        metadata = self._get_metadata_for_record(record, metadataPrefix)
        return (common.Header('', identifier, self._get_header_timestamp(record), ['metax'], False),
                common.Metadata('', metadata), None)
