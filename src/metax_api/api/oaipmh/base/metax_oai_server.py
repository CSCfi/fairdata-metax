import datetime

from django.utils import timezone
from django.conf import settings
from oaipmh import common
from oaipmh.common import ResumptionOAIPMH
from oaipmh.error import IdDoesNotExistError
from oaipmh.error import BadArgumentError

from metax_api.models.catalog_record import CatalogRecord
from metax_api.services import CatalogRecordService as CRS


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

        if from_ and until:
            query_set = CatalogRecord.objects.filter(date_modified__gte=from_, date_modified__lte=until)
        elif from_:
            query_set = CatalogRecord.objects.filter(date_modified__gte=from_)
        elif until:
            query_set = CatalogRecord.objects.filter(date_modified__lte=until)

        if set and set != "urnresolver":
            query_set = query_set.filter(data_catalog__catalog_json__identifier__in=settings.OAI['SET_MAPPINGS'][set])
        else:
            query_set = query_set.filter(data_catalog__catalog_json__identifier__in=self._get_default_set_filter())
        return query_set[cursor:batch_size]

    def _get_oai_dc_urnresolver_metadata(self, record):
        """
        Preferred identifier is added only for ida and att catalog records
        other identifiers are added for all.
        """
        preferred_identifier = record.research_dataset.get('preferred_identifier')
        identifiers = []
        identifiers.append(settings.OAI['ETSIN_URL_TEMPLATE'] % preferred_identifier)

        if record.catalog_versions_datasets():
            identifiers.append(preferred_identifier)
        for id_obj in record.research_dataset.get('other_identifier', []):
            if id_obj.get('notation', '').startswith('urn:nbn:fi:csc-kata'):
                other_urn = id_obj['notation']
                identifiers.append(other_urn)

        meta = {
            'identifier':  identifiers
        }
        return meta

    def _get_oai_dc_metadata(self, record):
        identifier = record.research_dataset.get('preferred_identifier')
        meta = {
            'identifier':  [settings.OAI['ETSIN_URL_TEMPLATE'] % identifier, identifier]
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
        identifier = record.research_dataset.get('preferred_identifier')
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
                data_catalog__catalog_json__identifier__in=self._get_default_set_filter(),
                research_dataset__contains={'preferred_identifier': identifier}
            )
        except CatalogRecord.DoesNotExist:
            raise IdDoesNotExistError("No dataset with id %s available through the OAI-PMH interface." % identifier)
        metadata = self._get_metadata_for_record(record, metadataPrefix)
        identifier = record.research_dataset.get('preferred_identifier')
        return (common.Header('', identifier, self._get_header_timestamp(record), ['metax'], False),
                common.Metadata('', metadata), None)
