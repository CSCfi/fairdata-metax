# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT
import logging
import re
from os.path import dirname, join

import jsonschema
from datacite import schema41 as datacite_schema41, DataCiteMDSClient
from django.conf import settings as django_settings

from metax_api.exceptions import Http400
from metax_api.utils import extract_doi_from_doi_identifier, get_identifier_type, IdentifierType, \
    executing_travis, executing_test_case
from .common_service import CommonService


_logger = logging.getLogger(__name__)


def DataciteService(*args, **kwargs):
    """
    A factory for the Datacite service, which is capable of interacting with Datacite API and converting catalog records
    into datacite format.
    """
    if executing_travis() or executing_test_case() or kwargs.pop('dummy', False):
        return _DataciteServiceDummy(*args, **kwargs)
    else:
        return _DataciteService(*args, **kwargs)


class _DataciteService(CommonService):

    """
    Methods related to various aspects of Datacite metadata handling.
    """

    def __init__(self, settings=django_settings):
        if not isinstance(settings, dict):
            if hasattr(settings, 'DATACITE'):
                settings = settings.DATACITE
            else:
                raise Exception('Missing configuration from settings.py: DATACITE')

        if not settings.get('USERNAME', None):
            raise Exception('Missing configuration from settings for DATACITE: USERNAME')
        if not settings.get('PASSWORD', None):
            raise Exception('Missing configuration from settings for DATACITE: PASSWORD')
        if not settings.get('PREFIX', None):
            raise Exception('Missing configuration from settings for DATACITE: PREFIX')

        self.user = settings['USERNAME']
        self.pw = settings['PASSWORD']

        self.mds = DataCiteMDSClient(
            username=self.user,
            password=self.pw,
            prefix=settings['PREFIX'],
            test_mode=False)

    def create_doi_metadata(self, datacite_xml_metadata):
        """
        Create DOI and its metadata to Datacite. DOI docs state that this results in the DOI to enter the "draft" state.

        :param datacite_xml_metadata:
        :return:
        """
        self.mds.metadata_post(datacite_xml_metadata)

    def register_doi_url(self, doi, location_url):
        """
        Set URL for the DOI, meaning the URL to which e.g. doi.org/<doi> will resolve to, to Datacite.
        Transition from "draft" state to "findable" state, meaning DOI will become resolvable.
        DOI docs say metadata should exist before the following request is done, but it is unknown what
        happens if this request is done before metadata exists. Or whether the metadata
        is needed at all to make the DOI "findable".

        :param doi:
        :param location_url:
        :return:
        """
        self.mds.doi_post(doi, location_url)

    def delete_doi_metadata(self, doi):
        """
        Transition DOI from "findable" state to "registered" state by deleting the DOI metadata from Datacite.
        If DOI is in "draft" state, should not do anything.

        :param doi_identifier:
        :return:
        """
        self.mds.metadata_delete(doi)

    def delete_draft_doi(self, doi):
        """
        Delete DOI that is in "draft" state from Datacite.

        :param doi:
        :return:
        """
        from requests import delete
        try:
            delete('https://mds.datacite.org/doi/{0}'.format(doi),
                   headers={'Content-Type': 'application/plain;charset=UTF-8'}, auth=(self.user, self.pw))
        except:
            pass

    def convert_catalog_record_to_datacite_xml(self, catalog_record, include_xml_declaration):
        """
        Convert dataset from metax dataset datamodel to datacite json datamodel, validate datacite json,
        and convert to and return datacite xml as string.

        Currently only supports datacite version 4.1.
        """
        if isinstance(catalog_record, list):
            raise Http400({ 'detail': ['datacite conversion can only be done to individual datasets, not lists.']})

        rd = catalog_record['research_dataset']
        pref_id = rd['preferred_identifier']

        if 'language' in rd:
            # used when trying to get most relevant name from langString when only one value is allowed.
            # note: language contains a three-letter language. we need a two-letter language, in order to
            # access the langString translations. this may not work as intended for some languages
            main_lang = rd['language'][0]['identifier'].split('http://lexvo.org/id/iso639-3/')[1][:2]
        else:
            main_lang = 'fi'

        try:
            publisher = self._main_lang_or_default(rd['publisher']['name'], main_lang)
        except:
            publisher = self._main_lang_or_default(rd['creator'][0]['name'], main_lang)

        """
        here contains required fields only, as specified by datacite:
        identifier
        creators
        titles
        publisher
        publicationYear
        resourceType
        """

        # 10.0/0 is a dummy value for catalog records that do not have a DOI preferred identifier.
        # This is done so because datacite xml won't validate unless there is some (any) DOI in identifier field
        is_doi = get_identifier_type(pref_id) == IdentifierType.DOI
        identifier_value = extract_doi_from_doi_identifier(pref_id) if is_doi else '10.0/0'
        datacite_json = {
            'identifier': {
                # dummy-value until we start utilizing real DOI identifiers
                'identifier': identifier_value,
                'identifierType': 'DOI',
            },
            'publicationYear': rd['modified'][0:4],
            'creators': self._creators(rd['creator'], main_lang=main_lang),
            'titles': [
                { 'lang': lang, 'title': title } for lang, title in rd['title'].items()
            ],
            'publisher': publisher,
            'resourceType': {
                'resourceTypeGeneral': self._resourceTypeGeneral(rd)
            }
        }

        # add optional fields

        if get_identifier_type(rd['preferred_identifier']) == IdentifierType.URN:
            datacite_json['alternateIdentifiers'] = [{
                'alternateIdentifier': rd['preferred_identifier'],
                'alternateIdentifierType': 'URN'
            }]

        if 'issued' in rd or 'modified' in rd:
            datacite_json['dates'] = []
            if 'issued' in rd:
                datacite_json['dates'].append({ 'dateType': 'Issued', 'date': rd['issued']})
            if 'modified' in rd:
                datacite_json['dates'].append({ 'dateType': 'Updated', 'date': rd['modified']})

        if 'keyword' in rd or 'field_of_science' in rd or 'theme' in rd:
            datacite_json['subjects'] = []
            for kw in rd.get('keyword', []):
                datacite_json['subjects'].append({ 'subject': kw })
            for fos in rd.get('field_of_science', []):
                datacite_json['subjects'].extend(self._subjects(fos))
            for theme in rd.get('theme', []):
                datacite_json['subjects'].extend(self._subjects(theme))

        if 'total_ida_byte_size' in rd:
            datacite_json['sizes'] = [str(rd['total_ida_byte_size'])]

        if 'description' in rd:
            datacite_json['descriptions'] = [
                { 'lang': lang, 'description': desc, 'descriptionType': 'Abstract' }
                for lang, desc in rd['description'].items()
            ]

        if 'license' in rd['access_rights']:
            datacite_json['rightsList'] = self._licenses(rd['access_rights'])

        if 'language' in rd:
            datacite_json['language'] = rd['language'][0]['identifier'].split('http://lexvo.org/id/iso639-3/')[1]

        if 'curator' in rd or 'contributor' in rd or 'creator' in rd or 'rights_holder' in rd or 'publisher' in rd:
            datacite_json['contributors'] = []
            if 'curator' in rd:
                datacite_json['contributors'].extend(self._contributors(rd['curator'], main_lang=main_lang))
            if 'contributor' in rd:
                datacite_json['contributors'].extend(self._contributors(rd['contributor'], main_lang=main_lang))
            if 'creator' in rd:
                datacite_json['contributors'].extend(self._contributors(rd['creator'], main_lang=main_lang))
            if 'rights_holder' in rd:
                datacite_json['contributors'].extend(self._contributors(rd['rights_holder'], main_lang=main_lang))
            if 'publisher' in rd:
                datacite_json['contributors'].extend(self._contributors(rd['publisher'], main_lang=main_lang))
        if 'spatial' in rd:
            datacite_json['geoLocations'] = self._spatials(rd['spatial'])

        try:
            jsonschema.validate(
                datacite_json,
                self.get_json_schema(join(dirname(dirname(__file__)), 'api/rest/base/schemas'), 'datacite_4.1')
            )
        except:
            _logger.exception('Failed to validate metax dataset -> datacite conversion json')
            raise

        # generate and return datacite xml

        output_xml = datacite_schema41.tostring(datacite_json)
        if not include_xml_declaration:
            # the +1 is linebreak character
            output_xml = output_xml[len("<?xml version='1.0' encoding='utf-8'?>") + 1:]
        return output_xml

    @staticmethod
    def _main_lang_or_default(field, main_lang=None):
        """
        To help with cases where only one string string is accepted, try to use main_lang of a langString.

        Param 'field' may also be a standard str field, in which case the field's value is returned.
        """
        if isinstance(field, dict):
            for lang in (main_lang, 'en', 'fi', 'und'):
                try:
                    return field[lang]
                except:
                    pass
            return field[field.keys()[0]]
        else:
            return field

    @staticmethod
    def _resourceTypeGeneral(rd):
        """
        Probably needs mapping from metax resource_type to dc resourceTypeGeneral...
        """
        return "Dataset"

    def _creators(self, research_agents, main_lang):
        creators = []
        for ra in research_agents:

            cr = { 'creatorName': self._main_lang_or_default(ra['name'], main_lang=main_lang) }

            if 'identifier' in ra:
                cr['nameIdentifiers'] = [{ 'nameIdentifier': ra['identifier'], 'nameIdentifierScheme': 'URI' }]

            if 'member_of' in ra:
                cr['affiliations'] = self._person_affiliations(ra, main_lang)

            creators.append(cr)

        return creators

    def _contributors(self, research_agents, main_lang=None):
        if isinstance(research_agents, dict):
            research_agents = [research_agents]

        contributors = []
        for ra in research_agents:
            if 'contributor_type' not in ra:
                continue

            cr_base = {'contributorName': self._main_lang_or_default(ra['name'], main_lang=main_lang)}

            if 'identifier' in ra:
                cr_base['nameIdentifiers'] = [{'nameIdentifier': ra['identifier'], 'nameIdentifierScheme': 'URI'}]

            if 'member_of' in ra:
                cr_base['affiliations'] = self._person_affiliations(ra, main_lang)

            for ct in ra.get('contributor_type', []):
                # for example, extracts from initial value:
                # http://uri.suomi.fi/codelist/fairdata/contributor_type/code/Distributor
                # and produces value: Distributor
                cr = dict(cr_base)
                cr['contributorType'] = ct['identifier'].split('contributor_type/code/')[-1]
                contributors.append(cr)

        return contributors

    @staticmethod
    def _person_affiliations(person, main_lang):
        affs = []
        try:
            # try to use main_lang of the affiliation firstly. if it fails for whatever reason,
            # stuff the affiliation information in all its translations. the datacite spec is
            # not specific at all regarding this, it does not even care about the lang of the
            # the given value.
            affs.append(person['member_of']['name'][main_lang])
        except KeyError:
            for lang, name_translation in person['member_of']['name'].items():
                affs.append(name_translation)
        return affs

    @staticmethod
    def _subjects(concept):
        subjects = []
        for lang in concept['pref_label'].keys():
            item = {}
            if lang in concept['pref_label']:
                item['schemeURI'] = concept['in_scheme']
                item['subject'] = concept['pref_label'][lang]
                item['lang'] = lang
            subjects.append(item)
        return subjects

    @staticmethod
    def _licenses(access_rights):
        licenses = []
        for license in access_rights['license']:
            for lang in license['title'].keys():
                licenses.append({
                    'lang': lang,
                    'rightsURI': license['license'],
                    'rights': license['title'][lang],
                })
        return licenses

    @staticmethod
    def _spatials(spatials):
        geolocations = []
        for spatial in spatials:
            geo_location = {}

            if 'geographic_name' in spatial:
                geo_location['geoLocationPlace'] = spatial['geographic_name']

            for wkt in spatial.get('as_wkt', []):
                if wkt.startswith('POINT'):
                    geo_location['geoLocationPoint'] = {
                        'pointLongitude': float(re.search('POINT\((.*) ', wkt, re.IGNORECASE).group(1)),
                        'pointLatitude': float(re.search(' (.*)\)', wkt, re.IGNORECASE).group(1)),
                    }
                    # only one point can be placed
                    break

                elif wkt.startswith('POLYGON'):
                    geo_location['geoLocationPolygon'] = { 'polygonPoints': [] }
                    for point in wkt.split('POLYGON')[1][2:-2].split(','):
                        longitude, latitude = point.strip().split(' ')
                        polygon_point = {
                            'pointLongitude': int(longitude),
                            'pointLatitude': int(latitude),
                        }
                        geo_location['geoLocationPolygon']['polygonPoints'].append(polygon_point)

                else:
                    # not applicable
                    pass
            geolocations.append(geo_location)

        return geolocations


class _DataciteServiceDummy(_DataciteService):
    """
        A dummy Datacite service that doesn't connect to Datacite API but is able to convert catalog records to
        datacite format.
        """

    def __init__(self, settings=django_settings):
        pass

    def create_doi_metadata(self, datacite_xml_metadata):
        pass

    def register_doi_url(self, doi, url):
        pass

    def delete_doi_metadata(self, doi):
        pass

    def delete_draft_doi(self, doi):
        pass