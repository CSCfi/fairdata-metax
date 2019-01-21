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

from metax_api.utils import extract_doi_from_doi_identifier, is_metax_generated_doi_identifier, executing_travis, \
    executing_test_case, is_metax_generated_urn_identifier, datetime_to_str
from .common_service import CommonService


_logger = logging.getLogger(__name__)


def convert_cr_to_datacite_cr_json(cr):
    cr_json = {'research_dataset': cr.research_dataset}
    if cr.date_created:
        cr_json['date_created'] = datetime_to_str(cr.date_created)
    if cr.preservation_identifier:
        cr_json['preservation_identifier'] = cr.preservation_identifier

    return cr_json


def DataciteService(*args, **kwargs):
    """
    A factory for the Datacite service, which is capable of interacting with Datacite API and converting catalog records
    into datacite format.
    """
    if executing_travis() or executing_test_case() or kwargs.pop('dummy', False):
        return _DataciteServiceDummy(*args, **kwargs)
    else:
        return _DataciteService(*args, **kwargs)


class DataciteException(Exception):
    pass


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

    def get_validated_datacite_json(self, cr_json, is_strict):
        if isinstance(cr_json, list):
            raise DataciteException('Datacite conversion can only be done to individual datasets, not lists.')

        if not cr_json:
            raise DataciteException("Catalog record containing research_dataset required to convert anything "
                                    "to datacite format")

        rd = cr_json['research_dataset']

        # Figure out main lang for the dataset, if applicable
        if 'language' in rd and len(rd['language']) > 0:
            # used when trying to get most relevant name from langString when only one value is allowed.
            # note: language contains a three-letter language. we need a two-letter language, in order to
            # access the langString translations. this may not work as intended for some languages
            lid = rd['language'][0]['identifier']
            start_idx = lid.rindex('/') + 1
            main_lang = lid[start_idx:start_idx + 2]
        else:
            main_lang = None

        # Creators
        if rd.get('creator', False):
            creators = self._creators(rd['creator'], main_lang=main_lang)
        else:
            raise DataciteException('Dataset does not have a creator (field: research_dataset.creator), which is a '
                                    'required value for datacite format')

        # Titles
        if rd.get('title', False):
            titles = [{'lang': lang, 'title': title} for lang, title in rd['title'].items()]
        else:
            raise DataciteException('Dataset does not have a title (field: research_dataset.title), which is '
                                    'a required value for datacite format')

        # Publisher
        if rd.get('publisher', False):
            publisher = self._main_lang_or_default(rd['publisher']['name'], main_lang)
        elif is_strict:
            raise DataciteException('Dataset does not have a publisher (field: research_dataset.publisher), which '
                                    'is a required value for datacite format')
        else:
            publisher = self._main_lang_or_default(rd['creator'][0]['name'], main_lang)

        # Publication year
        if rd.get('issued', False):
            publication_year = rd['issued'][0:4]
        elif is_strict:
            raise DataciteException('Dataset does not have a date of issuance (field: research_dataset.issued), which '
                                    'is a required value for datacite format')
        else:
            publication_year = cr_json['date_created'][0:4]

        # Identifier
        pref_id = rd['preferred_identifier']
        identifier = cr_json.get('preservation_identifier', None) or pref_id
        is_metax_doi = is_metax_generated_doi_identifier(identifier)
        is_metax_urn = is_metax_generated_urn_identifier(identifier)

        if is_metax_doi:
            identifier_value = extract_doi_from_doi_identifier(identifier)
            identifier_type = 'DOI'
        elif not is_strict and is_metax_urn:
            identifier_value = identifier
            identifier_type = 'URN'
        else:
            raise DataciteException('Dataset does not have a valid preferred identifier (field: '
                                    'research_dataset.preferred_identifier), which should contain a Metax '
                                    'generated DOI, which is a required value for datacite format')

        """
        Required fields, as specified by datacite:

        identifier
        creators
        titles
        publisher
        publicationYear
        resourceType
        """

        datacite_json = {
            'identifier': {
                'identifier': identifier_value,
                'identifierType': identifier_type
            },
            'creators': creators,
            'titles': titles,
            'publisher': publisher,
            'publicationYear': publication_year,
            'resourceType': {
                'resourceTypeGeneral': self._resource_type_general(rd)
            }
        }

        # Optional fields

        if is_metax_generated_urn_identifier(pref_id) and pref_id != identifier:
            datacite_json['alternateIdentifiers'] = [{
                'alternateIdentifier': pref_id,
                'alternateIdentifierType': 'URN'
            }]

        if 'modified' in rd or 'available' in rd.get('access_rights', {}):
            datacite_json['dates'] = []
            if 'modified' in rd:
                datacite_json['dates'].append({'dateType': 'Updated', 'date': rd['modified']})
            if 'available' in rd.get('access_rights', {}):
                datacite_json['dates'].append({'dateType': 'Available', 'date': rd['access_rights']['available']})

        if 'keyword' in rd or 'field_of_science' in rd or 'theme' in rd:
            datacite_json['subjects'] = []
            for kw in rd.get('keyword', []):
                datacite_json['subjects'].append({'subject': kw})
            for fos in rd.get('field_of_science', []):
                datacite_json['subjects'].extend(self._subjects(fos))
            for theme in rd.get('theme', []):
                datacite_json['subjects'].extend(self._subjects(theme))

        if 'total_ida_byte_size' in rd:
            datacite_json['sizes'] = [str(rd['total_ida_byte_size'])]

        if rd.get('description', False):
            datacite_json['descriptions'] = [
                {'lang': lang, 'description': desc, 'descriptionType': 'Abstract'}
                for lang, desc in rd['description'].items()
            ]

        if 'license' in rd['access_rights']:
            datacite_json['rightsList'] = self._licenses(rd['access_rights'])

        if rd.get('language', False):
            lid = rd['language'][0]['identifier']
            datacite_json['language'] = lid[lid.rindex('/') + 1:]

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

        if is_strict:
            try:
                jsonschema.validate(
                    datacite_json,
                    self.get_json_schema(join(dirname(dirname(__file__)), 'api/rest/base/schemas'), 'datacite_4.1')
                )
            except Exception as e:
                _logger.error("Failed to validate catalog record against datacite schema")
                raise DataciteException(e)

        return datacite_json

    def convert_catalog_record_to_datacite_xml(self, cr_json, include_xml_declaration, is_strict):
        """
        Convert dataset from catalog record data model to datacite json data model. Validate the json against datacite
        schema. On success, convert and return as XML. Raise exceptions on errors.

        Currently only supports datacite version 4.1.
        """
        datacite_json = self.get_validated_datacite_json(cr_json, is_strict)

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
    def _resource_type_general(rd):
        """
        Probably needs mapping from metax resource_type to dc resourceTypeGeneral...
        """
        return "Dataset"

    def _creators(self, research_agents, main_lang):
        creators = []
        for ra in research_agents:
            cr = {'creatorName': self._main_lang_or_default(ra['name'], main_lang=main_lang)}
            if 'identifier' in ra:
                cr['nameIdentifiers'] = [{'nameIdentifier': ra['identifier'], 'nameIdentifierScheme': 'URI'}]
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
                    'rightsURI': license['license'] if 'license' in license else license['identifier'],
                    'rights': license['title'][lang]
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
                        'pointLongitude': float(re.search(r'POINT\((.*) ', wkt, re.IGNORECASE).group(1)),
                        'pointLatitude': float(re.search(r' (.*)\)', wkt, re.IGNORECASE).group(1)),
                    }
                    # only one point can be placed
                    break

                elif wkt.startswith('POLYGON'):
                    geo_location['geoLocationPolygon'] = { 'polygonPoints': [] }
                    # Split POLYGON in case it contains several polygon objects
                    for polygon in wkt.split('POLYGON')[1][2:-2].split('),('):
                        for point in polygon.split(','):
                            longitude, latitude = point.strip().split(' ')
                            polygon_point = {
                                'pointLongitude': float(longitude),
                                'pointLatitude': float(latitude),
                            }
                            geo_location['geoLocationPolygon']['polygonPoints'].append(polygon_point)
                        # Do not support for more than one polygon within one POLYGON value, for now
                        break

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
