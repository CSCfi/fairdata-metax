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
from datacite import schema41 as datacite_schema41

from metax_api.exceptions import Http400
from .common_service import CommonService


_logger = logging.getLogger(__name__)


class DataciteService(CommonService):

    """
    Methods related to various aspects of Datacite metadata handling.

    In the future will probably house DOI related operations too.
    """

    @classmethod
    def to_datacite_xml(cls, catalog_record):
        """
        Convert dataset from metax dataset datamodel to datacite json datamodel, validate datacite json,
        and convert to and return datacite xml as string.

        Currently only supports datacite version 4.1.
        """
        if isinstance(catalog_record, list):
            raise Http400({ 'detail': ['datacite conversion can only be done to individual datasets, not lists.']})

        rd = catalog_record['research_dataset']

        if 'language' in rd:
            # used when trying to get most relevant name from langString when only one value is allowed.
            # note: language contains a three-letter language. we need a two-letter language, in order to
            # access the langString translations. this may not work as intended for some languages
            main_lang = rd['language'][0]['identifier'].split('http://lexvo.org/id/iso639-3/')[1][:2]
        else:
            main_lang = 'fi'

        try:
            publisher = cls._main_lang_or_default(rd['publisher']['name'], main_lang)
        except:
            publisher = cls._main_lang_or_default(rd['creator'][0]['name'], main_lang)

        """
        here contains required fields only, as specified by datacite:
        identifier
        creators
        titles
        publisher
        publicationYear
        resourceType
        """
        datacite_json = {
            'identifier': {
                # dummy-value until we start utilizing real DOI identifiers
                'identifier': '10.0/0',
                'identifierType': 'DOI',
            },
            'publicationYear': rd['modified'][0:4],
            'creators': cls._creators(rd['creator'], main_lang=main_lang),
            'titles': [
                { 'lang': lang, 'title': title } for lang, title in rd['title'].items()
            ],
            'publisher': publisher,
            'resourceType': {
                'resourceTypeGeneral': cls._resourceTypeGeneral(rd)
            }
        }

        # add optional fields

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
                datacite_json['subjects'].extend(cls._subjects(fos))
            for theme in rd.get('theme', []):
                datacite_json['subjects'].extend(cls._subjects(theme))

        if 'total_ida_byte_size' in rd:
            datacite_json['sizes'] = [str(rd['total_ida_byte_size'])]

        if 'description' in rd:
            datacite_json['descriptions'] = [
                { 'lang': lang, 'description': desc, 'descriptionType': 'Abstract' }
                for lang, desc in rd['description'].items()
            ]

        if 'license' in rd['access_rights']:
            datacite_json['rightsList'] = cls._licenses(rd['access_rights'])

        if 'language' in rd:
            datacite_json['language'] = rd['language'][0]['identifier'].split('http://lexvo.org/id/iso639-3/')[1]

        if 'contributor' in rd or 'rights_holder' in rd:
            datacite_json['contributors'] = []
            if 'contributor' in rd:
                datacite_json['contributors'].extend(cls._contributors(rd['contributor'], main_lang=main_lang))
            if 'rights_holder' in rd:
                datacite_json['contributors'].extend(cls._contributors(rd['rights_holder'],
                    contributor_type='RightsHolder', main_lang=main_lang))

        if 'spatial' in rd:
            datacite_json['geoLocations'] = cls._spatials(rd['spatial'])

        try:
            jsonschema.validate(
                datacite_json,
                cls.get_json_schema(join(dirname(dirname(__file__)), 'api/rest/base/schemas'), 'datacite_4.1')
            )
        except:
            _logger.exception('Failed to validate metax dataset -> datacite conversion json')
            raise

        # generate and return datacite xml
        return datacite_schema41.tostring(datacite_json)

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

    @classmethod
    def _creators(cls, research_agents, main_lang):
        creators = []
        for ra in research_agents:

            cr = { 'creatorName': cls._main_lang_or_default(ra['name'], main_lang=main_lang) }

            if 'identifier' in ra:
                cr['nameIdentifiers'] = [{ 'nameIdentifier': ra['identifier'], 'nameIdentifierScheme': 'URI' }]

            if 'member_of' in ra:
                cr['affiliations'] = cls._person_affiliations(ra, main_lang)

            creators.append(cr)

        return creators

    @classmethod
    def _contributors(cls, research_agents, contributor_type=None, main_lang=None):
        contributors = []
        for ra in research_agents:
            cr = { 'contributorName': cls._main_lang_or_default(ra['name'], main_lang=main_lang) }

            if contributor_type:
                cr['contributorType'] = contributor_type
            elif 'contributor_role' in ra:
                cr['contributorType'] = cls._dc_contributor_type(ra['contributor_role']['identifier'])

            if 'identifier' in ra:
                cr['nameIdentifiers'] = [{ 'nameIdentifier': ra['identifier'], 'nameIdentifierScheme': 'URI' }]

            if 'member_of' in ra:
                cr['affiliations'] = cls._person_affiliations(ra, main_lang)

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
    def _dc_contributor_type(metax_contributor_role):
        """
        Probably needs to be some kind of mapping from metax types to dc types...
        """
        return 'Other'

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
