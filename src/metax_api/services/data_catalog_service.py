from collections import defaultdict

from rest_framework.serializers import ValidationError

from .reference_data_mixin import ReferenceDataMixin


class DataCatalogService(ReferenceDataMixin):

    @classmethod
    def validate_reference_data(cls, data_catalog, cache):
        """
        Validate certain fields from the received data catalog against reference data, which contains
        the allowed values for these fields.

        If a field value is valid, some of the object's fields will also be populated from the cached
        reference data, overwriting possible values already entered. The fields that will be populated
        from the reference data are:

        - uri (usually to object's field 'identifier')
        - label (usually to object's field 'pref_label')

        """
        reference_data = cls.get_reference_data(cache)
        refdata = reference_data['reference_data']
        orgdata = reference_data['organization_data']
        errors = defaultdict(list)

        for language in data_catalog.get('language', []):
            ref_entry = cls.check_ref_data(refdata['language'], language['identifier'],
                                           'data_catalog_json.language.identifier', errors)
            if ref_entry:
                label_field = 'title'
                cls.populate_from_ref_data(ref_entry, language, label_field=label_field)
                cls.remove_language_obj_irrelevant_titles(language, label_field)

        for fos in data_catalog.get('field_of_science', []):
            ref_entry = cls.check_ref_data(refdata['field_of_science'], fos['identifier'],
                                           'data_catalog_json.field_of_science.identifier', errors)
            if ref_entry:
                cls.populate_from_ref_data(ref_entry, fos, label_field='pref_label')

        access_rights = data_catalog.get('access_rights', None)
        if access_rights:
            for rights_statement_type in access_rights.get('access_type', []):
                ref_entry = cls.check_ref_data(refdata['access_type'], rights_statement_type['identifier'],
                                               'data_catalog_json.access_rights.access_type.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, rights_statement_type, label_field='pref_label')

            for license in access_rights.get('license', []):
                license_url = license.get('license', None)

                ref_entry = cls.check_ref_data(refdata['license'], license['identifier'],
                                               'data_catalog_json.access_rights.license.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, license, label_field='title')

                    # Populate license field from reference data only if it is empty, i.e. not provided by the user
                    # and when the reference data uri does not contain purl.org/att
                    if not license_url and ref_entry.get('uri', False):
                        license_url = ref_entry['uri'] if 'purl.org/att' not in ref_entry['uri'] else None

                if license_url:
                    license['license'] = license_url

            if 'has_rights_related_agent' in access_rights:
                for agent in access_rights.get('has_rights_related_agent', []):
                    cls.process_org_obj_against_ref_data(orgdata['organization'], agent,
                                                         'data_catalog_json.access_rights.has_rights_related_agent')

        publisher = data_catalog.get('publisher', None)
        if publisher:
            cls.process_org_obj_against_ref_data(orgdata['organization'], publisher, 'data_catalog_json.publisher')

        if errors:
            raise ValidationError(errors)
