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
        reference_data = cache.get('reference_data')
        refdata = reference_data['reference_data']
        errors = defaultdict(list)

        for language in data_catalog.get('language', []):
            ref_entry = cls.check_ref_data(refdata['language'], language['identifier'], 'data_catalog_json.language.identifier', errors)
            if ref_entry:
                # note that catalog language does not have an applicable field for label,
                # like dataset language has
                cls.populate_from_ref_data(ref_entry, language)

        for fos in data_catalog.get('field_of_science', []):
            ref_entry = cls.check_ref_data(refdata['field_of_science'], fos['identifier'], 'data_catalog_json.field_of_science.identifier', errors)
            if ref_entry:
                cls.populate_from_ref_data(ref_entry, fos, label_field='pref_label')

        access_rights = data_catalog.get('rights', None)
        if access_rights:
            for rights_statement_type in access_rights.get('type', []):
                ref_entry = cls.check_ref_data(refdata['access_type'], rights_statement_type['identifier'], 'data_catalog_json.rights.type.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, rights_statement_type, label_field='pref_label')

            for rights_statement_license in access_rights.get('license', []):
                ref_entry = cls.check_ref_data(refdata['license'], rights_statement_license['identifier'], 'data_catalog_json.rights.license.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, rights_statement_license, label_field='title')

        if errors:
            raise ValidationError(errors)
