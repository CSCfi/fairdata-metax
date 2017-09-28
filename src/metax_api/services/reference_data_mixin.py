
class ReferenceDataMixin():

    """
    Helper methods that other service classes can use when dealing with reference data
    """

    def check_ref_data(ref_data_type, field_to_check, relation_name, errors):
        """
        Check if the given field exists in the reference data. The value of the
        field can be either the actual uri, or a shorthand code.

        If the value is found, the ref data entry is returned, so it may later be
        used to populate the received dataset.

        If the value is not found, an error is appended to the 'errors' dict.

        params:
        ref_data_type:  the ES datatype to search from
        field_to_check: the field value being checked
        relation_name:  the full relation path to the field to hand out in case of errors
        """
        try:
            return next(entry for entry in ref_data_type if field_to_check in (entry['uri'], entry['code']))
        except StopIteration:
            errors[relation_name].append('Identifier \'%s\' not found in reference data' % field_to_check)
        return None

    def populate_from_ref_data(ref_entry, obj, uri_field='identifier', label_field=None):
        """
        Always populate at least uri field (even if it was alread there, no big deal).
        Label field population is necessary for some ref data types only.
        """
        obj[uri_field] = ref_entry['uri']
        if label_field:
            obj[label_field] = ref_entry['label']