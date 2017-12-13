from copy import deepcopy
import logging

from django.contrib.postgres.fields import JSONField, ArrayField
from django.db import models
from rest_framework.serializers import ValidationError

from metax_api.utils import get_tz_aware_now_without_micros
from .common import Common, CommonManager
from .contract import Contract
from .data_catalog import DataCatalog
from .file import File

_logger = logging.getLogger(__name__)


class AlternateRecordSet(models.Model):

    """
    A table which contains records that share a common preferred_identifier,
    but belong to different data catalogs.

    Note! Does not inherit from model Common, so does not have timestmap fields,
    and a delete is an actual delete.
    """

    id = models.BigAutoField(primary_key=True, editable=False)


class VersionSet(models.Model):

    """
    A table which contains records that are versions of each other.

    Note! Does not inherit from model Common, so does not have timestmap fields,
    and a delete is an actual delete.
    """

    id = models.BigAutoField(primary_key=True, editable=False)


class CatalogRecordManager(CommonManager):

    def get(self, *args, **kwargs):
        if kwargs.get('using_dict', None):
            # for a simple "just get me the instance that equals this dict i have" search.
            # preferred_identifier is not a valid search key, since it wouldnt necessarily
            # work during an update (if preferred_identifier is being updated).

            # this is useful if during a request the url does not contain the identifier (bulk update),
            # and in generic operations where the type of object being handled is not known (also bulk operations).
            row = kwargs.pop('using_dict')
            if row.get('id', None):
                kwargs['id'] = row['id']
            elif row.get('research_dataset', None) and row['research_dataset'].get('urn_identifier', None):
                kwargs['research_dataset__contains'] = { 'urn_identifier': row['research_dataset']['urn_identifier'] }
            else:
                raise ValidationError(
                    'this operation requires an identifying key to be present: '
                    'id, or research_dataset ->> urn_identifier')
        return super(CatalogRecordManager, self).get(*args, **kwargs)


class CatalogRecord(Common):

    PRESERVATION_STATE_NOT_IN_PAS = 0
    PRESERVATION_STATE_PROPOSED_MIDTERM = 1
    PRESERVATION_STATE_PROPOSED_LONGTERM = 2
    PRESERVATION_STATE_IN_PACKAGING_SERVICE = 3
    PRESERVATION_STATE_IN_DISSEMINATION = 4
    PRESERVATION_STATE_IN_MIDTERM_PAS = 5
    PRESERVATION_STATE_IN_LONGTERM_PAS = 6
    PRESERVATION_STATE_LONGTERM_PAS_REJECTED = 7
    PRESERVATION_STATE_MIDTERM_PAS_REJECTED = 8

    PRESERVATION_STATE_CHOICES = (
        (PRESERVATION_STATE_NOT_IN_PAS, 'Not in PAS'),
        (PRESERVATION_STATE_PROPOSED_MIDTERM, 'Proposed for midterm'),
        (PRESERVATION_STATE_PROPOSED_LONGTERM, 'Proposed for longterm'),
        (PRESERVATION_STATE_IN_PACKAGING_SERVICE, 'In packaging service'),
        (PRESERVATION_STATE_IN_DISSEMINATION, 'In dissemination'),
        (PRESERVATION_STATE_IN_MIDTERM_PAS, 'In midterm PAS'),
        (PRESERVATION_STATE_IN_LONGTERM_PAS, 'In longterm PAS'),
        (PRESERVATION_STATE_LONGTERM_PAS_REJECTED, 'Longterm PAS rejected'),
        (PRESERVATION_STATE_MIDTERM_PAS_REJECTED, 'Midterm PAS rejected'),
    )

    # MODEL FIELD DEFINITIONS #

    alternate_record_set = models.ForeignKey(
        AlternateRecordSet, on_delete=models.SET_NULL, null=True, related_name='records',
        help_text='Records which are duplicates of this record, but in another catalog.')

    contract = models.ForeignKey(Contract, null=True, on_delete=models.DO_NOTHING)

    data_catalog = models.ForeignKey(DataCatalog, on_delete=models.DO_NOTHING)

    dataset_group_edit = models.CharField(
        max_length=200, blank=True, null=True,
        help_text='Group which is allowed to edit the dataset in this catalog record.')

    deprecated = models.BooleanField(
        default=False, help_text='Is True when files attached to a dataset have been deleted in IDA.')

    files = models.ManyToManyField(File)

    mets_object_identifier = ArrayField(models.CharField(max_length=200), null=True)

    editor = JSONField(null=True, help_text='Editor specific fields, such as owner_id, modified, record_identifier')

    preservation_description = models.CharField(
        max_length=200, blank=True, null=True, help_text='Reason for accepting or rejecting PAS proposal.')

    preservation_reason_description = models.CharField(
        max_length=200, blank=True, null=True, help_text='Reason for PAS proposal from the user.')

    preservation_state = models.IntegerField(
        choices=PRESERVATION_STATE_CHOICES, default=PRESERVATION_STATE_NOT_IN_PAS, help_text='Record state in PAS.')

    preservation_state_modified = models.DateTimeField(null=True, help_text='Date of last preservation state change.')

    research_dataset = JSONField()

    next_version = models.OneToOneField('self', on_delete=models.DO_NOTHING, null=True,
        related_name='+')

    previous_version = models.OneToOneField('self', on_delete=models.DO_NOTHING, null=True,
        related_name='+')

    version_set = models.ForeignKey(
        VersionSet, on_delete=models.SET_NULL, null=True, related_name='records',
        help_text='Records which are versions of each other.')

    # END OF MODEL FIELD DEFINITIONS #

    """
    Can be set on an instance when updates are requested without creating new versions,
    when a new version would otherwise normally be created. Has no effect on create operations.
    Note: Has to be explicitly set before calling instance.save() on the record!
    """
    preserve_version = False

    objects = CatalogRecordManager()

    class Meta:
        ordering = ['id']

    def __init__(self, *args, **kwargs):
        super(CatalogRecord, self).__init__(*args, **kwargs)
        self.track_fields(
            'preservation_state',
            'research_dataset',
            'research_dataset.files',
            'research_dataset.total_byte_size',
            'research_dataset.urn_identifier',
            'research_dataset.preferred_identifier',
        )

    def save(self, *args, **kwargs):
        if self._operation_is_create():
            self._pre_create_operations()
            super(CatalogRecord, self).save(*args, **kwargs)
            self._post_create_operations()
        else:
            self._pre_update_operations()
            super(CatalogRecord, self).save(*args, **kwargs)

    def save_as_new_version(self):
        super(CatalogRecord, self).save()
        self._post_create_operations()

        if self.field_changed('research_dataset.preferred_identifier'):
            self._handle_preferred_identifier_changed()

    def delete(self, *args, **kwargs):
        if self.has_alternate_records():
            self._remove_from_alternate_record_set()
        super(CatalogRecord, self).delete(*args, **kwargs)

    def can_be_proposed_to_pas(self):
        return self.preservation_state in (
            CatalogRecord.PRESERVATION_STATE_NOT_IN_PAS,
            CatalogRecord.PRESERVATION_STATE_LONGTERM_PAS_REJECTED,
            CatalogRecord.PRESERVATION_STATE_MIDTERM_PAS_REJECTED)

    @property
    def preferred_identifier(self):
        try:
            return self.research_dataset['preferred_identifier']
        except:
            return None

    @property
    def urn_identifier(self):
        try:
            return self.research_dataset['urn_identifier']
        except:
            return None

    def catalog_versions_datasets(self):
        return self.data_catalog.catalog_json.get('dataset_versioning', False) is True

    def has_alternate_records(self):
        return bool(self.alternate_record_set)

    def has_versions(self):
        return bool(self.version_set)

    def _calculate_total_byte_size(self):
        """
        Take identifiers from research_dataset.files, and check file sizes of the files from the db.
        """
        rd = self.research_dataset
        if rd.get('files', None):
            file_identifiers = [ f['identifier'] for f in rd['files'] ]
            file_sizes = File.objects.filter(identifier__in=file_identifiers).values_list('byte_size', flat=True)
            rd['total_byte_size'] = sum(file_sizes)
        else:
            rd['total_byte_size'] = 0

    def _pre_create_operations(self):
        # There used to be more stuff here... Let it be like this
        self._calculate_total_byte_size()

    def _post_create_operations(self):
        self._generate_urn_identifier()

        other_record = self._check_alternate_records()
        if other_record:
            self._create_or_update_alternate_record_set(other_record)

        super(CatalogRecord, self).save()

    def _pre_update_operations(self):
        if self.field_changed('research_dataset.urn_identifier'):
            # read-only after creating
            self.research_dataset['urn_identifier'] = self._initial_data['research_dataset']['urn_identifier']

        if self.field_changed('research_dataset.total_byte_size'):
            # read-only
            self.research_dataset['total_byte_size'] = self._initial_data['research_dataset']['total_byte_size']

        if self.field_changed('preservation_state'):
            self.preservation_state_modified = get_tz_aware_now_without_micros()

        if self.catalog_versions_datasets() and not self.preserve_version:
            if self.field_changed('research_dataset'):
                self._create_new_version()
        else:
            if self.field_changed('research_dataset.preferred_identifier'):
                self._handle_preferred_identifier_changed()

    def _create_new_version(self):
        """
        stuff
        """
        _logger.info('Creating new version from CatalogRecord %s...' % self.urn_identifier)

        new_version = CatalogRecord.objects.get(pk=self.id)
        new_version.id = None
        new_version.contract = None
        new_version.date_created = self.date_modified
        new_version.next_version = None
        new_version.user_created = self.user_modified
        new_version.user_modified = None
        new_version.preservation_description = None
        new_version.preservation_state = 0
        new_version.preservation_state_modified = None
        new_version.previous_version = self
        new_version.research_dataset = deepcopy(self.research_dataset)
        new_version.research_dataset.pop('urn_identifier')
        new_version.service_created = self.service_modified or self.service_created
        new_version.service_modified = None

        # some of the old editor fields cant be true in the new version, so keep
        # only the ones that make sense. it is up to the editor, to update other fields
        # they see as relevant. we also dont want null values in there
        old_editor = deepcopy(new_version.editor)
        new_version.editor = {}
        if 'owner_id' in old_editor:
            new_version.editor['owner_id'] = old_editor['owner_id']
        if 'owner_id' in old_editor:
            new_version.editor['creator_id'] = old_editor['owner_id']
        if 'identifier' in old_editor:
            new_version.editor['identifier'] = old_editor['identifier']

        if self._files_changed():
            # files changed requires that preferred_identifier is changed also. if a new value
            # is not provided by the user, preferred_identifier is removed, so that
            # urn_identifier will be copied to it once it is generated.
            if not new_version.field_changed('research_dataset.preferred_identifier'):
                new_version.research_dataset.pop('preferred_identifier', None)
            new_version._calculate_total_byte_size()

        new_version.save_as_new_version()

        if self.has_versions():
            self.version_set.records.add(new_version)
        else:
            vs = VersionSet()
            vs.save()
            vs.records.add(self, new_version)
            vs.save()

        self.next_version = new_version

        # nothing must change in the now old version of research_dataset, so copy
        # from _initial_data so that super().save() does not change it later.
        self.research_dataset = deepcopy(self._initial_data['research_dataset'])

        _logger.info('New CatalogRecord version %s created' % new_version.urn_identifier)

    def _files_changed(self):
        """
        Check if research_dataset.files have changed. Since there are objects and not simple values,
        easier to do a custom check because the target of checking is a specific field in the object.
        If a need arises, more magic can be applied to track_fields() to be more generic.

        Note: set removes duplicates. It is assumed that file listings do not include duplicate files.
        """
        if not self._field_is_loaded('research_dataset'):
            return False

        if self._field_initial_value_loaded('research_dataset'):
            initial_files = set( f['identifier'] for f in self._initial_data['research_dataset'].get('files', []) )
            received_files = set( f['identifier'] for f in self.research_dataset.get('files', []) )
            return initial_files != received_files
        else:
            self._raise_field_not_tracked_error('research_dataset.files')

        return False

    def _generate_urn_identifier(self):
        """
        Field urn_identifier in research_dataset is always generated, and it can not be changed later.
        If preferred_identifier is missing during create, copy urn_identifier to it also.
        """
        urn_identifier = self._generate_identifier('cr')
        self.research_dataset['urn_identifier'] = urn_identifier
        if not self.research_dataset.get('preferred_identifier', None):
            self.research_dataset['preferred_identifier'] = urn_identifier

    def _handle_preferred_identifier_changed(self):
        if self.has_alternate_records():
            self._remove_from_alternate_record_set()

        other_record = self._check_alternate_records()

        if other_record:
            self._create_or_update_alternate_record_set(other_record)

    def _check_alternate_records(self):
        """
        Check if there exists records in other catalogs with identical preferred_identifier
        value, and return it.

        It is enough that the first match is returned, because the related alternate_record_set
        (if it exists) can be accessed through it, or, if alternate_record_set does not exist,
        then it is the only duplicate record, and will later be used for creating the record set.

        Note: Due to some limitations in the ORM, using select_related to get CatalogRecord and
        DataCatalog is preferable to using .only() to select specific fields, since that seems
        to cause multiple queries to the db for some reason (even as many as 4!!). The query
        below makes only one query. We are only interested in alternate_record_set though, since
        fetching it now saves another query later when checking if it already exists.
        """
        return CatalogRecord.objects.select_related('data_catalog', 'alternate_record_set') \
            .filter(research_dataset__contains={ 'preferred_identifier': self.preferred_identifier }) \
            .exclude(data_catalog__id=self.data_catalog_id, id=self.id) \
            .first()

    def _create_or_update_alternate_record_set(self, other_record):
        """
        Create a new one, or update an existing alternate_records set using the
        passed other_record.
        """
        if other_record.alternate_record_set:
            # append to existing alternate record set
            other_record.alternate_record_set.records.add(self)
        else:
            # create a new set, and add the current, and the other record to it.
            # note that there should ever only be ONE other alternate record
            # at this stage, if the alternate_record_set didnt exist already.
            ars = AlternateRecordSet()
            ars.save()
            ars.records.add(self, other_record)

    def _remove_from_alternate_record_set(self):
        """
        Remove record from previous alternate_records set, and delete the record set if
        necessary.
        """
        if self.alternate_record_set.records.count() <= 2:
            # only two records in the set, so the set can be deleted.
            # delete() takes care of the references in the other record,
            # since models.SET_NULL is used.
            self.alternate_record_set.delete()

        # note: save to db occurs in delete()
        self.alternate_record_set = None

    def __repr__(self):
        return '<%s: %d, removed: %s, data_catalog: %s, urn_identifier: %s, preferred_identifier: %s >' % (
            'CatalogRecord',
            self.id,
            str(self.removed),
            self.data_catalog.catalog_json['identifier'],
            self.urn_identifier,
            self.preferred_identifier
        )
