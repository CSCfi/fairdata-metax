from datetime import datetime
from time import time

from django.contrib.postgres.fields import JSONField, ArrayField
from django.db import models

from .common import Common
from .file import File
from .dataset_catalog import DatasetCatalog
from .contract import Contract


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

    READY_STATUS_FINISHED = 'Finished'
    READY_STATUS_UNFINISHED = 'Unfinished'
    READY_STATUS_REMOVED = 'Removed'

    research_dataset = JSONField()
    dataset_catalog = models.ForeignKey(DatasetCatalog)
    contract = models.ForeignKey(Contract, on_delete=models.DO_NOTHING)
    files = models.ManyToManyField(File)
    preservation_state = models.IntegerField(choices=PRESERVATION_STATE_CHOICES, default=PRESERVATION_STATE_NOT_IN_PAS, help_text='Record state in PAS.')
    preservation_state_modified = models.DateTimeField(null=True, help_text='Date of last preservation state change.')
    preservation_description = models.CharField(max_length=200, blank=True, null=True, help_text='Reason for accepting or rejecting PAS proposal.')
    preservation_reason_description = models.CharField(max_length=200, blank=True, null=True, help_text='Reason for PAS proposal from the user.')
    mets_object_identifier = ArrayField(models.CharField(max_length=200), null=True)
    dataset_group_edit = models.CharField(max_length=200, blank=True, null=True, help_text='Group which is allowed to edit the dataset in this catalog record.')

    next_version_id = models.OneToOneField('self', on_delete=models.DO_NOTHING, null=True, db_column='next_version_id', related_name='next_version')
    next_version_identifier = models.CharField(max_length=200, null=True)
    previous_version_id = models.OneToOneField('self', on_delete=models.DO_NOTHING, null=True, db_column='previous_version_id', related_name='previous_version')
    previous_version_identifier = models.CharField(max_length=200, null=True)
    version_created = models.DateTimeField(help_text='Date when this version was first created.', null=True)

    _need_to_generate_urn_identifier = False

    class Meta:
        ordering = ['id']

    def __init__(self, *args, **kwargs):
        super(CatalogRecord, self).__init__(*args, **kwargs)
        self.track_fields('preservation_state')

        # save original urn_identifier to change the value in research_dataset back
        # to this value, in case someone is trying to change it.
        self._original_urn_identifer = self.research_dataset and self.research_dataset.get('urn_identifier', None) or None

    def save(self, *args, **kwargs):
        if self.field_changed('preservation_state'):
            self.preservation_state_modified = datetime.now()

        if self._operation_is_create():
            self._need_to_generate_urn_identifier = True
        elif self._urn_identifier_changed():
            # dont allow updating urn_identifier
            self.research_dataset['urn_identifier'] = self._original_urn_identifer

        super(CatalogRecord, self).save(*args, **kwargs)

        if self._need_to_generate_urn_identifier:
            self._generate_urn_identifier()

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

    def dataset_is_finished(self):
        return self.research_dataset.get('ready_status', False) == self.READY_STATUS_FINISHED

    def _generate_urn_identifier(self):
        """
        Field urn_identifier in research_dataset is always generated, and it can not be changed later.
        If preferred_identifier is missing during create, copy urn_identifier to it also.
        """
        urn_identifier = 'pid:urn:%d-%d' % (self.id, int(round(time() * 1000)))
        self.research_dataset['urn_identifier'] = urn_identifier
        if not self.research_dataset.get('preferred_identifier', None):
            self.research_dataset['preferred_identifier'] = urn_identifier
        super(CatalogRecord, self).save()

        # save can be called several times during an object's lifetime in a request. make sure
        # not to generate urn again.
        self._need_to_generate_urn_identifier = False

        # for the following update-operations, save value to prevent changes to urn_identifier
        self._original_urn_identifer = urn_identifier

    def _operation_is_create(self):
        return self.id is None

    def _urn_identifier_changed(self):
        return self.research_dataset.get('urn_identifier', None) != self._original_urn_identifer
