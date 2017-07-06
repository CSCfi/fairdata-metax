from datetime import datetime

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

    identifier = models.CharField(max_length=200, unique=True)
    research_dataset = JSONField()
    dataset_catalog = models.ForeignKey(DatasetCatalog)
    contract = models.ForeignKey(Contract, on_delete=models.DO_NOTHING)
    files = models.ManyToManyField(File)
    preservation_state = models.IntegerField(choices=PRESERVATION_STATE_CHOICES, default=PRESERVATION_STATE_NOT_IN_PAS, help_text='Record state in PAS.')
    preservation_state_modified = models.DateTimeField(null=True, help_text='Date of last preservation state change.')
    preservation_state_description = models.CharField(max_length=200, blank=True, null=True, help_text='Reason for accepting or rejecting PAS proposal.')
    preservation_reason_description = models.CharField(max_length=200, blank=True, null=True, help_text='Reason for PAS proposal from the user.')
    contract_identifier = models.CharField(max_length=200, blank=True, null=True)
    mets_object_identifier = ArrayField(models.CharField(max_length=200), null=True)
    dataset_group_edit = models.CharField(max_length=200, blank=True, null=True, help_text='Group which is allowed to edit the dataset in this catalog record.')

    next_version_id = models.OneToOneField('self', on_delete=models.DO_NOTHING, null=True, db_column='next_version_id', related_name='next_version')
    next_version_identifier = models.CharField(max_length=200, null=True)
    previous_version_id = models.OneToOneField('self', on_delete=models.DO_NOTHING, null=True, db_column='previous_version_id', related_name='previous_version')
    previous_version_identifier = models.CharField(max_length=200, null=True)
    version_created = models.DateTimeField(help_text='Date when this version was first created.', null=True)

    class Meta:
        indexes = [
            models.Index(fields=['identifier'])
        ]
        ordering = ['id']

    def __init__(self, *args, **kwargs):
        super(CatalogRecord, self).__init__(*args, **kwargs)
        self.track_fields('preservation_state')

    def save(self, *args, **kwargs):
        if self.field_changed('preservation_state'):
            self.preservation_state_modified = datetime.now()
        super(CatalogRecord, self).save(*args, **kwargs)
