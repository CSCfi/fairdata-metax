from django.contrib.postgres.fields import JSONField, ArrayField
from django.db import models

from .common import Common
from .file import File
from .dataset_catalog import DatasetCatalog

class CatalogRecord(Common):

    PRESERVATION_STATE_NOT_IN_PAS = 0
    PRESERVATION_STATE_PROPOSED_MIDTERM = 1
    PRESERVATION_STATE_PROPOSED_LONGTERM = 2
    PRESERVATION_STATE_IN_PACKAGING_SERVICE = 3
    PRESERVATION_STATE_IN_DISSEMINATION = 4
    PRESERVATION_STATE_IN_MIDTERM_PAS = 5
    PRESERVATION_STATE_IN_LONGTERM_PAS = 6

    PRESERVATION_STATE_CHOICES = (
        (PRESERVATION_STATE_NOT_IN_PAS, 'Not in PAS'),
        (PRESERVATION_STATE_PROPOSED_MIDTERM, 'Proposed for midtterm'),
        (PRESERVATION_STATE_PROPOSED_LONGTERM, 'Proposed for longterm'),
        (PRESERVATION_STATE_IN_PACKAGING_SERVICE, 'In packaging service'),
        (PRESERVATION_STATE_IN_DISSEMINATION, 'In dissemination'),
        (PRESERVATION_STATE_IN_MIDTERM_PAS, 'In midterm PAS'),
        (PRESERVATION_STATE_IN_LONGTERM_PAS, 'In longterm PAS'),
    )

    identifier = models.CharField(max_length=200, unique=True)
    research_dataset = JSONField()
    dataset_catalog = models.ForeignKey(DatasetCatalog)
    files = models.ManyToManyField(File)

    preservation_state = models.IntegerField(choices=PRESERVATION_STATE_CHOICES, default=PRESERVATION_STATE_NOT_IN_PAS, help_text='Record state in PAS.')
    preservation_state_modified = models.DateTimeField(null=True, help_text='Date of last preservation state change.')
    preservation_state_description = models.CharField(max_length=200, blank=True, null=True, help_text='Reason for accepting or rejecting PAS proposal.')
    preservation_reason_description = models.CharField(max_length=200, blank=True, null=True, help_text='Reason for PAS proposal from the user.')
    contract_identifier = models.CharField(max_length=200, blank=True, null=True)
    mets_object_identifier = ArrayField(models.CharField(max_length=200), null=True)
    catalog_record_modified = models.DateTimeField(null=True, help_text='Date of last change in Catalog Record -specific fields.')
    dataset_group_edit = models.CharField(max_length=200, blank=True, null=True, help_text='Group which is allowed to edit the dataset in this catalog record.')

    indexes = [
        models.Index(fields=['identifier']),
    ]
