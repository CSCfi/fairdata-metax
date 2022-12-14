# Generated by Django 2.0 on 2018-09-19 12:02

from django.db import migrations, models

"""
Add new preservation_state option in CatalogRecord: 75 (Metadata confirmed)
"""


class Migration(migrations.Migration):

    dependencies = [
        ('metax_api', '0003_auto_20180830_1534'),
    ]

    operations = [
        migrations.AlterField(
            model_name='catalogrecord',
            name='preservation_state',
            field=models.IntegerField(choices=[(0, 'Initialized'), (10, 'Proposed for digital preservation'), (20, 'Technical metadata generated'), (30, 'Technical metadata generation failed'), (40, 'Invalid metadata'), (50, 'Metadata validation failed'), (60, 'Validated metadata updated'), (70, 'Valid metadata'), (75, 'Metadata confirmed'), (80, 'Accepted to digital preservation'), (90, 'in packaging service'), (100, 'Packaging failed'), (110, 'SIP sent to ingestion in digital preservation service'), (120, 'in digital preservation'), (130, 'Rejected in digital preservation service'), (140, 'in dissemination')], default=0, help_text='Record state in PAS.'),
        ),
    ]
