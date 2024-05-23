# Generated by Django 3.2.18 on 2023-09-08 10:03

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('metax_api', '0064_auto_20230908_0950'),
    ]

    operations = [
        migrations.AlterField(
            model_name='catalogrecord',
            name='preservation_state',
            field=models.IntegerField(choices=[(0, 'Initialized'), (10, 'Generating technical metadata'), (20, 'Technical metadata generated'), (30, 'Technical metadata generation failed'), (40, 'Invalid metadata'), (50, 'Metadata validation failed'), (60, 'Validated metadata updated'), (65, 'Validating metadata'), (70, 'Rejected by user'), (75, 'Metadata confirmed'), (80, 'Accepted to digital preservation'), (90, 'in packaging service'), (100, 'Packaging failed'), (110, 'SIP sent to ingestion in digital preservation service'), (120, 'in digital preservation'), (130, 'Rejected in digital preservation service'), (140, 'in dissemination')], default=0, help_text='Record state in PAS.'),
        ),
    ]