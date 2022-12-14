# Generated by Django 2.0 on 2018-08-30 12:34

from django.db import migrations, models

"""
End User Access is added to Metax.

For some objects in the db, "service_created" field is no longer
reasonable to be assumed to be always relevant. Make field nullable.
"""


class Migration(migrations.Migration):

    dependencies = [
        ('metax_api', '0002_create_json_indexes'),
    ]

    operations = [
        migrations.AlterField(
            model_name='catalogrecord',
            name='service_created',
            field=models.CharField(help_text='Name of the service who created the record', max_length=200, null=True),
        ),
        migrations.AlterField(
            model_name='contract',
            name='service_created',
            field=models.CharField(help_text='Name of the service who created the record', max_length=200, null=True),
        ),
        migrations.AlterField(
            model_name='datacatalog',
            name='service_created',
            field=models.CharField(help_text='Name of the service who created the record', max_length=200, null=True),
        ),
        migrations.AlterField(
            model_name='directory',
            name='service_created',
            field=models.CharField(help_text='Name of the service who created the record', max_length=200, null=True),
        ),
        migrations.AlterField(
            model_name='file',
            name='service_created',
            field=models.CharField(help_text='Name of the service who created the record', max_length=200, null=True),
        ),
        migrations.AlterField(
            model_name='filestorage',
            name='service_created',
            field=models.CharField(help_text='Name of the service who created the record', max_length=200, null=True),
        ),
        migrations.AlterField(
            model_name='xmlmetadata',
            name='service_created',
            field=models.CharField(help_text='Name of the service who created the record', max_length=200, null=True),
        ),
    ]
