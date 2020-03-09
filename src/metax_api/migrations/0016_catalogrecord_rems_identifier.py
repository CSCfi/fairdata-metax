# Generated by Django 2.2.10 on 2020-02-24 12:30

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('metax_api', '0015_auto_20200218_1607'),
    ]

    operations = [
        migrations.AddField(
            model_name='catalogrecord',
            name='rems_identifier',
            field=models.CharField(default=None, help_text='Defines corresponding item in REMS service', max_length=200, null=True),
        ),
    ]
