# Generated by Django 3.1.13 on 2021-11-08 10:56

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('metax_api', '0041_add_editorpermissions'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='editoruserpermission',
            name='verification_token',
        ),
        migrations.RemoveField(
            model_name='editoruserpermission',
            name='verification_token_expires',
        ),
    ]
