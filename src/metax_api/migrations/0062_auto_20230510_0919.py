# Generated by Django 3.2.18 on 2023-05-10 09:19

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('metax_api', '0061_organizationstatistics_bigintegerfield'),
    ]

    operations = [
        migrations.AddConstraint(
            model_name='directory',
            constraint=models.UniqueConstraint(fields=('directory_path', 'project_identifier'), name='directory_path_and_project'),
        ),
    ]
