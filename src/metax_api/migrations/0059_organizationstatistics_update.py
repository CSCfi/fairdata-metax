# Generated by Django 3.2.10 on 2023-01-23 14:44

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('metax_api', '0058_update_fmi_catalog_name'),
    ]

    operations = [
        migrations.AddField(
            model_name='organizationstatistics',
            name='count_att',
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name='organizationstatistics',
            name='count_ida',
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name='organizationstatistics',
            name='count_other',
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name='organizationstatistics',
            name='count_pas',
            field=models.IntegerField(default=0),
        ),
        migrations.AlterField(
            model_name='organizationstatistics',
            name='count',
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name='organizationstatistics',
            name='byte_size_ida',
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name='organizationstatistics',
            name='byte_size_pas',
            field=models.IntegerField(default=0),
        ),
        migrations.RenameField(
            model_name='organizationstatistics',
            old_name='count',
            new_name='count_total'
        ),
        migrations.RenameField(
            model_name='organizationstatistics',
            old_name='byte_size',
            new_name='byte_size_total'
        )
    ]
