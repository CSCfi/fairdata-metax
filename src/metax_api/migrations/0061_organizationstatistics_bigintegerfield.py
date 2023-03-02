from django.db import migrations, models

class Migration(migrations.Migration):

    dependencies = [
        ('metax_api', '0060_update_publish_attributes'),
    ]

    operations = [
        migrations.AlterField(
            model_name='organizationstatistics',
            name='byte_size_pas',
            field=models.BigIntegerField(),
        ),
        migrations.AlterField(
            model_name='organizationstatistics',
            name='byte_size_ida',
            field=models.BigIntegerField(),
        ),
    ]
