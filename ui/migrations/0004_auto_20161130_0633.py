# -*- coding: utf-8 -*-
# Generated by Django 1.10.3 on 2016-11-30 06:33
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('ui', '0003_auto_20161130_0627'),
    ]

    operations = [
        migrations.AlterField(
            model_name='batchconfig',
            name='name',
            field=models.CharField(choices=[('ad_client_id', 'AD Client ID'), ('ad_org_id', 'AD Org ID'), ('ad_role_id', 'AD Role ID'), ('m_warehouse_id', 'M Warehouse ID'), ('user', 'Username'), ('pass', 'Password'), ('mssql_host', 'MSSQL Host'), ('mssql_port', 'MSSQL Port'), ('mssql_db', 'MSSQL Database'), ('mssql_user', 'MSSQL Username'), ('mssql_pass', 'MSSQL Password')], max_length=40, primary_key=True, serialize=False, verbose_name='Name'),
        ),
    ]
