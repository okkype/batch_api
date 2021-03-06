# -*- coding: utf-8 -*-
# Generated by Django 1.10.3 on 2016-12-01 02:28
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('ui', '0006_auto_20161130_0854'),
    ]

    operations = [
        migrations.CreateModel(
            name='BatchProc',
            fields=[
                ('created', models.DateTimeField(auto_now_add=True)),
                ('updated', models.DateTimeField(auto_now=True)),
                ('batch_id', models.CharField(max_length=100, primary_key=True, serialize=False, verbose_name='BatchID')),
                ('batch_created', models.DateTimeField(verbose_name='CreatedDate')),
                ('state', models.CharField(choices=[('DR', 'New'), ('CO', 'Complete'), ('UP', 'Upload')], max_length=2, verbose_name='State')),
            ],
        ),
    ]
