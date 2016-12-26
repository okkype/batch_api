# -*- coding: utf-8 -*-
# Generated by Django 1.10.3 on 2016-12-25 15:16
from __future__ import unicode_literals

from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ('ui', '0016_auto_20161225_1415'),
    ]

    operations = [
        migrations.DeleteModel(
            name='BatchProc',
        ),
        migrations.DeleteModel(
            name='TicketProc',
        ),
        migrations.RemoveField(
            model_name='loadproc',
            name='load_created',
        ),
        migrations.AddField(
            model_name='loadproc',
            name='load_end',
            field=models.DateTimeField(default=django.utils.timezone.now, verbose_name='Load End'),
            preserve_default=False,
        ),
    ]
