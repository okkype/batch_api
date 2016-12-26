from __future__ import unicode_literals

from django.db import models

# Create your models here.

class BatchConfig(models.Model):
    created = models.DateTimeField(auto_now_add=True, null=True)
    updated = models.DateTimeField(auto_now=True, null=True)
    name = models.CharField('Name', max_length=40, choices=(
        ('ad_url', 'ADempiere URL'),
        ('ad_client_id', 'AD Client ID'),
        ('ad_org_id', 'AD Org ID'),
        ('ad_role_id', 'AD Role ID'),
        ('m_warehouse_id', 'M Warehouse ID'),
        ('username', 'Username'),
        ('password', 'Password'),
        ('mssql_host', 'MSSQL Host'),
        ('mssql_port', 'MSSQL Port'),
        ('mssql_db', 'MSSQL Database'),
        ('mssql_user', 'MSSQL Username'),
        ('mssql_pass', 'MSSQL Password'),
    ), primary_key=True, unique=True)
    value = models.CharField('Value', max_length=40)
    
    class Meta:
        verbose_name = 'EBatch Config'
        
# class BatchProc(models.Model):
#     created = models.DateTimeField(auto_now_add=True, null=True)
#     updated = models.DateTimeField(auto_now=True, null=True)
#     batch_id = models.CharField('BatchID', max_length=100, primary_key=True, unique=True)
#     c_batch_id = models.CharField('BatchID', max_length=100, null=True)
#     batch_created = models.DateTimeField('CreatedDate')
#     state = models.CharField('State', max_length=2, choices=(
#         ('DR', 'New'),
#         ('CO', 'Complete'),
#         ('UP', 'Upload')
#     ), default='DR')
#       
#     class Meta:
#         verbose_name = 'EBatch Batch Proc'
#         
# class TicketProc(models.Model):
#     created = models.DateTimeField(auto_now_add=True, null=True)
#     updated = models.DateTimeField(auto_now=True, null=True)
#     ticket_id = models.CharField('TicketID', max_length=100, primary_key=True, unique=True)
#     c_ticket_id = models.CharField('Forca TicketID', max_length=100, null=True)
#     ticket_created = models.DateTimeField('CreatedDate')
#     state = models.CharField('State', max_length=2, choices=(
#         ('DR', 'New'),
#         ('CO', 'Complete'),
#         ('UP', 'Upload')
#     ), default='DR')
#     
#     class Meta:
#         verbose_name = 'EBatch Ticket Proc'
        
class LoadProc(models.Model):
    created = models.DateTimeField(auto_now_add=True, null=True)
    updated = models.DateTimeField(auto_now=True, null=True)
    load_id = models.CharField('LoadID', max_length=100, primary_key=True, unique=True)
    c_load_id = models.CharField('Forca LoadID', max_length=100, null=True)
    load_end = models.DateTimeField('Load End')
    state = models.CharField('State', max_length=2, choices=(
        ('DR', 'New'),
        ('CO', 'Complete'),
        ('UP', 'Upload')
    ), default='DR')
     
    class Meta:
        verbose_name = 'EBatch Load Proc'