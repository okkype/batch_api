from django.contrib import admin
from ui import models

class BatchConfig(admin.ModelAdmin):
    list_display = ['name', 'value']

# class BatchProc(admin.ModelAdmin):
#     list_display = ['batch_id', 'batch_created', 'c_batch_id', 'state']

class LoadProc(admin.ModelAdmin):
    list_display = ['load_id', 'load_end', 'c_load_id', 'state']
    
# class TicketProc(admin.ModelAdmin):
#     list_display = ['ticket_id', 'ticket_created', 'c_ticket_id', 'state']
    
admin.site.register(models.BatchConfig, BatchConfig)
# admin.site.register(models.BatchProc, BatchProc)
admin.site.register(models.LoadProc, LoadProc)
# admin.site.register(models.TicketProc, TicketProc)