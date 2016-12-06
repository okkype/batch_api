from django.contrib import admin
from ui import models

class BatchConfig(admin.ModelAdmin):
    list_display = ['name', 'value']

class BatchProc(admin.ModelAdmin):
    list_display = ['batch_id', 'batch_created', 'c_batch_plant_id', 'state']
    
admin.site.register(models.BatchConfig, BatchConfig)
admin.site.register(models.BatchProc, BatchProc)