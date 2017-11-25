from ui.models import *

from django.core.urlresolvers import reverse_lazy
from django.shortcuts import render, redirect
from django.views.generic import TemplateView, ListView
from django.views.generic.edit import CreateView, UpdateView, DeleteView
from django.utils import timezone

from idempierewsc.request import CreateDataRequest, UpdateDataRequest
from idempierewsc.base import LoginRequest
from idempierewsc.enums import WebServiceResponseStatus
from idempierewsc.net import WebServiceConnection
from idempierewsc.base import Field

import pymssql
import time
from django.utils.datetime_safe import strftime
from django.views.decorators.csrf import csrf_exempt

import requests, zlib, json

# ad_client_id = BatchConfig.objects.get(name='ad_client_id').value
# ad_org_id = BatchConfig.objects.get(name='ad_org_id').value
# ad_role_id = BatchConfig.objects.get(name='ad_role_id').value
# ad_url = BatchConfig.objects.get(name='ad_url').value
# m_warehouse_id = BatchConfig.objects.get(name='m_warehouse_id').value
# username = BatchConfig.objects.get(name='username').value
# password = BatchConfig.objects.get(name='password').value
# mssql_host = BatchConfig.objects.get(name='mssql_host').value
# mssql_port = BatchConfig.objects.get(name='mssql_port').value
# mssql_db = BatchConfig.objects.get(name='mssql_db').value
# mssql_user = BatchConfig.objects.get(name='mssql_user').value
# mssql_pass = BatchConfig.objects.get(name='mssql_pass').value

# login = LoginRequest()
# login.client_id = ad_client_id
# login.org_id = ad_org_id
# login.role_id = ad_role_id
# login.password = password
# login.user = username
# login.warehouse_id = m_warehouse_id

loadproc_fields = ['load_id', 'c_load_id', 'ticket_code', 'load_end', 'state']
loadlineproc_fields = ['load_id', 'loadline_id', 'c_loadline_id', 'state']

class LoadProcList(ListView):
    model = LoadProc
    paginate_by = 10
        
    def get_context_data(self, **kwargs):
        context = super(LoadProcList, self).get_context_data(**kwargs)
        context['filter'] = self.request.GET.get('filter', '')
        if context['filter']:
            context['object_list'] = LoadProc.objects.filter(ticket_code__icontains=context['filter'])
        return context
    
    def get_paginate_by(self, queryset):
        if self.request.GET.get('filter'):
            return 0
        return ListView.get_paginate_by(self, queryset)
    
class LoadProcCreate(CreateView):
    model = LoadProc
    # success_url = reverse_lazy('loadproc_list')
    fields = loadproc_fields
    
    def get_success_url(self):
        return self.request.META.get('HTTP_REFERER', None) or '/'
    
    def get_initial(self):
        initial = super(LoadProcCreate, self).get_initial()
        initial['load_end'] = timezone.now()
        return initial

class LoadProcUpdate(UpdateView):
    model = LoadProc
    # success_url = reverse_lazy('loadproc_list')
    fields = loadproc_fields
    
    def get_success_url(self):
        return self.request.META.get('HTTP_REFERER', None) or '/'
    
    def get_context_data(self, **kwargs):
        context = super(LoadProcUpdate, self).get_context_data(**kwargs)
        context['object_list'] = LoadLineProc.objects.filter(load_id=self.kwargs['pk'])
        return context

class LoadProcDelete(DeleteView):
    model = LoadProc
    # success_url = reverse_lazy('loadproc_list')
    
    def get_success_url(self):
        return self.request.META.get('HTTP_REFERER', None) or '/'
    
class LoadLineProcList(ListView):
    model = LoadLineProc
    paginate_by = 10
        
    def get_context_data(self, **kwargs):
        context = super(LoadLineProcList, self).get_context_data(**kwargs)
        context['filter'] = self.request.GET.get('filter', '')
        if context['filter']:
            context['object_list'] = LoadLineProc.objects.filter(load_id__icontains=context['filter'])
        return context
    
    def get_paginate_by(self, queryset):
        if self.request.GET.get('filter'):
            return 0
        return ListView.get_paginate_by(self, queryset)
    
class LoadLineProcCreate(CreateView):
    model = LoadLineProc
    # success_url = reverse_lazy('loadproc_list')
    fields = loadlineproc_fields
    
    def get_success_url(self):
        return self.request.META.get('HTTP_REFERER', None) or '/'
    
    def get_initial(self):
        initial = super(LoadLineProcCreate, self).get_initial()
        initial['load_id'] = self.kwargs['fk']
        return initial

class LoadLineProcUpdate(UpdateView):
    model = LoadLineProc
    # success_url = reverse_lazy('loadproc_list')
    fields = loadlineproc_fields
    
    def get_success_url(self):
        return self.request.META.get('HTTP_REFERER', None) or '/'

class LoadLineProcDelete(DeleteView):
    model = LoadLineProc
    # success_url = reverse_lazy('loadproc_list')
    
    def get_success_url(self):
        return self.request.META.get('HTTP_REFERER', None) or '/'
    
def UploadLoadProcByID(request, pk):
    ad_client_id = BatchConfig.objects.get(name='ad_client_id').value
    ad_org_id = BatchConfig.objects.get(name='ad_org_id').value
    ad_role_id = BatchConfig.objects.get(name='ad_role_id').value
    ad_url = BatchConfig.objects.get(name='ad_url').value
    m_warehouse_id = BatchConfig.objects.get(name='m_warehouse_id').value
    username = BatchConfig.objects.get(name='username').value
    password = BatchConfig.objects.get(name='password').value
    mssql_host = BatchConfig.objects.get(name='mssql_host').value
    mssql_port = BatchConfig.objects.get(name='mssql_port').value
    mssql_db = BatchConfig.objects.get(name='mssql_db').value
    mssql_user = BatchConfig.objects.get(name='mssql_user').value
    mssql_pass = BatchConfig.objects.get(name='mssql_pass').value
    # combatchmachine = BatchConfig.objects.get(name='combatchmachine') and BatchConfig.objects.get(name='combatchmachine').value or ''
    
    try:
        combatchmachine = BatchConfig.objects.get(name='combatchmachine') and BatchConfig.objects.get(name='combatchmachine').value or ''
    except:
        combatchmachine = ''
    try:
        mssql_limit_by = BatchConfig.objects.get(name='mssql_limit_by') and BatchConfig.objects.get(name='mssql_limit_by').value or 'DAY'
    except:
        mssql_limit_by = 'DAY'
    try:
        mssql_limit = BatchConfig.objects.get(name='mssql_limit') and BatchConfig.objects.get(name='mssql_limit').value or '-7'
    except:
        mssql_limit = '-7'

    try:
        mssql_lomit_by = BatchConfig.objects.get(name='mssql_lomit_by') and BatchConfig.objects.get(name='mssql_lomit_by').value or 'MINUTE'
    except:
        mssql_lomit_by = 'MINUTE'
    try:
        mssql_lomit = BatchConfig.objects.get(name='mssql_lomit') and BatchConfig.objects.get(name='mssql_lomit').value or '-1'
    except:
        mssql_lomit = '-1'
    
    uuid = requests.post('%s/crm-ws/gettoken' % (ad_url.strip('/')), data={
        'username':username,
        'password':password,
        'client_id':ad_client_id,
        'role_id':ad_role_id,
        'org_id':ad_org_id,
        'warehouse_id':m_warehouse_id
    })
    
    cn = pymssql.connect(
        server=mssql_host,
        user=mssql_user,
        password=mssql_pass,
        database=mssql_db
    )
    procs = LoadProc.objects.filter(load_id=pk)
    for proc in procs:
        load_ok = False
        proc.state = 'CO'

        cr = cn.cursor(as_dict=True)
        cr.execute("""
            SELECT
                *
            FROM
                LOAD,
                TICKET_LINE,
                TICKET
            WHERE
                LOAD.LoadID = TICKET_LINE.LoadID
                AND TICKET.Ticket_Code <> 0
                AND TICKET_LINE.TicketID = TICKET.TicketID
                AND TICKET_LINE.Delete_Flag = 0
                AND LOAD.LoadID LIKE '%s';
        """ % (proc.load_id))
        data = cr.fetchone()
        if data:
            time.sleep(0.01)
            try:
                    
                check = requests.post('%s/crm-ws/json/get-info-token' % (ad_url.strip('/')), data={'uuid':uuid.text})
                if 'berhasil' == json.loads(check.content)['message'].lower():
                    pass
                else:
                    uuid = requests.post('%s/crm-ws/gettoken' % (ad_url.strip('/')), data={
                        'username':username,
                        'password':password,
                        'client_id':ad_client_id,
                        'role_id':ad_role_id,
                        'org_id':ad_org_id,
                        'warehouse_id':m_warehouse_id
                        })
                
#                     response = wsc.send_request(ws)
#                     wsc.print_xml_request()
#                     wsc.print_xml_response()

                createHeader = requests.post('%s/crm-ws/create' % (ad_url.strip('/')), data={
                    'uuid':uuid.text,
                    'table':'C_Batch_Load',
                    'fields':'''[{
                                    "C_Batch_Load_ID":"@Generate_ID=C_Batch_Load@",
                                    "C_Batch_Load_UU":"\'@Generate_UUID@\'",
                                    "AD_Client_ID":"@AD_Client_ID@",
                                    "AD_Org_ID":"@AD_Org_ID@",
                                    "Created":"NOW()",
                                    "CreatedBy":"@AD_User_ID@",
                                    "Updated":"NOW()",
                                    "UpdatedBy":"@AD_User_ID@",
                                    "Alias_Code":"\'%s\'",
                                    "Apply_Add_Trim_Flag":"\'%s\'",
                                    "Archive_Flag":"\'%s\'",
                                    "Auto_TDS":"\'%s\'",
                                    "BCP_Load_ID":"\'%s\'",
                                    "BCP_Load_Sort":"\'%s\'",
                                    "Calc_Ack_Counter":"\'%s\'",
                                    "Calc_Req_Counter":"\'%s\'",
                                    "Charge_Rate_Percent":"\'%s\'",
                                    "Control_Sys_Code":"\'%s\'",
                                    "Control_SysID":"\'%s\'",
                                    "CreateDate":"\'%s\'",
                                    "CreatedBy_Batch":"\'%s\'",
                                    "DemandID":"\'%s\'",
                                    "Design_Slump":"\'%s\'",
                                    "Ended_Load_Status":"\'%s\'",
                                    "Failed_Flag":"\'%s\'",
                                    "Load_Code":"\'%s\'",
                                    "Load_Discharge_TDS":"\'%s\'",
                                    "Load_End_TDS":"\'%s\'",
                                    "Load_Size":"\'%s\'",
                                    "Load_Sort_Num":"\'%s\'",
                                    "Load_Start_TDS":"\'%s\'",
                                    "LoadID":"\'%s\'",
                                    "LoadStarted_UserCode":"\'%s\'",
                                    "Manual_Feed_Flag":"\'%s\'",
                                    "Manual_TDS":"\'%s\'",
                                    "Max_Batch":"\'%s\'",
                                    "Max_Batch_Size":"\'%s\'",
                                    "Max_Batch_Size_UOM":"\'%s\'",
                                    "Max_Dispatch_WCR":"\'%s\'",
                                    "Mix_Entry_Ref_Type":"\'%s\'",
                                    "Mixer_TS":"\'%s\'",
                                    "NoteExistsFlag":"\'%s\'",
                                    "Notify_UserCode":"\'%s\'",
                                    "RecordDate":"\'%s\'",
                                    "ReferenceLoadID":"\'%s\'",
                                    "Reship_Batched_TDS":"\'%s\'",
                                    "Reship_Incompatible_Flag":"\'%s\'",
                                    "Reship_Item_Code":"\'%s\'",
                                    "Reship_Item_Description":"\'%s\'",
                                    "Reship_ItemID":"\'%s\'",
                                    "Reship_Load_Code":"\'%s\'",
                                    "Reship_LoadID":"\'%s\'",
                                    "Reship_Qty":"\'%s\'",
                                    "Reship_Qty_UOM":"\'%s\'",
                                    "Reship_Source_Type":"\'%s\'",
                                    "RowPointer":"\'%s\'",
                                    "Run_Ack_Counter":"\'%s\'",
                                    "Run_Req_Counter":"\'%s\'",
                                    "Sequence_Code":"\'%s\'",
                                    "SequenceID":"\'%s\'",
                                    "Sequencing_Flag":"\'%s\'",
                                    "Simulate_Status":"\'%s\'",
                                    "Slump":"\'%s\'",
                                    "Slump_Code":"\'%s\'",
                                    "Slump_UOM":"\'%s\'",
                                    "SlumpID":"\'%s\'",
                                    "Started_Load_Status":"\'%s\'",
                                    "UpdatedBy_Batch":"\'%s\'",
                                    "Washout_Used_Flag":"\'%s\'",
                                    "Water_Cement_Ratio":"\'%s\'",
                                    "Water_In_Truck":"\'%s\'",
                                    "Water_In_Truck_UOM":"\'%s\'",
                                    "Item_Code":"\'%s\'",
                                    "Ticket_Code":"\'%s\'",
                                    "M_Warehouse_ID":"\'%s\'",
                                    "ComBatchMachine":"\'%s\'",
                                    "IsActive":"\'%s\'"
                                }]''' % (
                                    (data['Alias_Code'] or '0'),
                                    (data['Apply_Add_Trim_Flag'] and 'Y' or 'N'),
                                    (data['Archive_Flag'] and 'Y' or 'N'),
                                    (data['Auto_TDS'] or '1970-01-01 00:00:00'),
                                    (data['BCP_Load_ID'] or '0'),
                                    (data['BCP_Load_Sort'] or '0'),
                                    (data['Calc_Ack_Counter'] or '0'),
                                    (data['Calc_Req_Counter'] or '0'),
                                    (data['Charge_Rate_Percent'] or '0'),
                                    (data['Control_Sys_Code'] or '0'),
                                    (data['Control_SysID'] or '0'),
                                    (data['CreateDate'] or '1970-01-01 00:00:00'),
                                    (data['CreatedBy'] or '0'),
                                    (data['DemandID'] or '0'),
                                    (data['Design_Slump'] or '0'),
                                    (data['Ended_Load_Status'] or '0'),
                                    (data['Failed_Flag'] and 'Y' or 'N'),
                                    (data['Load_Code'] or '0'),
                                    (data['Load_Discharge_TDS'] or '1970-01-01 00:00:00'),
                                    (data['Load_End_TDS'] or '1970-01-01 00:00:00'),
                                    (data['Load_Size'] or '0'),
                                    (data['Load_Sort_Num'] or '0'),
                                    (data['Load_Start_TDS'] or '1970-01-01 00:00:00'),
                                    (data['LoadID'] or '0'),
                                    (data['LoadStarted_UserCode'] or '0'),
                                    (data['Manual_Feed_Flag'] and 'Y' or 'N'),
                                    (data['Manual_TDS'] or '1970-01-01 00:00:00'),
                                    (data['Max_Batch'] or '0'),
                                    (data['Max_Batch_Size'] or '0'),
                                    (data['Max_Batch_Size_UOM'] or '0'),
                                    (data['Max_Dispatch_WCR'] or '0'),
                                    (data['Mix_Entry_Ref_Type'] or '0'),
                                    (data['Mixer_TS'] or '0'),
                                    (data['NoteExistsFlag'] and 'Y' or 'N'),
                                    (data['Notify_UserCode'] or '0'),
                                    (data['RecordDate'] or '1970-01-01 00:00:00'),
                                    (data['ReferenceLoadID'] or '0'),
                                    (data['Reship_Batched_TDS'] or '1970-01-01 00:00:00'),
                                    (data['Reship_Incompatible_Flag'] and 'Y' or 'N'),
                                    (data['Reship_Item_Code'] or '0'),
                                    (data['Reship_Item_Description'] or '0'),
                                    (data['Reship_ItemID'] or '0'),
                                    (data['Reship_Load_Code'] or '0'),
                                    (data['Reship_LoadID'] or '0'),
                                    (data['Reship_Qty'] or '0'),
                                    (data['Reship_Qty_UOM'] or '0'),
                                    (data['Reship_Source_Type'] or '0'),
                                    (data['RowPointer'] or '0'),
                                    (data['Run_Ack_Counter'] or '0'),
                                    (data['Run_Req_Counter'] or '0'),
                                    (data['Sequence_Code'] or '0'),
                                    (data['SequenceID'] or '0'),
                                    (data['Sequencing_Flag'] and 'Y' or 'N'),
                                    (data['Simulate_Status'] or '0'),
                                    (data['Slump'] or '0'),
                                    (data['Slump_Code'] or '0'),
                                    (data['Slump_UOM'] or '0'),
                                    (data['SlumpID'] or '0'),
                                    (data['Started_Load_Status'] or '0'),
                                    (data['UpdatedBy'] or '0'),
                                    (data['Washout_Used_Flag'] and 'Y' or 'N'),
                                    (data['Water_Cement_Ratio'] or '0'),
                                    (data['Water_In_Truck'] or '0'),
                                    (data['Water_In_Truck_UOM'] or '0'),
                                    (data['Item_Code'] or '0'),
                                    (data['Ticket_Code'] or '0'),
                                    m_warehouse_id,
                                    combatchmachine,
                                    'N'
                                ),
                    'returning':'C_Batch_Load_ID'
                    })
               
#                     if response.status == WebServiceResponseStatus.Error:
#                         print('Error: ' + response.error_message)
#                     else:
#                         print('RecordID: ' + str(response.record_id))
#                         for field in response.output_fields:
#                             print(str(field.column) + ': ' + str(field.value))
#                         print('---------------------------------------------')
#                         print('Web Service Type: ' + ws.web_service_type)
#                         print('Attempts: ' + str(wsc.attempts_request))
#                         print('Time: ' + str(wsc.time_request))
#                         proc.c_load_id = response.record_id
#                         load_ok = True
                try:
                    if createHeader.content and zlib.decompress(createHeader.content) and json.loads(zlib.decompress(createHeader.content)) and json.loads(zlib.decompress(createHeader.content))[0]['C_Batch_Load_ID'.lower()]:
                        proc.c_load_id = json.loads(zlib.decompress(createHeader.content))[0]['C_Batch_Load_ID'.lower()]
                        proc.save()
                        print "Success Create Header ID.%s" % (proc.c_load_id)
#                         load_ok = True
                    else:
                        print "Error Create Header"
                except:
                    print "Server Error Create Header"
                    print createHeader.content and zlib.decompress(createHeader.content)
                finally:
                    pass
            except Exception as e:
                print(e)
            finally:
                pass

            cr.close()
    
    proc_lines = LoadLineProc.objects.filter(load_id__in=procs.values_list('load_id', flat=True))
    for proc_line in proc_lines:
        loadline_ok = False
        proc_line.state = 'CO'
        
        cr = cn.cursor(as_dict=True)
        cr.execute("select * from LOAD_LINE where Load_LineID like '%s' and Delete_Flag = 0" % (proc_line.loadline_id))
        data = cr.fetchone()
        
        if data:
            time.sleep(0.01)
            try:
#                     if response.status == WebServiceResponseStatus.Error:
#                         print('Error: ' + response.error_message)
#                     else:
#                         print('RecordID: ' + str(response.record_id))
#                         for field in response.output_fields:
#                             print(str(field.column) + ': ' + str(field.value))
#                         print('---------------------------------------------')
#                         print('Web Service Type: ' + ws.web_service_type)
#                         print('Attempts: ' + str(wsc.attempts_request))
#                         print('Time: ' + str(wsc.time_request))
#                         proc_line.c_loadline_id = response.record_id
#                         proc_line.save()

                createLine = requests.post('%s/crm-ws/create' % (ad_url.strip('/')), data={
                    'uuid':uuid.text,
                    'table':'C_Batch_LoadLine',
                    'fields':'''[{
                                    "C_Batch_LoadLine_ID":"@Generate_ID=C_Batch_LoadLine@",
                                    "C_Batch_LoadLine_UU":"\'@Generate_UUID@\'",
                                    "AD_Client_ID":"@AD_Client_ID@",
                                    "AD_Org_ID":"@AD_Org_ID@",
                                    "Created":"NOW()",
                                    "CreatedBy":"@AD_User_ID@",
                                    "Updated":"NOW()",
                                    "UpdatedBy":"@AD_User_ID@",
                                    "C_Batch_Load_ID":"\'%s\'",
                                    "Absorbed_Water":"\'%s\'",
                                    "Absorption_Percent":"\'%s\'",
                                    "Actual_Water":"\'%s\'",
                                    "Actual_Water_Calc_Type":"\'%s\'",
                                    "Adjust_UOM":"\'%s\'",
                                    "Alias_Code":"\'%s\'",
                                    "Amt_UOM":"\'%s\'",
                                    "Archive_Flag":"\'%s\'",
                                    "Based_On_Factor":"\'%s\'",
                                    "Based_On_Qty":"\'%s\'",
                                    "Based_On_UOM":"\'%s\'",
                                    "Calc_Factor":"\'%s\'",
                                    "Calc_Moisture_Percent":"\'%s\'",
                                    "Correction_Factor":"\'%s\'",
                                    "CreateDate":"\'%s\'",
                                    "CreatedBy_Batch":"\'%s\'",
                                    "Delete_Flag":"\'%s\'",
                                    "Design_Absorbed_Water":"\'%s\'",
                                    "Design_Entry_Qty":"\'%s\'",
                                    "Design_Free_Water":"\'%s\'",
                                    "Design_SSD_Net_Target_Qty":"\'%s\'",
                                    "Design_SSD_Qty":"\'%s\'",
                                    "Design_UOM":"\'%s\'",
                                    "Dispatch_Design_Qty":"\'%s\'",
                                    "Dispatch_Design_UOM":"\'%s\'",
                                    "Do_Not_Batch_Flag":"\'%s\'",
                                    "Effectiveness_Percent":"\'%s\'",
                                    "Ingred_ItemID":"\'%s\'",
                                    "Ingredient_Source_Type":"\'%s\'",
                                    "Item_Code":"\'%s\'",
                                    "Item_Description":"\'%s\'",
                                    "Kgs_Per_Liter":"\'%s\'",
                                    "Load_Adjust_Qty":"\'%s\'",
                                    "Load_LineID":"\'%s\'",
                                    "LoadID":"\'%s\'",
                                    "Manual_Feed_Flag":"\'%s\'",
                                    "Modified_Flag":"\'%s\'",
                                    "Moisture_Entry_Ref_Type":"\'%s\'",
                                    "Net_Auto_Batched_Amt":"\'%s\'",
                                    "Net_Batched_Amt":"\'%s\'",
                                    "Net_Target_Amt":"\'%s\'",
                                    "Net_Used_Amt":"\'%s\'",
                                    "NoteExistsFlag":"\'%s\'",
                                    "RecordDate":"\'%s\'",
                                    "RowPointer":"\'%s\'",
                                    "Scale_UOM":"\'%s\'",
                                    "Slump_Factor":"\'%s\'",
                                    "Solids_Specific_Gravity":"\'%s\'",
                                    "Sort_Line_Num":"\'%s\'",
                                    "Specific_Gravity":"\'%s\'",
                                    "Substitution_Factor":"\'%s\'",
                                    "Tolerance_Over_Amt":"\'%s\'",
                                    "Tolerance_Under_Amt":"\'%s\'",
                                    "Total_Moisture_Percent":"\'%s\'",
                                    "Trim_Qty":"\'%s\'",
                                    "Trim_UOM":"\'%s\'",
                                    "UpdatedBy_Batch":"\'%s\'",
                                    "Water_UOM":"\'%s\'",
                                    "IsActive":"\'%s\'"
                                }]''' % (
                                    proc_line.load_id.c_load_id,
                                    (data["Absorbed_Water"] or '0'),
                                    (data["Absorption_Percent"] or '0'),
                                    (data["Actual_Water"] or '0'),
                                    (data["Actual_Water_Calc_Type"] or '0'),
                                    (data["Adjust_UOM"] or '0'),
                                    (data["Alias_Code"] or '0'),
                                    (data["Amt_UOM"] or '0'),
                                    (data["Archive_Flag"] and "Y" or "N"),
                                    (data["Based_On_Factor"] or '0'),
                                    (data["Based_On_Qty"] or '0'),
                                    (data["Based_On_UOM"] or '0'),
                                    (data["Calc_Factor"] or '0'),
                                    (data["Calc_Moisture_Percent"] or '0'),
                                    (data["Correction_Factor"] or '0'),
                                    (data["CreateDate"] or '1970-01-01 00:00:00'),
                                    (data["CreatedBy"] or '0'),
                                    (data["Delete_Flag"] and "Y" or "N"),
                                    (data["Design_Absorbed_Water"] or '0'),
                                    (data["Design_Entry_Qty"] or '0'),
                                    (data["Design_Free_Water"] or '0'),
                                    (data["Design_SSD_Net_Target_Qty"] or '0'),
                                    (data["Design_SSD_Qty"] or '0'),
                                    (data["Design_UOM"] or '0'),
                                    (data["Dispatch_Design_Qty"] or '0'),
                                    (data["Dispatch_Design_UOM"] or '0'),
                                    (data["Do_Not_Batch_Flag"] and "Y" or "N"),
                                    (data["Effectiveness_Percent"] or '0'),
                                    (data["Ingred_ItemID"] or '0'),
                                    (data["Ingredient_Source_Type"] or '0'),
                                    (data["Item_Code"] or '0'),
                                    (data["Item_Description"] or '0'),
                                    (data["Kgs_Per_Liter"] or '0'),
                                    (data["Load_Adjust_Qty"] or '0'),
                                    (data["Load_LineID"] or '0'),
                                    (data["LoadID"] or '0'),
                                    (data["Manual_Feed_Flag"] and "Y" or "N"),
                                    (data["Modified_Flag"] and "Y" or "N"),
                                    (data["Moisture_Entry_Ref_Type"] or '0'),
                                    (data["Net_Auto_Batched_Amt"] or '0'),
                                    (data["Net_Batched_Amt"] or '0'),
                                    (data["Net_Target_Amt"] or '0'),
                                    (data["Net_Used_Amt"] or '0'),
                                    (data["NoteExistsFlag"] and "Y" or "N"),
                                    (data["RecordDate"] or '1970-01-01 00:00:00'),
                                    (data["RowPointer"] or '0'),
                                    (data["Scale_UOM"] or '0'),
                                    (data["Slump_Factor"] or '0'),
                                    (data["Solids_Specific_Gravity"] or '0'),
                                    (data["Sort_Line_Num"] or '0'),
                                    (data["Specific_Gravity"] or '0'),
                                    (data["Substitution_Factor"] or '0'),
                                    (data["Tolerance_Over_Amt"] or '0'),
                                    (data["Tolerance_Under_Amt"] or '0'),
                                    (data["Total_Moisture_Percent"] or '0'),
                                    (data["Trim_Qty"] or '0'),
                                    (data["Trim_UOM"] or '0'),
                                    (data["UpdatedBy"] or '0'),
                                    (data["Water_UOM"] or '0'),
                                    "N"
                                ),
                    'returning':'C_Batch_LoadLine_ID'
                    })
                try:
                    if createLine.content and zlib.decompress(createLine.content) and json.loads(zlib.decompress(createLine.content)) and json.loads(zlib.decompress(createLine.content))[0]['C_Batch_LoadLine_ID'.lower()]:
                        proc_line.c_loadline_id = json.loads(zlib.decompress(createLine.content))[0]['C_Batch_LoadLine_ID'.lower()]
                        proc_line.save()
                        print "Success Create Line ID.%s" % (proc_line.c_loadline_id)
                    else:
                        print "Error Create Line"
                except:
                    print "Server Error Create Line"
                    print createLine.content and zlib.decompress(createLine.content)
                finally:
                    pass
            except Exception as e:
                print(e)
            finally:
                pass
    
    proc_lines = LoadLineProc.objects.filter(state='CO', load_id__in=procs.values_list('load_id', flat=True))
    for proc_line in proc_lines:
        loadline_ok = False
        proc_line.state = 'UP'
        
        time.sleep(0.01)
        try:
#                 if response.status == WebServiceResponseStatus.Error:
#                     print('Error: ' + response.error_message)
#                 else:
#                     print('RecordID: ' + str(response.record_id))
#                     for field in response.output_fields:
#                         print(str(field.column) + ': ' + str(field.value))
#                     print('---------------------------------------------')
#                     print('Web Service Type: ' + ws.web_service_type)
#                     print('Attempts: ' + str(wsc.attempts_request))
#                     print('Time: ' + str(wsc.time_request))
#                     # proc_line.c_loadline_id = response.record_id
#                     proc_line.save()

            activateLine = requests.post('%s/crm-ws/update' % (ad_url.strip('/')), data={
                'uuid':uuid.text,
                'table':'C_Batch_LoadLine',
                'fields':'[{"IsActive":"\'Y\'"}]',
                'returning':'C_Batch_LoadLine_ID',
                'id':proc_line.c_loadline_id
                })
            try:
                if activateLine.content and zlib.decompress(activateLine.content) and json.loads(zlib.decompress(activateLine.content)) and json.loads(zlib.decompress(activateLine.content))[0]['C_Batch_LoadLine_ID'.lower()]:
#                     proc_line.c_loadline_id = json.loads(zlib.decompress(createLine.content))[0]['C_Batch_LoadLine_ID'.lower()]
                    proc_line.save()
                    print "Success Activate Line ID.%s" % (proc_line.c_loadline_id)
                else:
                    print "Error Activate Line ID.%s" % (proc_line.c_loadline_id)
            except:
                print "Server Error Activate Line ID.%s" % (proc_line.c_loadline_id)
                print activateLine.content and zlib.decompress(activateLine.content)
            finally:
                pass
        except Exception as e:
            print(e)
        finally:
            pass
    
    procs = LoadProc.objects.filter(state='CO', load_id=pk)  # LoadProc.objects.filter(state='CO', c_load_id__isnull=False)
    for proc in procs:
        load_ok = False
        proc.state = 'UP'

        time.sleep(0.01)
        try:
#                 response = wsc.send_request(ws)
#                 wsc.print_xml_request()
#                 wsc.print_xml_response()
#                
#                 if response.status == WebServiceResponseStatus.Error:
#                     print('Error: ' + response.error_message)
#                 else:
#                     print('RecordID: ' + str(response.record_id))
#                     for field in response.output_fields:
#                         print(str(field.column) + ': ' + str(field.value))
#                     print('---------------------------------------------')
#                     print('Web Service Type: ' + ws.web_service_type)
#                     print('Attempts: ' + str(wsc.attempts_request))
#                     print('Time: ' + str(wsc.time_request))
#                     # proc.c_load_id = response.record_id
#                     proc.save()

            activateHeader = requests.post('%s/crm-ws/update' % (ad_url.strip('/')), data={
                'uuid':uuid.text,
                'table':'C_Batch_Load',
                'fields':'[{"IsActive":"\'Y\'"}]',
                'returning':'C_Batch_Load_ID',
                'id':proc.c_load_id
                })
            try:
                if activateHeader.content and zlib.decompress(activateHeader.content) and json.loads(zlib.decompress(activateHeader.content)) and json.loads(zlib.decompress(activateHeader.content))[0]['C_Batch_Load_ID'.lower()]:
#                     proc.c_load_id = json.loads(zlib.decompress(createHeader.content))[0]['C_Batch_Load_ID'.lower()]
                    proc.save()
                    print "Success Activate Header ID.%s" % (proc.c_load_id)
#                         load_ok = True
                else:
                    print "Error Activate Header ID.%s" % (proc.c_load_id)
            except:
                print "Server Error Activate Header ID.%s" % (proc.c_load_id)
                print activateHeader.content and zlib.decompress(activateHeader.content)
            finally:
                pass
        except Exception as e:
            print(e)     
        finally:
            pass
    return redirect(request.META.get('HTTP_REFERER', None) or '/')

@csrf_exempt
def LoadProcByDate(request):
    ad_client_id = BatchConfig.objects.get(name='ad_client_id').value
    ad_org_id = BatchConfig.objects.get(name='ad_org_id').value
    ad_role_id = BatchConfig.objects.get(name='ad_role_id').value
    ad_url = BatchConfig.objects.get(name='ad_url').value
    m_warehouse_id = BatchConfig.objects.get(name='m_warehouse_id').value
    username = BatchConfig.objects.get(name='username').value
    password = BatchConfig.objects.get(name='password').value
    mssql_host = BatchConfig.objects.get(name='mssql_host').value
    mssql_port = BatchConfig.objects.get(name='mssql_port').value
    mssql_db = BatchConfig.objects.get(name='mssql_db').value
    mssql_user = BatchConfig.objects.get(name='mssql_user').value
    mssql_pass = BatchConfig.objects.get(name='mssql_pass').value
    # combatchmachine = BatchConfig.objects.get(name='combatchmachine') and BatchConfig.objects.get(name='combatchmachine').value or ''
    
    try:
        combatchmachine = BatchConfig.objects.get(name='combatchmachine') and BatchConfig.objects.get(name='combatchmachine').value or ''
    except:
        combatchmachine = ''
    try:
        mssql_limit_by = BatchConfig.objects.get(name='mssql_limit_by') and BatchConfig.objects.get(name='mssql_limit_by').value or 'DAY'
    except:
        mssql_limit_by = 'DAY'
    try:
        mssql_limit = BatchConfig.objects.get(name='mssql_limit') and BatchConfig.objects.get(name='mssql_limit').value or '-7'
    except:
        mssql_limit = '-7'

    try:
        mssql_lomit_by = BatchConfig.objects.get(name='mssql_lomit_by') and BatchConfig.objects.get(name='mssql_lomit_by').value or 'MINUTE'
    except:
        mssql_lomit_by = 'MINUTE'
    try:
        mssql_lomit = BatchConfig.objects.get(name='mssql_lomit') and BatchConfig.objects.get(name='mssql_lomit').value or '-1'
    except:
        mssql_lomit = '-1'
    
    uuid = requests.post('%s/crm-ws/gettoken' % (ad_url.strip('/')), data={
        'username':username,
        'password':password,
        'client_id':ad_client_id,
        'role_id':ad_role_id,
        'org_id':ad_org_id,
        'warehouse_id':m_warehouse_id
    })
    
    context = {}
    str_date = strftime(timezone.now(), '%Y-%m-%d')
    context['load_end_from'], context['load_end_to'] = str_date, str_date
    load_end_from = request.GET.get('load_end_from')
    load_end_to = request.GET.get('load_end_to')
    if load_end_from and load_end_to :
        if request.GET.get('comm') == 'Upload':
            cn = pymssql.connect(
                server=mssql_host,
                user=mssql_user,
                password=mssql_pass,
                database=mssql_db
            )
            procs = LoadProc.objects.filter(load_end__range=[load_end_from, load_end_to])
            for proc in procs:
                load_ok = False
                proc.state = 'CO'
    
                cr = cn.cursor(as_dict=True)
                cr.execute("""
                    SELECT
                        *
                    FROM
                        LOAD,
                        TICKET_LINE,
                        TICKET
                    WHERE
                        LOAD.LoadID = TICKET_LINE.LoadID
                        AND TICKET.Ticket_Code <> 0
                        AND TICKET_LINE.TicketID = TICKET.TicketID
                        AND TICKET_LINE.Delete_Flag = 0
                        AND LOAD.LoadID LIKE '%s';
                """ % (proc.load_id))
                data = cr.fetchone()
                if data:
                    time.sleep(0.01)
                    try:
                    
                        check = requests.post('%s/crm-ws/json/get-info-token' % (ad_url.strip('/')), data={'uuid':uuid.text})
                        if 'berhasil' == json.loads(check.content)['message'].lower():
                            pass
                        else:
                            uuid = requests.post('%s/crm-ws/gettoken' % (ad_url.strip('/')), data={
                                'username':username,
                                'password':password,
                                'client_id':ad_client_id,
                                'role_id':ad_role_id,
                                'org_id':ad_org_id,
                                'warehouse_id':m_warehouse_id
                                })
                        
    #                     response = wsc.send_request(ws)
    #                     wsc.print_xml_request()
    #                     wsc.print_xml_response()
    
                        createHeader = requests.post('%s/crm-ws/create' % (ad_url.strip('/')), data={
                            'uuid':uuid.text,
                            'table':'C_Batch_Load',
                            'fields':'''[{
                                            "C_Batch_Load_ID":"@Generate_ID=C_Batch_Load@",
                                            "C_Batch_Load_UU":"\'@Generate_UUID@\'",
                                            "AD_Client_ID":"@AD_Client_ID@",
                                            "AD_Org_ID":"@AD_Org_ID@",
                                            "Created":"NOW()",
                                            "CreatedBy":"@AD_User_ID@",
                                            "Updated":"NOW()",
                                            "UpdatedBy":"@AD_User_ID@",
                                            "Alias_Code":"\'%s\'",
                                            "Apply_Add_Trim_Flag":"\'%s\'",
                                            "Archive_Flag":"\'%s\'",
                                            "Auto_TDS":"\'%s\'",
                                            "BCP_Load_ID":"\'%s\'",
                                            "BCP_Load_Sort":"\'%s\'",
                                            "Calc_Ack_Counter":"\'%s\'",
                                            "Calc_Req_Counter":"\'%s\'",
                                            "Charge_Rate_Percent":"\'%s\'",
                                            "Control_Sys_Code":"\'%s\'",
                                            "Control_SysID":"\'%s\'",
                                            "CreateDate":"\'%s\'",
                                            "CreatedBy_Batch":"\'%s\'",
                                            "DemandID":"\'%s\'",
                                            "Design_Slump":"\'%s\'",
                                            "Ended_Load_Status":"\'%s\'",
                                            "Failed_Flag":"\'%s\'",
                                            "Load_Code":"\'%s\'",
                                            "Load_Discharge_TDS":"\'%s\'",
                                            "Load_End_TDS":"\'%s\'",
                                            "Load_Size":"\'%s\'",
                                            "Load_Sort_Num":"\'%s\'",
                                            "Load_Start_TDS":"\'%s\'",
                                            "LoadID":"\'%s\'",
                                            "LoadStarted_UserCode":"\'%s\'",
                                            "Manual_Feed_Flag":"\'%s\'",
                                            "Manual_TDS":"\'%s\'",
                                            "Max_Batch":"\'%s\'",
                                            "Max_Batch_Size":"\'%s\'",
                                            "Max_Batch_Size_UOM":"\'%s\'",
                                            "Max_Dispatch_WCR":"\'%s\'",
                                            "Mix_Entry_Ref_Type":"\'%s\'",
                                            "Mixer_TS":"\'%s\'",
                                            "NoteExistsFlag":"\'%s\'",
                                            "Notify_UserCode":"\'%s\'",
                                            "RecordDate":"\'%s\'",
                                            "ReferenceLoadID":"\'%s\'",
                                            "Reship_Batched_TDS":"\'%s\'",
                                            "Reship_Incompatible_Flag":"\'%s\'",
                                            "Reship_Item_Code":"\'%s\'",
                                            "Reship_Item_Description":"\'%s\'",
                                            "Reship_ItemID":"\'%s\'",
                                            "Reship_Load_Code":"\'%s\'",
                                            "Reship_LoadID":"\'%s\'",
                                            "Reship_Qty":"\'%s\'",
                                            "Reship_Qty_UOM":"\'%s\'",
                                            "Reship_Source_Type":"\'%s\'",
                                            "RowPointer":"\'%s\'",
                                            "Run_Ack_Counter":"\'%s\'",
                                            "Run_Req_Counter":"\'%s\'",
                                            "Sequence_Code":"\'%s\'",
                                            "SequenceID":"\'%s\'",
                                            "Sequencing_Flag":"\'%s\'",
                                            "Simulate_Status":"\'%s\'",
                                            "Slump":"\'%s\'",
                                            "Slump_Code":"\'%s\'",
                                            "Slump_UOM":"\'%s\'",
                                            "SlumpID":"\'%s\'",
                                            "Started_Load_Status":"\'%s\'",
                                            "UpdatedBy_Batch":"\'%s\'",
                                            "Washout_Used_Flag":"\'%s\'",
                                            "Water_Cement_Ratio":"\'%s\'",
                                            "Water_In_Truck":"\'%s\'",
                                            "Water_In_Truck_UOM":"\'%s\'",
                                            "Item_Code":"\'%s\'",
                                            "Ticket_Code":"\'%s\'",
                                            "M_Warehouse_ID":"\'%s\'",
                                            "ComBatchMachine":"\'%s\'",
                                            "IsActive":"\'%s\'"
                                        }]''' % (
                                            (data['Alias_Code'] or '0'),
                                            (data['Apply_Add_Trim_Flag'] and 'Y' or 'N'),
                                            (data['Archive_Flag'] and 'Y' or 'N'),
                                            (data['Auto_TDS'] or '1970-01-01 00:00:00'),
                                            (data['BCP_Load_ID'] or '0'),
                                            (data['BCP_Load_Sort'] or '0'),
                                            (data['Calc_Ack_Counter'] or '0'),
                                            (data['Calc_Req_Counter'] or '0'),
                                            (data['Charge_Rate_Percent'] or '0'),
                                            (data['Control_Sys_Code'] or '0'),
                                            (data['Control_SysID'] or '0'),
                                            (data['CreateDate'] or '1970-01-01 00:00:00'),
                                            (data['CreatedBy'] or '0'),
                                            (data['DemandID'] or '0'),
                                            (data['Design_Slump'] or '0'),
                                            (data['Ended_Load_Status'] or '0'),
                                            (data['Failed_Flag'] and 'Y' or 'N'),
                                            (data['Load_Code'] or '0'),
                                            (data['Load_Discharge_TDS'] or '1970-01-01 00:00:00'),
                                            (data['Load_End_TDS'] or '1970-01-01 00:00:00'),
                                            (data['Load_Size'] or '0'),
                                            (data['Load_Sort_Num'] or '0'),
                                            (data['Load_Start_TDS'] or '1970-01-01 00:00:00'),
                                            (data['LoadID'] or '0'),
                                            (data['LoadStarted_UserCode'] or '0'),
                                            (data['Manual_Feed_Flag'] and 'Y' or 'N'),
                                            (data['Manual_TDS'] or '1970-01-01 00:00:00'),
                                            (data['Max_Batch'] or '0'),
                                            (data['Max_Batch_Size'] or '0'),
                                            (data['Max_Batch_Size_UOM'] or '0'),
                                            (data['Max_Dispatch_WCR'] or '0'),
                                            (data['Mix_Entry_Ref_Type'] or '0'),
                                            (data['Mixer_TS'] or '0'),
                                            (data['NoteExistsFlag'] and 'Y' or 'N'),
                                            (data['Notify_UserCode'] or '0'),
                                            (data['RecordDate'] or '1970-01-01 00:00:00'),
                                            (data['ReferenceLoadID'] or '0'),
                                            (data['Reship_Batched_TDS'] or '1970-01-01 00:00:00'),
                                            (data['Reship_Incompatible_Flag'] and 'Y' or 'N'),
                                            (data['Reship_Item_Code'] or '0'),
                                            (data['Reship_Item_Description'] or '0'),
                                            (data['Reship_ItemID'] or '0'),
                                            (data['Reship_Load_Code'] or '0'),
                                            (data['Reship_LoadID'] or '0'),
                                            (data['Reship_Qty'] or '0'),
                                            (data['Reship_Qty_UOM'] or '0'),
                                            (data['Reship_Source_Type'] or '0'),
                                            (data['RowPointer'] or '0'),
                                            (data['Run_Ack_Counter'] or '0'),
                                            (data['Run_Req_Counter'] or '0'),
                                            (data['Sequence_Code'] or '0'),
                                            (data['SequenceID'] or '0'),
                                            (data['Sequencing_Flag'] and 'Y' or 'N'),
                                            (data['Simulate_Status'] or '0'),
                                            (data['Slump'] or '0'),
                                            (data['Slump_Code'] or '0'),
                                            (data['Slump_UOM'] or '0'),
                                            (data['SlumpID'] or '0'),
                                            (data['Started_Load_Status'] or '0'),
                                            (data['UpdatedBy'] or '0'),
                                            (data['Washout_Used_Flag'] and 'Y' or 'N'),
                                            (data['Water_Cement_Ratio'] or '0'),
                                            (data['Water_In_Truck'] or '0'),
                                            (data['Water_In_Truck_UOM'] or '0'),
                                            (data['Item_Code'] or '0'),
                                            (data['Ticket_Code'] or '0'),
                                            m_warehouse_id,
                                            combatchmachine,
                                            'N'
                                        ),
                            'returning':'C_Batch_Load_ID'
                            })
                       
    #                     if response.status == WebServiceResponseStatus.Error:
    #                         print('Error: ' + response.error_message)
    #                     else:
    #                         print('RecordID: ' + str(response.record_id))
    #                         for field in response.output_fields:
    #                             print(str(field.column) + ': ' + str(field.value))
    #                         print('---------------------------------------------')
    #                         print('Web Service Type: ' + ws.web_service_type)
    #                         print('Attempts: ' + str(wsc.attempts_request))
    #                         print('Time: ' + str(wsc.time_request))
    #                         proc.c_load_id = response.record_id
    #                         load_ok = True
                        try:
                            if createHeader.content and zlib.decompress(createHeader.content) and json.loads(zlib.decompress(createHeader.content)) and json.loads(zlib.decompress(createHeader.content))[0]['C_Batch_Load_ID'.lower()]:
                                proc.c_load_id = json.loads(zlib.decompress(createHeader.content))[0]['C_Batch_Load_ID'.lower()]
                                proc.save()
                                print "Success Create Header ID.%s" % (proc.c_load_id)
        #                         load_ok = True
                            else:
                                print "Error Create Header"
                        except:
                            print "Server Error Create Header"
                            print createHeader.content and zlib.decompress(createHeader.content)
                        finally:
                            pass
                    except Exception as e:
                        print(e)
                    finally:
                        pass
    
                    cr.close()
            
            proc_lines = LoadLineProc.objects.filter(load_id__in=procs.values_list('load_id', flat=True))
            for proc_line in proc_lines:
                loadline_ok = False
                proc_line.state = 'CO'
                
                cr = cn.cursor(as_dict=True)
                cr.execute("select * from LOAD_LINE where Load_LineID like '%s' and Delete_Flag = 0" % (proc_line.loadline_id))
                data = cr.fetchone()
                
                if data:
                    time.sleep(0.01)
                    try:
#                     if response.status == WebServiceResponseStatus.Error:
#                         print('Error: ' + response.error_message)
#                     else:
#                         print('RecordID: ' + str(response.record_id))
#                         for field in response.output_fields:
#                             print(str(field.column) + ': ' + str(field.value))
#                         print('---------------------------------------------')
#                         print('Web Service Type: ' + ws.web_service_type)
#                         print('Attempts: ' + str(wsc.attempts_request))
#                         print('Time: ' + str(wsc.time_request))
#                         proc_line.c_loadline_id = response.record_id
#                         proc_line.save()

                        createLine = requests.post('%s/crm-ws/create' % (ad_url.strip('/')), data={
                            'uuid':uuid.text,
                            'table':'C_Batch_LoadLine',
                            'fields':'''[{
                                            "C_Batch_LoadLine_ID":"@Generate_ID=C_Batch_LoadLine@",
                                            "C_Batch_LoadLine_UU":"\'@Generate_UUID@\'",
                                            "AD_Client_ID":"@AD_Client_ID@",
                                            "AD_Org_ID":"@AD_Org_ID@",
                                            "Created":"NOW()",
                                            "CreatedBy":"@AD_User_ID@",
                                            "Updated":"NOW()",
                                            "UpdatedBy":"@AD_User_ID@",
                                            "C_Batch_Load_ID":"\'%s\'",
                                            "Absorbed_Water":"\'%s\'",
                                            "Absorption_Percent":"\'%s\'",
                                            "Actual_Water":"\'%s\'",
                                            "Actual_Water_Calc_Type":"\'%s\'",
                                            "Adjust_UOM":"\'%s\'",
                                            "Alias_Code":"\'%s\'",
                                            "Amt_UOM":"\'%s\'",
                                            "Archive_Flag":"\'%s\'",
                                            "Based_On_Factor":"\'%s\'",
                                            "Based_On_Qty":"\'%s\'",
                                            "Based_On_UOM":"\'%s\'",
                                            "Calc_Factor":"\'%s\'",
                                            "Calc_Moisture_Percent":"\'%s\'",
                                            "Correction_Factor":"\'%s\'",
                                            "CreateDate":"\'%s\'",
                                            "CreatedBy_Batch":"\'%s\'",
                                            "Delete_Flag":"\'%s\'",
                                            "Design_Absorbed_Water":"\'%s\'",
                                            "Design_Entry_Qty":"\'%s\'",
                                            "Design_Free_Water":"\'%s\'",
                                            "Design_SSD_Net_Target_Qty":"\'%s\'",
                                            "Design_SSD_Qty":"\'%s\'",
                                            "Design_UOM":"\'%s\'",
                                            "Dispatch_Design_Qty":"\'%s\'",
                                            "Dispatch_Design_UOM":"\'%s\'",
                                            "Do_Not_Batch_Flag":"\'%s\'",
                                            "Effectiveness_Percent":"\'%s\'",
                                            "Ingred_ItemID":"\'%s\'",
                                            "Ingredient_Source_Type":"\'%s\'",
                                            "Item_Code":"\'%s\'",
                                            "Item_Description":"\'%s\'",
                                            "Kgs_Per_Liter":"\'%s\'",
                                            "Load_Adjust_Qty":"\'%s\'",
                                            "Load_LineID":"\'%s\'",
                                            "LoadID":"\'%s\'",
                                            "Manual_Feed_Flag":"\'%s\'",
                                            "Modified_Flag":"\'%s\'",
                                            "Moisture_Entry_Ref_Type":"\'%s\'",
                                            "Net_Auto_Batched_Amt":"\'%s\'",
                                            "Net_Batched_Amt":"\'%s\'",
                                            "Net_Target_Amt":"\'%s\'",
                                            "Net_Used_Amt":"\'%s\'",
                                            "NoteExistsFlag":"\'%s\'",
                                            "RecordDate":"\'%s\'",
                                            "RowPointer":"\'%s\'",
                                            "Scale_UOM":"\'%s\'",
                                            "Slump_Factor":"\'%s\'",
                                            "Solids_Specific_Gravity":"\'%s\'",
                                            "Sort_Line_Num":"\'%s\'",
                                            "Specific_Gravity":"\'%s\'",
                                            "Substitution_Factor":"\'%s\'",
                                            "Tolerance_Over_Amt":"\'%s\'",
                                            "Tolerance_Under_Amt":"\'%s\'",
                                            "Total_Moisture_Percent":"\'%s\'",
                                            "Trim_Qty":"\'%s\'",
                                            "Trim_UOM":"\'%s\'",
                                            "UpdatedBy_Batch":"\'%s\'",
                                            "Water_UOM":"\'%s\'",
                                            "IsActive":"\'%s\'"
                                        }]''' % (
                                            proc_line.load_id.c_load_id,
                                            (data["Absorbed_Water"] or '0'),
                                            (data["Absorption_Percent"] or '0'),
                                            (data["Actual_Water"] or '0'),
                                            (data["Actual_Water_Calc_Type"] or '0'),
                                            (data["Adjust_UOM"] or '0'),
                                            (data["Alias_Code"] or '0'),
                                            (data["Amt_UOM"] or '0'),
                                            (data["Archive_Flag"] and "Y" or "N"),
                                            (data["Based_On_Factor"] or '0'),
                                            (data["Based_On_Qty"] or '0'),
                                            (data["Based_On_UOM"] or '0'),
                                            (data["Calc_Factor"] or '0'),
                                            (data["Calc_Moisture_Percent"] or '0'),
                                            (data["Correction_Factor"] or '0'),
                                            (data["CreateDate"] or '1970-01-01 00:00:00'),
                                            (data["CreatedBy"] or '0'),
                                            (data["Delete_Flag"] and "Y" or "N"),
                                            (data["Design_Absorbed_Water"] or '0'),
                                            (data["Design_Entry_Qty"] or '0'),
                                            (data["Design_Free_Water"] or '0'),
                                            (data["Design_SSD_Net_Target_Qty"] or '0'),
                                            (data["Design_SSD_Qty"] or '0'),
                                            (data["Design_UOM"] or '0'),
                                            (data["Dispatch_Design_Qty"] or '0'),
                                            (data["Dispatch_Design_UOM"] or '0'),
                                            (data["Do_Not_Batch_Flag"] and "Y" or "N"),
                                            (data["Effectiveness_Percent"] or '0'),
                                            (data["Ingred_ItemID"] or '0'),
                                            (data["Ingredient_Source_Type"] or '0'),
                                            (data["Item_Code"] or '0'),
                                            (data["Item_Description"] or '0'),
                                            (data["Kgs_Per_Liter"] or '0'),
                                            (data["Load_Adjust_Qty"] or '0'),
                                            (data["Load_LineID"] or '0'),
                                            (data["LoadID"] or '0'),
                                            (data["Manual_Feed_Flag"] and "Y" or "N"),
                                            (data["Modified_Flag"] and "Y" or "N"),
                                            (data["Moisture_Entry_Ref_Type"] or '0'),
                                            (data["Net_Auto_Batched_Amt"] or '0'),
                                            (data["Net_Batched_Amt"] or '0'),
                                            (data["Net_Target_Amt"] or '0'),
                                            (data["Net_Used_Amt"] or '0'),
                                            (data["NoteExistsFlag"] and "Y" or "N"),
                                            (data["RecordDate"] or '1970-01-01 00:00:00'),
                                            (data["RowPointer"] or '0'),
                                            (data["Scale_UOM"] or '0'),
                                            (data["Slump_Factor"] or '0'),
                                            (data["Solids_Specific_Gravity"] or '0'),
                                            (data["Sort_Line_Num"] or '0'),
                                            (data["Specific_Gravity"] or '0'),
                                            (data["Substitution_Factor"] or '0'),
                                            (data["Tolerance_Over_Amt"] or '0'),
                                            (data["Tolerance_Under_Amt"] or '0'),
                                            (data["Total_Moisture_Percent"] or '0'),
                                            (data["Trim_Qty"] or '0'),
                                            (data["Trim_UOM"] or '0'),
                                            (data["UpdatedBy"] or '0'),
                                            (data["Water_UOM"] or '0'),
                                            "N"
                                        ),
                            'returning':'C_Batch_LoadLine_ID'
                            })
                        try:
                            if createLine.content and zlib.decompress(createLine.content) and json.loads(zlib.decompress(createLine.content)) and json.loads(zlib.decompress(createLine.content))[0]['C_Batch_LoadLine_ID'.lower()]:
                                proc_line.c_loadline_id = json.loads(zlib.decompress(createLine.content))[0]['C_Batch_LoadLine_ID'.lower()]
                                proc_line.save()
                                print "Success Create Line ID.%s" % (proc_line.c_loadline_id)
                            else:
                                print "Error Create Line"
                        except:
                            print "Server Error Create Line"
                            print createLine.content and zlib.decompress(createLine.content)
                        finally:
                            pass
                    except Exception as e:
                        print(e)
                    finally:
                        pass
            
            proc_lines = LoadLineProc.objects.filter(state='CO', load_id__in=procs.values_list('load_id', flat=True))
            for proc_line in proc_lines:
                loadline_ok = False
                proc_line.state = 'UP'
                
                time.sleep(0.01)
                try:
#                 if response.status == WebServiceResponseStatus.Error:
#                     print('Error: ' + response.error_message)
#                 else:
#                     print('RecordID: ' + str(response.record_id))
#                     for field in response.output_fields:
#                         print(str(field.column) + ': ' + str(field.value))
#                     print('---------------------------------------------')
#                     print('Web Service Type: ' + ws.web_service_type)
#                     print('Attempts: ' + str(wsc.attempts_request))
#                     print('Time: ' + str(wsc.time_request))
#                     # proc_line.c_loadline_id = response.record_id
#                     proc_line.save()

                    activateLine = requests.post('%s/crm-ws/update' % (ad_url.strip('/')), data={
                        'uuid':uuid.text,
                        'table':'C_Batch_LoadLine',
                        'fields':'[{"IsActive":"\'Y\'"}]',
                        'returning':'C_Batch_LoadLine_ID',
                        'id':proc_line.c_loadline_id
                        })
                    try:
                        if activateLine.content and zlib.decompress(activateLine.content) and json.loads(zlib.decompress(activateLine.content)) and json.loads(zlib.decompress(activateLine.content))[0]['C_Batch_LoadLine_ID'.lower()]:
        #                     proc_line.c_loadline_id = json.loads(zlib.decompress(createLine.content))[0]['C_Batch_LoadLine_ID'.lower()]
                            proc_line.save()
                            print "Success Activate Line ID.%s" % (proc_line.c_loadline_id)
                        else:
                            print "Error Activate Line ID.%s" % (proc_line.c_loadline_id)
                    except:
                        print "Server Error Activate Line ID.%s" % (proc_line.c_loadline_id)
                        print activateLine.content and zlib.decompress(activateLine.content)
                    finally:
                        pass
                except Exception as e:
                    print(e)
                finally:
                    pass
            
            procs = LoadProc.objects.filter(state='CO', load_end__range=[load_end_from, load_end_to])  # LoadProc.objects.filter(state='CO', c_load_id__isnull=False)
            for proc in procs:
                load_ok = False
                proc.state = 'UP'
    
                time.sleep(0.01)
                try:
#                 response = wsc.send_request(ws)
#                 wsc.print_xml_request()
#                 wsc.print_xml_response()
#                
#                 if response.status == WebServiceResponseStatus.Error:
#                     print('Error: ' + response.error_message)
#                 else:
#                     print('RecordID: ' + str(response.record_id))
#                     for field in response.output_fields:
#                         print(str(field.column) + ': ' + str(field.value))
#                     print('---------------------------------------------')
#                     print('Web Service Type: ' + ws.web_service_type)
#                     print('Attempts: ' + str(wsc.attempts_request))
#                     print('Time: ' + str(wsc.time_request))
#                     # proc.c_load_id = response.record_id
#                     proc.save()

                    activateHeader = requests.post('%s/crm-ws/update' % (ad_url.strip('/')), data={
                        'uuid':uuid.text,
                        'table':'C_Batch_Load',
                        'fields':'[{"IsActive":"\'Y\'"}]',
                        'returning':'C_Batch_Load_ID',
                        'id':proc.c_load_id
                        })
                    try:
                        if activateHeader.content and zlib.decompress(activateHeader.content) and json.loads(zlib.decompress(activateHeader.content)) and json.loads(zlib.decompress(activateHeader.content))[0]['C_Batch_Load_ID'.lower()]:
        #                     proc.c_load_id = json.loads(zlib.decompress(createHeader.content))[0]['C_Batch_Load_ID'.lower()]
                            proc.save()
                            print "Success Activate Header ID.%s" % (proc.c_load_id)
        #                         load_ok = True
                        else:
                            print "Error Activate Header ID.%s" % (proc.c_load_id)
                    except:
                        print "Server Error Activate Header ID.%s" % (proc.c_load_id)
                        print activateHeader.content and zlib.decompress(activateHeader.content)
                    finally:
                        pass
                except Exception as e:
                    print(e)     
                finally:
                    pass
                
        context['load_end_from'], context['load_end_to'] = load_end_from, load_end_to
        context['object_list'] = LoadProc.objects.filter(load_end__range=[load_end_from, load_end_to])
#         for test in context['object_list'].values_list('load_id'):
#             print test
    return render(request, 'ui/loadproc_date_list.html', context)
