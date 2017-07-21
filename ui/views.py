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

login = LoginRequest()
login.client_id = ad_client_id
login.org_id = ad_org_id
login.role_id = ad_role_id
login.password = password
login.user = username
login.warehouse_id = m_warehouse_id

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
            ws = CreateDataRequest()
            ws.web_service_type = 'InsertEBatchLoad'
            ws.login = login

            ws.data_row = [
                Field('Alias_Code', data['Alias_Code']),
                Field('Apply_Add_Trim_Flag', data['Apply_Add_Trim_Flag'] and 'Y' or 'N'),
                Field('Archive_Flag', data['Archive_Flag'] and 'Y' or 'N'),
                Field('Auto_TDS', data['Auto_TDS']),
                Field('BCP_Load_ID', data['BCP_Load_ID']),
                Field('BCP_Load_Sort', data['BCP_Load_Sort']),
                Field('Calc_Ack_Counter', data['Calc_Ack_Counter']),
                Field('Calc_Req_Counter', data['Calc_Req_Counter']),
                Field('Charge_Rate_Percent', data['Charge_Rate_Percent']),
                Field('Control_Sys_Code', data['Control_Sys_Code']),
                Field('Control_SysID', data['Control_SysID']),
                Field('CreateDate', data['CreateDate']),
                Field('CreatedBy_Batch', data['CreatedBy']),
                Field('DemandID', data['DemandID']),
                Field('Design_Slump', data['Design_Slump']),
                Field('Ended_Load_Status', data['Ended_Load_Status']),
                Field('Failed_Flag', data['Failed_Flag'] and 'Y' or 'N'),
                Field('Load_Code', data['Load_Code']),
                Field('Load_Discharge_TDS', data['Load_Discharge_TDS']),
                Field('Load_End_TDS', data['Load_End_TDS']),
                Field('Load_Size', data['Load_Size']),
                Field('Load_Sort_Num', data['Load_Sort_Num']),
                Field('Load_Start_TDS', data['Load_Start_TDS']),
                Field('LoadID', data['LoadID']),
                Field('LoadStarted_UserCode', data['LoadStarted_UserCode']),
                Field('Manual_Feed_Flag', data['Manual_Feed_Flag'] and 'Y' or 'N'),
                Field('Manual_TDS', data['Manual_TDS']),
                Field('Max_Batch', data['Max_Batch']),
                Field('Max_Batch_Size', data['Max_Batch_Size']),
                Field('Max_Batch_Size_UOM', data['Max_Batch_Size_UOM']),
                Field('Max_Dispatch_WCR', data['Max_Dispatch_WCR']),
                Field('Mix_Entry_Ref_Type', data['Mix_Entry_Ref_Type']),
                Field('Mixer_TS', data['Mixer_TS']),
                Field('NoteExistsFlag', data['NoteExistsFlag']),
                Field('Notify_UserCode', data['Notify_UserCode']),
                Field('RecordDate', data['RecordDate']),
                Field('ReferenceLoadID', data['ReferenceLoadID']),
                Field('Reship_Batched_TDS', data['Reship_Batched_TDS']),
                Field('Reship_Incompatible_Flag', data['Reship_Incompatible_Flag'] and 'Y' or 'N'),
                Field('Reship_Item_Code', data['Reship_Item_Code']),
                Field('Reship_Item_Description', data['Reship_Item_Description']),
                Field('Reship_ItemID', data['Reship_ItemID']),
                Field('Reship_Load_Code', data['Reship_Load_Code']),
                Field('Reship_LoadID', data['Reship_LoadID']),
                Field('Reship_Qty', data['Reship_Qty']),
                Field('Reship_Qty_UOM', data['Reship_Qty_UOM']),
                Field('Reship_Source_Type', data['Reship_Source_Type']),
                Field('RowPointer', data['RowPointer']),
                Field('Run_Ack_Counter', data['Run_Ack_Counter']),
                Field('Run_Req_Counter', data['Run_Req_Counter']),
                Field('Sequence_Code', data['Sequence_Code']),
                Field('SequenceID', data['SequenceID']),
                Field('Sequencing_Flag', data['Sequencing_Flag'] and 'Y' or 'N'),
                Field('Simulate_Status', data['Simulate_Status']),
                Field('Slump', data['Slump']),
                Field('Slump_Code', data['Slump_Code']),
                Field('Slump_UOM', data['Slump_UOM']),
                Field('SlumpID', data['SlumpID']),
                Field('Started_Load_Status', data['Started_Load_Status']),
                Field('UpdatedBy_Batch', data['UpdatedBy']),
                Field('Washout_Used_Flag', data['Washout_Used_Flag'] and 'Y' or 'N'),
                Field('Water_Cement_Ratio', data['Water_Cement_Ratio']),
                Field('Water_In_Truck', data['Water_In_Truck']),
                Field('Water_In_Truck_UOM', data['Water_In_Truck_UOM']),
                Field('Item_Code', data['Item_Code']),
                Field('Ticket_Code', data['Ticket_Code']),
                Field('M_Warehouse_ID', m_warehouse_id),
                Field('IsActive', 'N')
            ]

            wsc = WebServiceConnection()
            wsc.url = ad_url
            wsc.attempts = 1
            wsc.app_name = 'InsertEBatchLoad'

            try:
                response = wsc.send_request(ws)
                wsc.print_xml_request()
                wsc.print_xml_response()
               
                if response.status == WebServiceResponseStatus.Error:
                    print('Error: ' + response.error_message)
                else:
                    print('RecordID: ' + str(response.record_id))
                    for field in response.output_fields:
                        print(str(field.column) + ': ' + str(field.value))
                    print('---------------------------------------------')
                    print('Web Service Type: ' + ws.web_service_type)
                    print('Attempts: ' + str(wsc.attempts_request))
                    print('Time: ' + str(wsc.time_request))
                    proc.c_load_id = response.record_id
                    load_ok = True
            except Exception as e:
                print(e)

            cr.close()

            if load_ok:
                proc.save()
    
    proc_lines = LoadLineProc.objects.filter(load_id__in=procs.values_list('load_id', flat=True))
    for proc_line in proc_lines:
        loadline_ok = False
        proc_line.state = 'CO'
        
        cr = cn.cursor(as_dict=True)
        cr.execute("select * from LOAD_LINE where Load_LineID like '%s' and Delete_Flag = 0" % (proc_line.loadline_id))
        data = cr.fetchone()
        
        if data:
            time.sleep(0.01)
            ws = CreateDataRequest()
            ws.web_service_type = 'InsertEBatchLoadLine'
            ws.login = login

            ws.data_row = [
                Field('C_Batch_Load_ID', proc_line.load_id.c_load_id),
                Field('Absorbed_Water', data['Absorbed_Water']),
                Field('Absorption_Percent', data['Absorption_Percent']),
                Field('Actual_Water', data['Actual_Water']),
                Field('Actual_Water_Calc_Type', data['Actual_Water_Calc_Type']),
                Field('Adjust_UOM', data['Adjust_UOM']),
                Field('Alias_Code', data['Alias_Code']),
                Field('Amt_UOM', data['Amt_UOM']),
                Field('Archive_Flag', data['Archive_Flag'] and 'Y' or 'N'),
                Field('Based_On_Factor', data['Based_On_Factor']),
                Field('Based_On_Qty', data['Based_On_Qty']),
                Field('Based_On_UOM', data['Based_On_UOM']),
                Field('Calc_Factor', data['Calc_Factor']),
                Field('Calc_Moisture_Percent', data['Calc_Moisture_Percent']),
                Field('Correction_Factor', data['Correction_Factor']),
                Field('CreateDate', data['CreateDate']),
                Field('CreatedBy_Batch', data['CreatedBy']),
                Field('Delete_Flag', data['Delete_Flag'] and 'Y' or 'N'),
                Field('Design_Absorbed_Water', data['Design_Absorbed_Water']),
                Field('Design_Entry_Qty', data['Design_Entry_Qty']),
                Field('Design_Free_Water', data['Design_Free_Water']),
                Field('Design_SSD_Net_Target_Qty', data['Design_SSD_Net_Target_Qty']),
                Field('Design_SSD_Qty', data['Design_SSD_Qty']),
                Field('Design_UOM', data['Design_UOM']),
                Field('Dispatch_Design_Qty', data['Dispatch_Design_Qty']),
                Field('Dispatch_Design_UOM', data['Dispatch_Design_UOM']),
                Field('Do_Not_Batch_Flag', data['Do_Not_Batch_Flag'] and 'Y' or 'N'),
                Field('Effectiveness_Percent', data['Effectiveness_Percent']),
                Field('Ingred_ItemID', data['Ingred_ItemID']),
                Field('Ingredient_Source_Type', data['Ingredient_Source_Type']),
                Field('Item_Code', data['Item_Code']),
                Field('Item_Description', data['Item_Description']),
                Field('Kgs_Per_Liter', data['Kgs_Per_Liter']),
                Field('Load_Adjust_Qty', data['Load_Adjust_Qty']),
                Field('Load_LineID', data['Load_LineID']),
                Field('LoadID', data['LoadID']),
                Field('Manual_Feed_Flag', data['Manual_Feed_Flag'] and 'Y' or 'N'),
                Field('Modified_Flag', data['Modified_Flag'] and 'Y' or 'N'),
                Field('Moisture_Entry_Ref_Type', data['Moisture_Entry_Ref_Type']),
                Field('Net_Auto_Batched_Amt', data['Net_Auto_Batched_Amt']),
                Field('Net_Batched_Amt', data['Net_Batched_Amt']),
                Field('Net_Target_Amt', data['Net_Target_Amt']),
                Field('Net_Used_Amt', data['Net_Used_Amt']),
                Field('NoteExistsFlag', data['NoteExistsFlag']),
                Field('RecordDate', data['RecordDate']),
                Field('RowPointer', data['RowPointer']),
                Field('Scale_UOM', data['Scale_UOM']),
                Field('Slump_Factor', data['Slump_Factor']),
                Field('Solids_Specific_Gravity', data['Solids_Specific_Gravity']),
                Field('Sort_Line_Num', data['Sort_Line_Num']),
                Field('Specific_Gravity', data['Specific_Gravity']),
                Field('Substitution_Factor', data['Substitution_Factor']),
                Field('Tolerance_Over_Amt', data['Tolerance_Over_Amt']),
                Field('Tolerance_Under_Amt', data['Tolerance_Under_Amt']),
                Field('Total_Moisture_Percent', data['Total_Moisture_Percent']),
                Field('Trim_Qty', data['Trim_Qty']),
                Field('Trim_UOM', data['Trim_UOM']),
                Field('UpdatedBy_Batch', data['UpdatedBy']),
                Field('Water_UOM', data['Water_UOM']),
                Field('IsActive', 'N')
            ]

            wsc = WebServiceConnection()
            wsc.url = ad_url
            wsc.attempts = 1
            wsc.app_name = 'InsertEBatchLoadLine'
             
            response = wsc.send_request(ws)
            wsc.print_xml_request()
            wsc.print_xml_response()
            try:
                if response.status == WebServiceResponseStatus.Error:
                    print('Error: ' + response.error_message)
                else:
                    print('RecordID: ' + str(response.record_id))
                    for field in response.output_fields:
                        print(str(field.column) + ': ' + str(field.value))
                    print('---------------------------------------------')
                    print('Web Service Type: ' + ws.web_service_type)
                    print('Attempts: ' + str(wsc.attempts_request))
                    print('Time: ' + str(wsc.time_request))
                    proc_line.c_loadline_id = response.record_id
                    proc_line.save()
            except Exception as e:
                print(e)
    
    proc_lines = LoadLineProc.objects.filter(state='CO', load_id__in=procs.values_list('load_id', flat=True))
    for proc_line in proc_lines:
        loadline_ok = False
        proc_line.state = 'UP'
        
        time.sleep(0.01)
        ws = UpdateDataRequest()
        ws.web_service_type = 'ActivateEBatchLoadLine'
        ws.record_id = proc_line.c_loadline_id
        ws.login = login

        ws.data_row = [
            Field('IsActive', 'Y')
        ]

        wsc = WebServiceConnection()
        wsc.url = ad_url
        wsc.attempts = 1
        wsc.app_name = 'ActivateEBatchLoadLine'
         
        response = wsc.send_request(ws)
        wsc.print_xml_request()
        wsc.print_xml_response()
        try:
            if response.status == WebServiceResponseStatus.Error:
                print('Error: ' + response.error_message)
            else:
                print('RecordID: ' + str(response.record_id))
                for field in response.output_fields:
                    print(str(field.column) + ': ' + str(field.value))
                print('---------------------------------------------')
                print('Web Service Type: ' + ws.web_service_type)
                print('Attempts: ' + str(wsc.attempts_request))
                print('Time: ' + str(wsc.time_request))
                # proc_line.c_loadline_id = response.record_id
                proc_line.save()
        except Exception as e:
            print(e)
    
    procs = LoadProc.objects.filter(state='CO', load_id=pk)  # LoadProc.objects.filter(state='CO', c_load_id__isnull=False)
    for proc in procs:
        load_ok = False
        proc.state = 'UP'

        time.sleep(0.01)
        ws = UpdateDataRequest()
        ws.web_service_type = 'ActivateEBatchLoad'
        ws.record_id = proc.c_load_id
        ws.login = login

        ws.data_row = [
            Field('IsActive', 'Y')
        ]

        wsc = WebServiceConnection()
        wsc.url = ad_url
        wsc.attempts = 1
        wsc.app_name = 'ActivateEBatchLoad'

        try:
            response = wsc.send_request(ws)
            wsc.print_xml_request()
            wsc.print_xml_response()
           
            if response.status == WebServiceResponseStatus.Error:
                print('Error: ' + response.error_message)
            else:
                print('RecordID: ' + str(response.record_id))
                for field in response.output_fields:
                    print(str(field.column) + ': ' + str(field.value))
                print('---------------------------------------------')
                print('Web Service Type: ' + ws.web_service_type)
                print('Attempts: ' + str(wsc.attempts_request))
                print('Time: ' + str(wsc.time_request))
                # proc.c_load_id = response.record_id
                proc.save()
        except Exception as e:
            print(e)
    return redirect(request.META.get('HTTP_REFERER', None) or '/')

@csrf_exempt
def LoadProcByDate(request):    
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
                    ws = CreateDataRequest()
                    ws.web_service_type = 'InsertEBatchLoad'
                    ws.login = login
    
                    ws.data_row = [
                        Field('Alias_Code', data['Alias_Code']),
                        Field('Apply_Add_Trim_Flag', data['Apply_Add_Trim_Flag'] and 'Y' or 'N'),
                        Field('Archive_Flag', data['Archive_Flag'] and 'Y' or 'N'),
                        Field('Auto_TDS', data['Auto_TDS']),
                        Field('BCP_Load_ID', data['BCP_Load_ID']),
                        Field('BCP_Load_Sort', data['BCP_Load_Sort']),
                        Field('Calc_Ack_Counter', data['Calc_Ack_Counter']),
                        Field('Calc_Req_Counter', data['Calc_Req_Counter']),
                        Field('Charge_Rate_Percent', data['Charge_Rate_Percent']),
                        Field('Control_Sys_Code', data['Control_Sys_Code']),
                        Field('Control_SysID', data['Control_SysID']),
                        Field('CreateDate', data['CreateDate']),
                        Field('CreatedBy_Batch', data['CreatedBy']),
                        Field('DemandID', data['DemandID']),
                        Field('Design_Slump', data['Design_Slump']),
                        Field('Ended_Load_Status', data['Ended_Load_Status']),
                        Field('Failed_Flag', data['Failed_Flag'] and 'Y' or 'N'),
                        Field('Load_Code', data['Load_Code']),
                        Field('Load_Discharge_TDS', data['Load_Discharge_TDS']),
                        Field('Load_End_TDS', data['Load_End_TDS']),
                        Field('Load_Size', data['Load_Size']),
                        Field('Load_Sort_Num', data['Load_Sort_Num']),
                        Field('Load_Start_TDS', data['Load_Start_TDS']),
                        Field('LoadID', data['LoadID']),
                        Field('LoadStarted_UserCode', data['LoadStarted_UserCode']),
                        Field('Manual_Feed_Flag', data['Manual_Feed_Flag'] and 'Y' or 'N'),
                        Field('Manual_TDS', data['Manual_TDS']),
                        Field('Max_Batch', data['Max_Batch']),
                        Field('Max_Batch_Size', data['Max_Batch_Size']),
                        Field('Max_Batch_Size_UOM', data['Max_Batch_Size_UOM']),
                        Field('Max_Dispatch_WCR', data['Max_Dispatch_WCR']),
                        Field('Mix_Entry_Ref_Type', data['Mix_Entry_Ref_Type']),
                        Field('Mixer_TS', data['Mixer_TS']),
                        Field('NoteExistsFlag', data['NoteExistsFlag']),
                        Field('Notify_UserCode', data['Notify_UserCode']),
                        Field('RecordDate', data['RecordDate']),
                        Field('ReferenceLoadID', data['ReferenceLoadID']),
                        Field('Reship_Batched_TDS', data['Reship_Batched_TDS']),
                        Field('Reship_Incompatible_Flag', data['Reship_Incompatible_Flag'] and 'Y' or 'N'),
                        Field('Reship_Item_Code', data['Reship_Item_Code']),
                        Field('Reship_Item_Description', data['Reship_Item_Description']),
                        Field('Reship_ItemID', data['Reship_ItemID']),
                        Field('Reship_Load_Code', data['Reship_Load_Code']),
                        Field('Reship_LoadID', data['Reship_LoadID']),
                        Field('Reship_Qty', data['Reship_Qty']),
                        Field('Reship_Qty_UOM', data['Reship_Qty_UOM']),
                        Field('Reship_Source_Type', data['Reship_Source_Type']),
                        Field('RowPointer', data['RowPointer']),
                        Field('Run_Ack_Counter', data['Run_Ack_Counter']),
                        Field('Run_Req_Counter', data['Run_Req_Counter']),
                        Field('Sequence_Code', data['Sequence_Code']),
                        Field('SequenceID', data['SequenceID']),
                        Field('Sequencing_Flag', data['Sequencing_Flag'] and 'Y' or 'N'),
                        Field('Simulate_Status', data['Simulate_Status']),
                        Field('Slump', data['Slump']),
                        Field('Slump_Code', data['Slump_Code']),
                        Field('Slump_UOM', data['Slump_UOM']),
                        Field('SlumpID', data['SlumpID']),
                        Field('Started_Load_Status', data['Started_Load_Status']),
                        Field('UpdatedBy_Batch', data['UpdatedBy']),
                        Field('Washout_Used_Flag', data['Washout_Used_Flag'] and 'Y' or 'N'),
                        Field('Water_Cement_Ratio', data['Water_Cement_Ratio']),
                        Field('Water_In_Truck', data['Water_In_Truck']),
                        Field('Water_In_Truck_UOM', data['Water_In_Truck_UOM']),
                        Field('Item_Code', data['Item_Code']),
                        Field('Ticket_Code', data['Ticket_Code']),
                        Field('M_Warehouse_ID', m_warehouse_id),
                        Field('IsActive', 'N')
                    ]
    
                    wsc = WebServiceConnection()
                    wsc.url = ad_url
                    wsc.attempts = 1
                    wsc.app_name = 'InsertEBatchLoad'
    
                    try:
                        response = wsc.send_request(ws)
                        wsc.print_xml_request()
                        wsc.print_xml_response()
                       
                        if response.status == WebServiceResponseStatus.Error:
                            print('Error: ' + response.error_message)
                        else:
                            print('RecordID: ' + str(response.record_id))
                            for field in response.output_fields:
                                print(str(field.column) + ': ' + str(field.value))
                            print('---------------------------------------------')
                            print('Web Service Type: ' + ws.web_service_type)
                            print('Attempts: ' + str(wsc.attempts_request))
                            print('Time: ' + str(wsc.time_request))
                            proc.c_load_id = response.record_id
                            load_ok = True
                    except Exception as e:
                        print(e)
    
                    cr.close()
    
                    if load_ok:
                        proc.save()
            
            proc_lines = LoadLineProc.objects.filter(load_id__in=procs.values_list('load_id', flat=True))
            for proc_line in proc_lines:
                loadline_ok = False
                proc_line.state = 'CO'
                
                cr = cn.cursor(as_dict=True)
                cr.execute("select * from LOAD_LINE where Load_LineID like '%s' and Delete_Flag = 0" % (proc_line.loadline_id))
                data = cr.fetchone()
                
                if data:
                    time.sleep(0.01)
                    ws = CreateDataRequest()
                    ws.web_service_type = 'InsertEBatchLoadLine'
                    ws.login = login
    
                    ws.data_row = [
                        Field('C_Batch_Load_ID', proc_line.load_id.c_load_id),
                        Field('Absorbed_Water', data['Absorbed_Water']),
                        Field('Absorption_Percent', data['Absorption_Percent']),
                        Field('Actual_Water', data['Actual_Water']),
                        Field('Actual_Water_Calc_Type', data['Actual_Water_Calc_Type']),
                        Field('Adjust_UOM', data['Adjust_UOM']),
                        Field('Alias_Code', data['Alias_Code']),
                        Field('Amt_UOM', data['Amt_UOM']),
                        Field('Archive_Flag', data['Archive_Flag'] and 'Y' or 'N'),
                        Field('Based_On_Factor', data['Based_On_Factor']),
                        Field('Based_On_Qty', data['Based_On_Qty']),
                        Field('Based_On_UOM', data['Based_On_UOM']),
                        Field('Calc_Factor', data['Calc_Factor']),
                        Field('Calc_Moisture_Percent', data['Calc_Moisture_Percent']),
                        Field('Correction_Factor', data['Correction_Factor']),
                        Field('CreateDate', data['CreateDate']),
                        Field('CreatedBy_Batch', data['CreatedBy']),
                        Field('Delete_Flag', data['Delete_Flag'] and 'Y' or 'N'),
                        Field('Design_Absorbed_Water', data['Design_Absorbed_Water']),
                        Field('Design_Entry_Qty', data['Design_Entry_Qty']),
                        Field('Design_Free_Water', data['Design_Free_Water']),
                        Field('Design_SSD_Net_Target_Qty', data['Design_SSD_Net_Target_Qty']),
                        Field('Design_SSD_Qty', data['Design_SSD_Qty']),
                        Field('Design_UOM', data['Design_UOM']),
                        Field('Dispatch_Design_Qty', data['Dispatch_Design_Qty']),
                        Field('Dispatch_Design_UOM', data['Dispatch_Design_UOM']),
                        Field('Do_Not_Batch_Flag', data['Do_Not_Batch_Flag'] and 'Y' or 'N'),
                        Field('Effectiveness_Percent', data['Effectiveness_Percent']),
                        Field('Ingred_ItemID', data['Ingred_ItemID']),
                        Field('Ingredient_Source_Type', data['Ingredient_Source_Type']),
                        Field('Item_Code', data['Item_Code']),
                        Field('Item_Description', data['Item_Description']),
                        Field('Kgs_Per_Liter', data['Kgs_Per_Liter']),
                        Field('Load_Adjust_Qty', data['Load_Adjust_Qty']),
                        Field('Load_LineID', data['Load_LineID']),
                        Field('LoadID', data['LoadID']),
                        Field('Manual_Feed_Flag', data['Manual_Feed_Flag'] and 'Y' or 'N'),
                        Field('Modified_Flag', data['Modified_Flag'] and 'Y' or 'N'),
                        Field('Moisture_Entry_Ref_Type', data['Moisture_Entry_Ref_Type']),
                        Field('Net_Auto_Batched_Amt', data['Net_Auto_Batched_Amt']),
                        Field('Net_Batched_Amt', data['Net_Batched_Amt']),
                        Field('Net_Target_Amt', data['Net_Target_Amt']),
                        Field('Net_Used_Amt', data['Net_Used_Amt']),
                        Field('NoteExistsFlag', data['NoteExistsFlag']),
                        Field('RecordDate', data['RecordDate']),
                        Field('RowPointer', data['RowPointer']),
                        Field('Scale_UOM', data['Scale_UOM']),
                        Field('Slump_Factor', data['Slump_Factor']),
                        Field('Solids_Specific_Gravity', data['Solids_Specific_Gravity']),
                        Field('Sort_Line_Num', data['Sort_Line_Num']),
                        Field('Specific_Gravity', data['Specific_Gravity']),
                        Field('Substitution_Factor', data['Substitution_Factor']),
                        Field('Tolerance_Over_Amt', data['Tolerance_Over_Amt']),
                        Field('Tolerance_Under_Amt', data['Tolerance_Under_Amt']),
                        Field('Total_Moisture_Percent', data['Total_Moisture_Percent']),
                        Field('Trim_Qty', data['Trim_Qty']),
                        Field('Trim_UOM', data['Trim_UOM']),
                        Field('UpdatedBy_Batch', data['UpdatedBy']),
                        Field('Water_UOM', data['Water_UOM']),
                        Field('IsActive', 'N')
                    ]
    
                    wsc = WebServiceConnection()
                    wsc.url = ad_url
                    wsc.attempts = 1
                    wsc.app_name = 'InsertEBatchLoadLine'
                     
                    response = wsc.send_request(ws)
                    wsc.print_xml_request()
                    wsc.print_xml_response()
                    try:
                        if response.status == WebServiceResponseStatus.Error:
                            print('Error: ' + response.error_message)
                        else:
                            print('RecordID: ' + str(response.record_id))
                            for field in response.output_fields:
                                print(str(field.column) + ': ' + str(field.value))
                            print('---------------------------------------------')
                            print('Web Service Type: ' + ws.web_service_type)
                            print('Attempts: ' + str(wsc.attempts_request))
                            print('Time: ' + str(wsc.time_request))
                            proc_line.c_loadline_id = response.record_id
                            proc_line.save()
                    except Exception as e:
                        print(e)
            
            proc_lines = LoadLineProc.objects.filter(state='CO', load_id__in=procs.values_list('load_id', flat=True))
            for proc_line in proc_lines:
                loadline_ok = False
                proc_line.state = 'UP'
                
                time.sleep(0.01)
                ws = UpdateDataRequest()
                ws.web_service_type = 'ActivateEBatchLoadLine'
                ws.record_id = proc_line.c_loadline_id
                ws.login = login
    
                ws.data_row = [
                    Field('IsActive', 'Y')
                ]
    
                wsc = WebServiceConnection()
                wsc.url = ad_url
                wsc.attempts = 1
                wsc.app_name = 'ActivateEBatchLoadLine'
                 
                response = wsc.send_request(ws)
                wsc.print_xml_request()
                wsc.print_xml_response()
                try:
                    if response.status == WebServiceResponseStatus.Error:
                        print('Error: ' + response.error_message)
                    else:
                        print('RecordID: ' + str(response.record_id))
                        for field in response.output_fields:
                            print(str(field.column) + ': ' + str(field.value))
                        print('---------------------------------------------')
                        print('Web Service Type: ' + ws.web_service_type)
                        print('Attempts: ' + str(wsc.attempts_request))
                        print('Time: ' + str(wsc.time_request))
                        # proc_line.c_loadline_id = response.record_id
                        proc_line.save()
                except Exception as e:
                    print(e)
            
            procs = LoadProc.objects.filter(state='CO', load_end__range=[load_end_from, load_end_to])  # LoadProc.objects.filter(state='CO', c_load_id__isnull=False)
            for proc in procs:
                load_ok = False
                proc.state = 'UP'
    
                time.sleep(0.01)
                ws = UpdateDataRequest()
                ws.web_service_type = 'ActivateEBatchLoad'
                ws.record_id = proc.c_load_id
                ws.login = login
    
                ws.data_row = [
                    Field('IsActive', 'Y')
                ]
    
                wsc = WebServiceConnection()
                wsc.url = ad_url
                wsc.attempts = 1
                wsc.app_name = 'ActivateEBatchLoad'
    
                try:
                    response = wsc.send_request(ws)
                    wsc.print_xml_request()
                    wsc.print_xml_response()
                   
                    if response.status == WebServiceResponseStatus.Error:
                        print('Error: ' + response.error_message)
                    else:
                        print('RecordID: ' + str(response.record_id))
                        for field in response.output_fields:
                            print(str(field.column) + ': ' + str(field.value))
                        print('---------------------------------------------')
                        print('Web Service Type: ' + ws.web_service_type)
                        print('Attempts: ' + str(wsc.attempts_request))
                        print('Time: ' + str(wsc.time_request))
                        # proc.c_load_id = response.record_id
                        proc.save()
                except Exception as e:
                    print(e)
                
        context['load_end_from'], context['load_end_to'] = load_end_from, load_end_to
        context['object_list'] = LoadProc.objects.filter(load_end__range=[load_end_from, load_end_to])
#         for test in context['object_list'].values_list('load_id'):
#             print test
    return render(request, 'ui/loadproc_date_list.html', context)
