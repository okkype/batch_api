from idempierewsc.request import CreateDataRequest, UpdateDataRequest
from idempierewsc.base import LoginRequest
from idempierewsc.enums import WebServiceResponseStatus
from idempierewsc.net import WebServiceConnection
from idempierewsc.base import Field
from django.db.models import Q

from ui.models import *
import pymssql
import time

import requests, zlib, json
from datetime import datetime, timedelta

# global  ad_client_id, ad_org_id, ad_role_id, ad_url, m_warehouse_id,\
#         username, password, mssql_host, mssql_port, mssql_db, mssql_user,\
#         mssql_pass, login, cn

ad_client_id = BatchConfig.objects.get(name='ad_client_id').value
ad_org_id = BatchConfig.objects.get(name='ad_org_id').value
ad_role_id = BatchConfig.objects.get(name='ad_role_id').value
ad_url = BatchConfig.objects.get(name='ad_url').value
m_warehouse_id = BatchConfig.objects.get(name='m_warehouse_id').value
combatchmachine = BatchConfig.objects.get(name='combatchmachine') and BatchConfig.objects.get(name='combatchmachine').value or ''
username = BatchConfig.objects.get(name='username').value
password = BatchConfig.objects.get(name='password').value
mssql_host = BatchConfig.objects.get(name='mssql_host').value
mssql_port = BatchConfig.objects.get(name='mssql_port').value
mssql_db = BatchConfig.objects.get(name='mssql_db').value
mssql_user = BatchConfig.objects.get(name='mssql_user').value
mssql_pass = BatchConfig.objects.get(name='mssql_pass').value
mssql_limit_by = BatchConfig.objects.get(name='mssql_limit_by') and BatchConfig.objects.get(name='mssql_limit_by').value or 'DAY'
mssql_limit = BatchConfig.objects.get(name='mssql_limit') and BatchConfig.objects.get(name='mssql_limit').value or '-7'

login = LoginRequest()
login.client_id = ad_client_id
login.org_id = ad_org_id
login.role_id = ad_role_id
login.password = password
login.user = username
login.warehouse_id = m_warehouse_id

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

# while True:
while uuid:
    try:
        cr = cn.cursor()
        cr.execute('''
            SELECT
                LOAD.LoadID,
                LOAD.Load_End_TDS,
                TICKET.Ticket_Code
            FROM
                LOAD,
                TICKET_LINE,
                TICKET
            WHERE
                LOAD.LoadID = TICKET_LINE.LoadID
                AND TICKET.Ticket_Code <> 0
                AND TICKET_LINE.TicketID = TICKET.TicketID
                AND TICKET_LINE.Delete_Flag = 0
                AND LOAD.Load_End_TDS IS NOT NULL
                AND LOAD.Load_End_TDS >= DATEADD(%s, %s, GETDATE())
            ORDER BY
                LOAD.Load_End_TDS DESC;
        ''' % (mssql_limit_by, mssql_limit))
        rw = cr.fetchall()
         
        for data in rw:
            proc = False
            try:
                proc = LoadProc.objects.get(pk=data[0])
            except:
                pass
            if proc:
                pass
            else:
                proc = LoadProc()
                proc.load_id = data[0]
                proc.load_end = data[1]
                proc.ticket_code = data[2]
                try:
                    proc.save()
                except:
                    pass
        cr.close()
        
        procs = LoadProc.objects.filter(Q(c_load_id__isnull=True) | Q(c_load_id='')).order_by('load_end')
        for proc in procs:
            cr = cn.cursor()
            cr.execute("select Load_LineID from LOAD_LINE where LoadID like '%s' and Delete_Flag = 0" % (proc.load_id))
            datas = cr.fetchall()

            for data in datas:
                proc_line = LoadLineProc()
                proc_line.load_id = proc
                proc_line.loadline_id = data[0]
                try:
                    proc_line.save()
                except:
                    pass
                
            cr.close()
        
        procs = LoadProc.objects.filter(Q(c_load_id__isnull=True) | Q(c_load_id='')).order_by('load_end')
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
#                 ws = CreateDataRequest()
#                 ws.web_service_type = 'InsertEBatchLoad'
#                 ws.login = login
# 
#                 ws.data_row = [
#                     Field('Alias_Code', data['Alias_Code']),
#                     Field('Apply_Add_Trim_Flag', data['Apply_Add_Trim_Flag'] and 'Y' or 'N'),
#                     Field('Archive_Flag', data['Archive_Flag'] and 'Y' or 'N'),
#                     Field('Auto_TDS', data['Auto_TDS']),
#                     Field('BCP_Load_ID', data['BCP_Load_ID']),
#                     Field('BCP_Load_Sort', data['BCP_Load_Sort']),
#                     Field('Calc_Ack_Counter', data['Calc_Ack_Counter']),
#                     Field('Calc_Req_Counter', data['Calc_Req_Counter']),
#                     Field('Charge_Rate_Percent', data['Charge_Rate_Percent']),
#                     Field('Control_Sys_Code', data['Control_Sys_Code']),
#                     Field('Control_SysID', data['Control_SysID']),
#                     Field('CreateDate', data['CreateDate']),
#                     Field('CreatedBy_Batch', data['CreatedBy']),
#                     Field('DemandID', data['DemandID']),
#                     Field('Design_Slump', data['Design_Slump']),
#                     Field('Ended_Load_Status', data['Ended_Load_Status']),
#                     Field('Failed_Flag', data['Failed_Flag'] and 'Y' or 'N'),
#                     Field('Load_Code', data['Load_Code']),
#                     Field('Load_Discharge_TDS', data['Load_Discharge_TDS']),
#                     Field('Load_End_TDS', data['Load_End_TDS']),
#                     Field('Load_Size', data['Load_Size']),
#                     Field('Load_Sort_Num', data['Load_Sort_Num']),
#                     Field('Load_Start_TDS', data['Load_Start_TDS']),
#                     Field('LoadID', data['LoadID']),
#                     Field('LoadStarted_UserCode', data['LoadStarted_UserCode']),
#                     Field('Manual_Feed_Flag', data['Manual_Feed_Flag'] and 'Y' or 'N'),
#                     Field('Manual_TDS', data['Manual_TDS']),
#                     Field('Max_Batch', data['Max_Batch']),
#                     Field('Max_Batch_Size', data['Max_Batch_Size']),
#                     Field('Max_Batch_Size_UOM', data['Max_Batch_Size_UOM']),
#                     Field('Max_Dispatch_WCR', data['Max_Dispatch_WCR']),
#                     Field('Mix_Entry_Ref_Type', data['Mix_Entry_Ref_Type']),
#                     Field('Mixer_TS', data['Mixer_TS']),
#                     Field('NoteExistsFlag', data['NoteExistsFlag']),
#                     Field('Notify_UserCode', data['Notify_UserCode']),
#                     Field('RecordDate', data['RecordDate']),
#                     Field('ReferenceLoadID', data['ReferenceLoadID']),
#                     Field('Reship_Batched_TDS', data['Reship_Batched_TDS']),
#                     Field('Reship_Incompatible_Flag', data['Reship_Incompatible_Flag'] and 'Y' or 'N'),
#                     Field('Reship_Item_Code', data['Reship_Item_Code']),
#                     Field('Reship_Item_Description', data['Reship_Item_Description']),
#                     Field('Reship_ItemID', data['Reship_ItemID']),
#                     Field('Reship_Load_Code', data['Reship_Load_Code']),
#                     Field('Reship_LoadID', data['Reship_LoadID']),
#                     Field('Reship_Qty', data['Reship_Qty']),
#                     Field('Reship_Qty_UOM', data['Reship_Qty_UOM']),
#                     Field('Reship_Source_Type', data['Reship_Source_Type']),
#                     Field('RowPointer', data['RowPointer']),
#                     Field('Run_Ack_Counter', data['Run_Ack_Counter']),
#                     Field('Run_Req_Counter', data['Run_Req_Counter']),
#                     Field('Sequence_Code', data['Sequence_Code']),
#                     Field('SequenceID', data['SequenceID']),
#                     Field('Sequencing_Flag', data['Sequencing_Flag'] and 'Y' or 'N'),
#                     Field('Simulate_Status', data['Simulate_Status']),
#                     Field('Slump', data['Slump']),
#                     Field('Slump_Code', data['Slump_Code']),
#                     Field('Slump_UOM', data['Slump_UOM']),
#                     Field('SlumpID', data['SlumpID']),
#                     Field('Started_Load_Status', data['Started_Load_Status']),
#                     Field('UpdatedBy_Batch', data['UpdatedBy']),
#                     Field('Washout_Used_Flag', data['Washout_Used_Flag'] and 'Y' or 'N'),
#                     Field('Water_Cement_Ratio', data['Water_Cement_Ratio']),
#                     Field('Water_In_Truck', data['Water_In_Truck']),
#                     Field('Water_In_Truck_UOM', data['Water_In_Truck_UOM']),
#                     Field('Item_Code', data['Item_Code']),
#                     Field('Ticket_Code', data['Ticket_Code']),
#                     Field('M_Warehouse_ID', m_warehouse_id),
#                     Field('ComBatchMachine', combatchmachine),
#                     Field('IsActive', 'N')
#                 ]
# 
#                 wsc = WebServiceConnection()
#                 wsc.url = ad_url
#                 wsc.attempts = 1
#                 wsc.app_name = 'InsertEBatchLoad'

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
                                        data['Alias_Code'] or '0',
                                        data['Apply_Add_Trim_Flag'] and 'Y' or 'N',
                                        data['Archive_Flag'] and 'Y' or 'N',
                                        data['Auto_TDS'] or '1970-01-01 00:00:00',
                                        data['BCP_Load_ID'] or '0',
                                        data['BCP_Load_Sort'] or '0',
                                        data['Calc_Ack_Counter'] or '0',
                                        data['Calc_Req_Counter'] or '0',
                                        data['Charge_Rate_Percent'] or '0',
                                        data['Control_Sys_Code'] or '0',
                                        data['Control_SysID'] or '0',
                                        data['CreateDate'] or '1970-01-01 00:00:00',
                                        data['CreatedBy'] or '0',
                                        data['DemandID'] or '0',
                                        data['Design_Slump'] or '0',
                                        data['Ended_Load_Status'] or '0',
                                        data['Failed_Flag'] and 'Y' or 'N',
                                        data['Load_Code'] or '0',
                                        data['Load_Discharge_TDS'] or '1970-01-01 00:00:00',
                                        data['Load_End_TDS'] or '1970-01-01 00:00:00',
                                        data['Load_Size'] or '0',
                                        data['Load_Sort_Num'] or '0',
                                        data['Load_Start_TDS'] or '1970-01-01 00:00:00',
                                        data['LoadID'] or '0',
                                        data['LoadStarted_UserCode'] or '0',
                                        data['Manual_Feed_Flag'] and 'Y' or 'N',
                                        data['Manual_TDS'] or '1970-01-01 00:00:00',
                                        data['Max_Batch'] or '0',
                                        data['Max_Batch_Size'] or '0',
                                        data['Max_Batch_Size_UOM'] or '0',
                                        data['Max_Dispatch_WCR'] or '0',
                                        data['Mix_Entry_Ref_Type'] or '0',
                                        data['Mixer_TS'] or '0',
                                        data['NoteExistsFlag'] and 'Y' or 'N',
                                        data['Notify_UserCode'] or '0',
                                        data['RecordDate'] or '1970-01-01 00:00:00',
                                        data['ReferenceLoadID'] or '0',
                                        data['Reship_Batched_TDS'] or '1970-01-01 00:00:00',
                                        data['Reship_Incompatible_Flag'] and 'Y' or 'N',
                                        data['Reship_Item_Code'] or '0',
                                        data['Reship_Item_Description'] or '0',
                                        data['Reship_ItemID'] or '0',
                                        data['Reship_Load_Code'] or '0',
                                        data['Reship_LoadID'] or '0',
                                        data['Reship_Qty'] or '0',
                                        data['Reship_Qty_UOM'] or '0',
                                        data['Reship_Source_Type'] or '0',
                                        data['RowPointer'] or '0',
                                        data['Run_Ack_Counter'] or '0',
                                        data['Run_Req_Counter'] or '0',
                                        data['Sequence_Code'] or '0',
                                        data['SequenceID'] or '0',
                                        data['Sequencing_Flag'] and 'Y' or 'N',
                                        data['Simulate_Status'] or '0',
                                        data['Slump'] or '0',
                                        data['Slump_Code'] or '0',
                                        data['Slump_UOM'] or '0',
                                        data['SlumpID'] or '0',
                                        data['Started_Load_Status'] or '0',
                                        data['UpdatedBy'] or '0',
                                        data['Washout_Used_Flag'] and 'Y' or 'N',
                                        data['Water_Cement_Ratio'] or '0',
                                        data['Water_In_Truck'] or '0',
                                        data['Water_In_Truck_UOM'] or '0',
                                        data['Item_Code'] or '0',
                                        data['Ticket_Code'] or '0',
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
                finally:
                    pass
                # except Exception as e:
                    # print(e)

                cr.close()

#                 if load_ok:
#                     proc.save()
        
        proc_lines = LoadLineProc.objects.filter(c_loadline_id__isnull=True)
        for proc_line in proc_lines:
            loadline_ok = False
            proc_line.state = 'CO'
            
            cr = cn.cursor(as_dict=True)
            cr.execute("select * from LOAD_LINE where Load_LineID like '%s' and Delete_Flag = 0" % (proc_line.loadline_id))
            data = cr.fetchone()
            
            if data and proc_line.load_id.c_load_id:
                time.sleep(0.01)
#                 ws = CreateDataRequest()
#                 ws.web_service_type = 'InsertEBatchLoadLine'
#                 ws.login = login
# 
#                 ws.data_row = [
#                     Field('C_Batch_Load_ID', proc_line.load_id.c_load_id),
#                     Field('Absorbed_Water', data['Absorbed_Water']),
#                     Field('Absorption_Percent', data['Absorption_Percent']),
#                     Field('Actual_Water', data['Actual_Water']),
#                     Field('Actual_Water_Calc_Type', data['Actual_Water_Calc_Type']),
#                     Field('Adjust_UOM', data['Adjust_UOM']),
#                     Field('Alias_Code', data['Alias_Code']),
#                     Field('Amt_UOM', data['Amt_UOM']),
#                     Field('Archive_Flag', data['Archive_Flag'] and 'Y' or 'N'),
#                     Field('Based_On_Factor', data['Based_On_Factor']),
#                     Field('Based_On_Qty', data['Based_On_Qty']),
#                     Field('Based_On_UOM', data['Based_On_UOM']),
#                     Field('Calc_Factor', data['Calc_Factor']),
#                     Field('Calc_Moisture_Percent', data['Calc_Moisture_Percent']),
#                     Field('Correction_Factor', data['Correction_Factor']),
#                     Field('CreateDate', data['CreateDate']),
#                     Field('CreatedBy_Batch', data['CreatedBy']),
#                     Field('Delete_Flag', data['Delete_Flag'] and 'Y' or 'N'),
#                     Field('Design_Absorbed_Water', data['Design_Absorbed_Water']),
#                     Field('Design_Entry_Qty', data['Design_Entry_Qty']),
#                     Field('Design_Free_Water', data['Design_Free_Water']),
#                     Field('Design_SSD_Net_Target_Qty', data['Design_SSD_Net_Target_Qty']),
#                     Field('Design_SSD_Qty', data['Design_SSD_Qty']),
#                     Field('Design_UOM', data['Design_UOM']),
#                     Field('Dispatch_Design_Qty', data['Dispatch_Design_Qty']),
#                     Field('Dispatch_Design_UOM', data['Dispatch_Design_UOM']),
#                     Field('Do_Not_Batch_Flag', data['Do_Not_Batch_Flag'] and 'Y' or 'N'),
#                     Field('Effectiveness_Percent', data['Effectiveness_Percent']),
#                     Field('Ingred_ItemID', data['Ingred_ItemID']),
#                     Field('Ingredient_Source_Type', data['Ingredient_Source_Type']),
#                     Field('Item_Code', data['Item_Code']),
#                     Field('Item_Description', data['Item_Description']),
#                     Field('Kgs_Per_Liter', data['Kgs_Per_Liter']),
#                     Field('Load_Adjust_Qty', data['Load_Adjust_Qty']),
#                     Field('Load_LineID', data['Load_LineID']),
#                     Field('LoadID', data['LoadID']),
#                     Field('Manual_Feed_Flag', data['Manual_Feed_Flag'] and 'Y' or 'N'),
#                     Field('Modified_Flag', data['Modified_Flag'] and 'Y' or 'N'),
#                     Field('Moisture_Entry_Ref_Type', data['Moisture_Entry_Ref_Type']),
#                     Field('Net_Auto_Batched_Amt', data['Net_Auto_Batched_Amt']),
#                     Field('Net_Batched_Amt', data['Net_Batched_Amt']),
#                     Field('Net_Target_Amt', data['Net_Target_Amt']),
#                     Field('Net_Used_Amt', data['Net_Used_Amt']),
#                     Field('NoteExistsFlag', data['NoteExistsFlag']),
#                     Field('RecordDate', data['RecordDate']),
#                     Field('RowPointer', data['RowPointer']),
#                     Field('Scale_UOM', data['Scale_UOM']),
#                     Field('Slump_Factor', data['Slump_Factor']),
#                     Field('Solids_Specific_Gravity', data['Solids_Specific_Gravity']),
#                     Field('Sort_Line_Num', data['Sort_Line_Num']),
#                     Field('Specific_Gravity', data['Specific_Gravity']),
#                     Field('Substitution_Factor', data['Substitution_Factor']),
#                     Field('Tolerance_Over_Amt', data['Tolerance_Over_Amt']),
#                     Field('Tolerance_Under_Amt', data['Tolerance_Under_Amt']),
#                     Field('Total_Moisture_Percent', data['Total_Moisture_Percent']),
#                     Field('Trim_Qty', data['Trim_Qty']),
#                     Field('Trim_UOM', data['Trim_UOM']),
#                     Field('UpdatedBy_Batch', data['UpdatedBy']),
#                     Field('Water_UOM', data['Water_UOM']),
#                     Field('IsActive', 'N')
#                 ]
# 
#                 wsc = WebServiceConnection()
#                 wsc.url = ad_url
#                 wsc.attempts = 1
#                 wsc.app_name = 'InsertEBatchLoadLine'
#                  
#                 response = wsc.send_request(ws)
#                 wsc.print_xml_request()
#                 wsc.print_xml_response()
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
                                        data["Absorbed_Water"] or '0',
                                        data["Absorption_Percent"] or '0',
                                        data["Actual_Water"] or '0',
                                        data["Actual_Water_Calc_Type"] or '0',
                                        data["Adjust_UOM"] or '0',
                                        data["Alias_Code"] or '0',
                                        data["Amt_UOM"] or '0',
                                        data["Archive_Flag"] and "Y" or "N",
                                        data["Based_On_Factor"] or '0',
                                        data["Based_On_Qty"] or '0',
                                        data["Based_On_UOM"] or '0',
                                        data["Calc_Factor"] or '0',
                                        data["Calc_Moisture_Percent"] or '0',
                                        data["Correction_Factor"] or '0',
                                        data["CreateDate"] or '1970-01-01 00:00:00',
                                        data["CreatedBy"] or '0',
                                        data["Delete_Flag"] and "Y" or "N",
                                        data["Design_Absorbed_Water"] or '0',
                                        data["Design_Entry_Qty"] or '0',
                                        data["Design_Free_Water"] or '0',
                                        data["Design_SSD_Net_Target_Qty"] or '0',
                                        data["Design_SSD_Qty"] or '0',
                                        data["Design_UOM"] or '0',
                                        data["Dispatch_Design_Qty"] or '0',
                                        data["Dispatch_Design_UOM"] or '0',
                                        data["Do_Not_Batch_Flag"] and "Y" or "N",
                                        data["Effectiveness_Percent"] or '0',
                                        data["Ingred_ItemID"] or '0',
                                        data["Ingredient_Source_Type"] or '0',
                                        data["Item_Code"] or '0',
                                        data["Item_Description"] or '0',
                                        data["Kgs_Per_Liter"] or '0',
                                        data["Load_Adjust_Qty"] or '0',
                                        data["Load_LineID"] or '0',
                                        data["LoadID"] or '0',
                                        data["Manual_Feed_Flag"] and "Y" or "N",
                                        data["Modified_Flag"] and "Y" or "N",
                                        data["Moisture_Entry_Ref_Type"] or '0',
                                        data["Net_Auto_Batched_Amt"] or '0',
                                        data["Net_Batched_Amt"] or '0',
                                        data["Net_Target_Amt"] or '0',
                                        data["Net_Used_Amt"] or '0',
                                        data["NoteExistsFlag"] and "Y" or "N",
                                        data["RecordDate"] or '1970-01-01 00:00:00',
                                        data["RowPointer"] or '0',
                                        data["Scale_UOM"] or '0',
                                        data["Slump_Factor"] or '0',
                                        data["Solids_Specific_Gravity"] or '0',
                                        data["Sort_Line_Num"] or '0',
                                        data["Specific_Gravity"] or '0',
                                        data["Substitution_Factor"] or '0',
                                        data["Tolerance_Over_Amt"] or '0',
                                        data["Tolerance_Under_Amt"] or '0',
                                        data["Total_Moisture_Percent"] or '0',
                                        data["Trim_Qty"] or '0',
                                        data["Trim_UOM"] or '0',
                                        data["UpdatedBy"] or '0',
                                        data["Water_UOM"] or '0',
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
                finally:
                    pass
                # except Exception as e:
                    # print(e)
        
        proc_lines = LoadLineProc.objects.filter(state='CO', c_loadline_id__isnull=False)
        for proc_line in proc_lines:
            loadline_ok = False
            proc_line.state = 'UP'
            
            time.sleep(0.01)
#             ws = UpdateDataRequest()
#             ws.web_service_type = 'ActivateEBatchLoadLine'
#             ws.record_id = proc_line.c_loadline_id
#             ws.login = login
# 
#             ws.data_row = [
#                 Field('IsActive', 'Y')
#             ]
# 
#             wsc = WebServiceConnection()
#             wsc.url = ad_url
#             wsc.attempts = 1
#             wsc.app_name = 'ActivateEBatchLoadLine'
#              
#             response = wsc.send_request(ws)
#             wsc.print_xml_request()
#             wsc.print_xml_response()
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
            finally:
                pass
            # except Exception as e:
                # print(e)
        
        procs = LoadProc.objects.filter(state='CO', c_load_id__isnull=False)
        for proc in procs:
            load_ok = False
            proc.state = 'UP'

            time.sleep(0.01)
#             ws = UpdateDataRequest()
#             ws.web_service_type = 'ActivateEBatchLoad'
#             ws.record_id = proc.c_load_id
#             ws.login = login
# 
#             ws.data_row = [
#                 Field('IsActive', 'Y')
#             ]
# 
#             wsc = WebServiceConnection()
#             wsc.url = ad_url
#             wsc.attempts = 1
#             wsc.app_name = 'ActivateEBatchLoad'

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
            finally:
                pass
            # except Exception as e:
                # print(e)     
    finally:
        pass
    # except Exception as e:
        # print(e)
