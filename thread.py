from idempierewsc.request import CreateDataRequest
from idempierewsc.base import LoginRequest
from idempierewsc.enums import WebServiceResponseStatus
from idempierewsc.net import WebServiceConnection
from idempierewsc.base import Field

from ui.models import BatchConfig, BatchProc
import pymssql
import time

# global  ad_client_id, ad_org_id, ad_role_id, ad_url, m_warehouse_id,\
#         username, password, mssql_host, mssql_port, mssql_db, mssql_user,\
#         mssql_pass, login, cn

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

cn = pymssql.connect(
    server=mssql_host,
    user=mssql_user,
    password=mssql_pass,
    database=mssql_db
)

while True:
    try:
        cr = cn.cursor()
        cr.execute('select BatchID,CreateDate from BATCH order by CreateDate desc')
        rw = cr.fetchall()
        
        for data in rw:
            batch_proc = False
            try:
                batch_proc = BatchProc.objects.get(batch_id=data[0])
            except:
                pass
            if batch_proc:
                break
            else:
                batch_proc = BatchProc()
                batch_proc.batch_id = data[0]
                batch_proc.batch_created = data[1]
                batch_proc.save()
        cr.close()
        
        batch_procs = BatchProc.objects.filter(state='DR')
        for batch_proc in batch_procs:
            time.sleep(0.001)
            batch_proc.state = 'CO'
             
            cr = cn.cursor(as_dict=True)
            cr.execute("select * from BATCH where BatchID like '%s'" % (batch_proc.batch_id))
            data = cr.fetchone()
             
            ws = CreateDataRequest()
            ws.web_service_type = 'InsertCBatchPlant'
            ws.login = login
              
            ws.data_row = [
                Field('M_Warehouse_ID', m_warehouse_id),
                Field('BatchID', data['BatchID']),
                Field('LoadID', data['LoadID']),
                Field('Mixer_DeviceID', data['Mixer_DeviceID']),
                Field('Mixed_TS', float(data['Mixed_TS'])),
                Field('Batch_Start_TDS', data['Batch_Start_TDS']),
                Field('Discharge_Start_TDS', data['Discharge_Start_TDS']),
                Field('Discharge_Complete_TDS', data['Discharge_Complete_TDS']),
                Field('Delete_Flag', data['Delete_Flag'] or 'N'),
                Field('Archive_Flag', data['Archive_Flag'] or 'N'),
                Field('RecordDate', data['RecordDate']),
                Field('NoteExistsFlag', data['NoteExistsFlag'] or 'N'),
                Field('CreatedBy_Batch', data['CreatedBy']),
                Field('UpdatedBy_Batch', data['UpdatedBy']),
                Field('RowPointer', data['RowPointer']),
                Field('Batch_Num', data['Batch_Num']),
                Field('Device_Code', data['Device_Code']),
                Field('Batch_Size', float(data['Batch_Size'])),
                Field('Power_Target_Req', data['Power_Target_Req'] or 'N'),
                Field('Power_Tolerance_Status', data['Power_Tolerance_Status'] or 'N'),
                Field('Power_Actual', data['Power_Actual']),
                Field('Temperature', data['Temperature']),
                Field('Temperature_UOM', data['Temperature_UOM']),
                Field('Power_Target', data['Power_Target']),
            ]
              
            wsc = WebServiceConnection()
            wsc.url = ad_url
            wsc.attempts = 3
            wsc.app_name = 'InsertCBatchPlant'
             
#             try:
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
#             except Exception as e:
#                 print(e)
     
            cr.close()
            batch_proc.c_batch_plant_id = response.record_id
            
            cr = cn.cursor(as_dict=True)
            cr.execute("select * from BATCH_LINE where BatchID like '%s'" % (batch_proc.batch_id))
            datas = cr.fetchall()

            for data in datas:
                time.sleep(0.001)
                ws = CreateDataRequest()
                ws.web_service_type = 'InsertCBatchPlantLine'
                ws.login = login
                
                ws.data_row = [
                    Field('C_Batch_Plant_ID', batch_proc.c_batch_plant_id),
                    Field('Absorbed_Water', data['Absorbed_Water']),
                    Field('Actual_Scale_Batched_Amt', data['Actual_Scale_Batched_Amt']),
                    Field('Actual_Scale_Target_Amt', data['Actual_Scale_Target_Amt']),
                    Field('Actual_Water', data['Actual_Water']),
                    Field('Alias_Code', data['Alias_Code']),
                    Field('Amt_UOM', data['Amt_UOM']),
                    Field('Archive_Flag', bool(data['Archive_Flag']) and 'Y' or 'N'),
                    Field('BatchID', data['BatchID']),
                    Field('Batch_LineID', data['Batch_LineID']),
                    Field('CreatedDate', data['CreateDate']),
                    Field('CreatedBy_Batch', data['CreatedBy']),
                    Field('Delete_Flag', bool(data['Delete_Flag']) and 'Y' or 'N'),
                    Field('Ingred_ItemID', data['Ingred_ItemID']),
                    Field('Item_Code', data['Item_Code']),
                    Field('Item_Description', data['Item_Description']),
                    Field('Layer_Num', data['Layer_Num']),
                    Field('Moisture_Auto_Status', bool(data['Moisture_Auto_Status']) and 'Y' or 'N'),
                    Field('Moisture_Percent', data['Moisture_Percent']),
                    Field('Net_Batched_Amt', data['Net_Batched_Amt']),
                    Field('Net_Target_Amt', data['Net_Target_Amt']),
                    Field('NoteExistsFlag', bool(data['NoteExistsFlag']) and 'Y' or 'N'),
                    Field('RecordDate', data['RecordDate']),
                    Field('RowPointer', data['RowPointer']),
                    Field('Sort_Line_Num', data['Sort_Line_Num']),
                    Field('Temperature', data['Temperature']),
                    Field('Temperature_UOM', data['Temperature_UOM']),
                    Field('Tol_Status', bool(data['Tol_Status']) and 'Y' or 'N'),
                    Field('UpdatedBy_Batch', data['UpdatedBy']),
                    Field('Water_UOM', data['Water_UOM']),
                ]
                
                wsc = WebServiceConnection()
                wsc.url = ad_url
                wsc.attempts = 3
                wsc.app_name = 'InsertCBatchPlantLine'
                
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
            cr.close()
            
            batch_proc.save()

    except Exception as e:
        print(e)