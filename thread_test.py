from idempierewsc.request import CreateDataRequest, UpdateDataRequest
from idempierewsc.base import LoginRequest
from idempierewsc.enums import WebServiceResponseStatus
from idempierewsc.net import WebServiceConnection
from idempierewsc.base import Field

from ui.models import *
import pymssql
import time

ad_url = 'http://apps.sisierp.id/'

login = LoginRequest()
login.client_id = 1000003
login.org_id = 1000006
login.role_id = 1000196
login.password = 'UserWebService'
login.user = 'UserWebService'
login.warehouse_id = 1000009

if True:
    ws = UpdateDataRequest()
    ws.web_service_type = 'ActivateEBatchLoadLine'
    ws.record_id = 1033324
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
    except Exception as e:
        print(e)

    ws = UpdateDataRequest()
    ws.web_service_type = 'ActivateEBatchLoad'
    ws.record_id = 1005090
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
    except Exception as e:
        print(e)
