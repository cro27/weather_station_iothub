#!/usr/bin/env python

# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for
# full license information.

import getopt
import sys
import random
import time
import sys
import re
from datetime import datetime
from dateutil import tz

import pywws.weatherstation

from iothub_client import IoTHubClient, IoTHubClientError, IoTHubTransportProvider, IoTHubClientResult
from iothub_client import IoTHubMessage, IoTHubMessageDispositionResult, IoTHubError, DeviceMethodReturnValue
import config as config
from telemetry import Telemetry

# String containing Hostname, Device Id & Device Key in the format:
# "HostName=<host_name>;DeviceId=<device_id>;SharedAccessKey=<device_key>"

CONNECTION_STRING = "HostName=<your_iot_hub_name>.azure-devices.net;DeviceId=<your_registered_deviceID>;SharedAccessKey=<your_device_key>"
DeviceID = "<your_deviceID>"

ws = pywws.weatherstation.WeatherStation()

# HTTP options
# Because it can poll "after 9 seconds" polls will happen effectively
# at ~10 seconds.
# Note that for scalabilty, the default value of minimumPollingTime
# is 25 minutes. For more information, see:
# https://azure.microsoft.com/documentation/articles/iot-hub-devguide/#messaging
TIMEOUT = 241000
MINIMUM_POLLING_TIME = 9

# messageTimeout - the maximum time in milliseconds until a message times out.
# The timeout period starts at IoTHubClient.send_event_async.
# By default, messages do not expire.
MESSAGE_TIMEOUT = 10000
RECEIVE_CONTEXT = 0
MESSAGE_COUNT = 0
MESSAGE_SWITCH = True
TWIN_CONTEXT = 0
SEND_REPORTED_STATE_CONTEXT = 0
METHOD_CONTEXT = 0
TEMPERATURE_ALERT = 40.0

# global counters
RECEIVE_CALLBACKS = 0
SEND_CALLBACKS = 0
BLOB_CALLBACKS = 0
TWIN_CALLBACKS = 0
SEND_REPORTED_STATE_CALLBACKS = 0
METHOD_CALLBACKS = 0
EVENT_SUCCESS = "success"
EVENT_FAILED = "failed"

# chose HTTP, AMQP or MQTT as transport protocol
PROTOCOL = IoTHubTransportProvider.HTTP

telemetry = Telemetry()
MSG_TXT = "{\"deviceId\": \"%s\",\"rec_time\": \"%s\",\"hum_out\": %d,\"temp_out\": %.1f,\"hum_in\": %d,\"temp_in\": %.1f, \"abs_pres\": %.1f,\"wind_gust\": %.1f,\"wind_avg\": %.1f,\"wind_dir\": %d,\"wind_pos\": \"%s\",\"rain_total\": %.1f}"

def receive_message_callback(message, counter):
    global RECEIVE_CALLBACKS
    message_buffer = message.get_bytearray()
    size = len(message_buffer)
    print ( "Received Message [%d]:" % counter )
    print ( "    Data: <<<%s>>> & Size=%d" % (message_buffer[:size].decode("utf-8"), size) )
    map_properties = message.properties()
    key_value_pair = map_properties.get_internals()
    print ( "    Properties: %s" % key_value_pair )
    counter += 1
    RECEIVE_CALLBACKS += 1
    print ( "    Total calls received: %d" % RECEIVE_CALLBACKS )
    return IoTHubMessageDispositionResult.ACCEPTED


def send_confirmation_callback(message, result, user_context):
    global SEND_CALLBACKS
    print ( "Confirmation[%d] received for message with result = %s" % (user_context, result) )
    map_properties = message.properties()
    #print ( "    message_id: %s" % message.message_id )
    #print ( "    correlation_id: %s" % message.correlation_id )
    key_value_pair = map_properties.get_internals()
    #print ( "    Properties: %s" % key_value_pair )
    SEND_CALLBACKS += 1
    #print ( "    Total calls confirmed: %d" % SEND_CALLBACKS )


def device_twin_callback(update_state, payload, user_context):
    global TWIN_CALLBACKS
    print ( "\nTwin callback called with:\nupdateStatus = %s\npayload = %s\ncontext = %s" % (update_state, payload, user_context) )
    TWIN_CALLBACKS += 1
    print ( "Total calls confirmed: %d\n" % TWIN_CALLBACKS )


def send_reported_state_callback(status_code, user_context):
    global SEND_REPORTED_STATE_CALLBACKS
    print ( "Confirmation for reported state received with:\nstatus_code = [%d]\ncontext = %s" % (status_code, user_context) )
    SEND_REPORTED_STATE_CALLBACKS += 1
    print ( "    Total calls confirmed: %d" % SEND_REPORTED_STATE_CALLBACKS )


def device_method_callback(method_name, payload, user_context):
    global METHOD_CALLBACKS,MESSAGE_SWITCH
    print ( "\nMethod callback called with:\nmethodName = %s\npayload = %s\ncontext = %s" % (method_name, payload, user_context) )
    METHOD_CALLBACKS += 1
    print ( "Total calls confirmed: %d\n" % METHOD_CALLBACKS )
    device_method_return_value = DeviceMethodReturnValue()
    device_method_return_value.response = "{ \"Response\": \"This is the response from the device\" }"
    device_method_return_value.status = 200
    if method_name == "start":
        MESSAGE_SWITCH = True
        print ( "Start sending message\n" )
        device_method_return_value.response = "{ \"Response\": \"Successfully started\" }"
        return device_method_return_value
    if method_name == "stop":
        MESSAGE_SWITCH = False
        print ( "Stop sending message\n" )
        device_method_return_value.response = "{ \"Response\": \"Successfully stopped\" }"
        return device_method_return_value
    return device_method_return_value


def blob_upload_conf_callback(result, user_context):
    global BLOB_CALLBACKS
    print ( "Blob upload confirmation[%d] received for message with result = %s" % (user_context, result) )
    BLOB_CALLBACKS += 1
    print ( "    Total calls confirmed: %d" % BLOB_CALLBACKS )


def iothub_client_init():
    # prepare iothub client
    client = IoTHubClient(CONNECTION_STRING, PROTOCOL)
    client.set_option("product_info", "HappyPath_RaspberryPi-Python")
    if client.protocol == IoTHubTransportProvider.HTTP:
        client.set_option("timeout", TIMEOUT)
        client.set_option("MinimumPollingTime", MINIMUM_POLLING_TIME)
    # set the time until a message times out
    client.set_option("messageTimeout", MESSAGE_TIMEOUT)
    # to enable MQTT logging set to 1
    if client.protocol == IoTHubTransportProvider.MQTT:
        client.set_option("logtrace", 0)
    client.set_message_callback(
        receive_message_callback, RECEIVE_CONTEXT)
    if client.protocol == IoTHubTransportProvider.MQTT or client.protocol == IoTHubTransportProvider.MQTT_WS:
        client.set_device_twin_callback(
            device_twin_callback, TWIN_CONTEXT)
        client.set_device_method_callback(
            device_method_callback, METHOD_CONTEXT)
    return client

def print_last_message_time(client):
    try:
        last_message = client.get_last_message_receive_time()
        print ( "Last Message: %s" % time.asctime(time.localtime(last_message)) )
        print ( "Actual time : %s" % time.asctime() )
    except IoTHubClientError as iothub_client_error:
        if iothub_client_error.args[0].result == IoTHubClientResult.INDEFINITE_TIME:
            print ( "No message received" )
        else:
            print ( iothub_client_error )

def wind_direction(dir_num):
    if dir_num == 0:
        return 'S'
    elif dir_num == 1: 
        return 'SSW'
    elif dir_num == 2:
        return 'SW'
    elif dir_num == 3:
        return 'WSW'
    elif dir_num == 4:
        return 'W'
    elif dir_num == 5:
        return 'WNW'
    elif dir_num == 6:
        return 'NW'
    elif dir_num == 7:
        return 'NNW'
    elif dir_num == 8:
        return 'N'
    elif dir_num == 9: 
        return 'NNE'
    elif dir_num == 10:
        return 'NE'
    elif dir_num == 11:
        return 'ENE'
    elif dir_num == 12:
        return 'E'
    elif dir_num == 13:
        return 'ESE'
    elif dir_num == 14:
        return 'SE'
    elif dir_num == 15:
        return 'SSE'

def iothub_client_run():
    try:
        client = iothub_client_init()
        if client.protocol == IoTHubTransportProvider.MQTT:
            print ( "IoTHubClient is reporting state" )
            reported_state = "{\"newState\":\"standBy\"}"
            client.send_reported_state(reported_state, len(reported_state), send_reported_state_callback, SEND_REPORTED_STATE_CONTEXT)
        telemetry.send_telemetry_data(parse_iot_hub_name(), EVENT_SUCCESS, "IoT hub connection is established")
        while True:
            global MESSAGE_COUNT,MESSAGE_SWITCH
            if MESSAGE_SWITCH:
		        #-------------------------------------------------------------------------------------------
		        # This for loop reads the live data from the weather station and converts some of the data.
		        #     ptrraw is the WS pointer to circluar log buffer offset (16 bytes)
		        #     ptrhex ptr in Hex little endian
		        #     Convert to correct Relative Pressure HPA for location by adding 9.6
		        #     Convert MPS to KPH
                # Error Handling:
                #   Check for lost connection or null data. If found break from loop and do not send message
		        #-------------------------------------------------------------------------------------------
                for data, ptr, logged in ws.live_data():
                    record_time_local = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')
                    print("\n")
                    print(record_time_local)
                    ws_status = data['status']
                    ws_conn = data['status']['lost_connection']
		            # Check for lost connection
                    if ws_conn == True:
                        print("Lost Connection to Weather Station")
                        break
                    # Check for Null Data
                    if ws_conn == None:
                        print("No Data Returned From Weather Station")
                        break
                    # if connection OK process the data
                    ptrraw = ptr        
                    ptrhex = "%04x" % ptr       
                    record_time_utc = data['idx'].strftime('%Y-%m-%d %H:%M:%S')
                    hum_out = data['hum_out']
                    temp_out = data['temp_out']
                    hum_in = data['hum_in']
                    temp_in = data['temp_in']
                    abs_pres = data['abs_pressure']+9.6 
                    wind_gust = data['wind_gust']*3.6  
                    wind_avg = data['wind_ave']*3.6    
                    wind_dir = data['wind_dir']
                    wind_pos = wind_direction(wind_dir)
                    rain = data['rain']
                    msg_txt_formatted = MSG_TXT % (
                        DeviceID,
                        record_time_local,
                        hum_out,
                        temp_out,
                        hum_in,
                        temp_in,
                        abs_pres,
                        wind_gust,
                        wind_avg,
                        wind_dir,
                        wind_pos,
                        rain)
                    break
                if ws_conn == False:
                    print ( "IoTHubClient sending %d messages" % MESSAGE_COUNT )
                    message = IoTHubMessage(msg_txt_formatted)
                    # optional: assign ids
                    message.message_id = "message_%d" % MESSAGE_COUNT
                    message.correlation_id = "correlation_%d" % MESSAGE_COUNT
                    # optional: assign properties
                    prop_map = message.properties()
                    prop_map.add("temperatureAlert", "true" if temp_out > TEMPERATURE_ALERT else "false")

                    client.send_event_async(message, send_confirmation_callback, MESSAGE_COUNT)
                    print ( "IoTHubClient.send_event_async accepted message [%d] for transmission to IoT Hub." % MESSAGE_COUNT )

                    status = client.get_send_status()
                    print ( "Send status: %s" % status )
                    MESSAGE_COUNT += 1
                time.sleep(config.MESSAGE_TIMESPAN / 1000.0)

    except IoTHubError as iothub_error:
        print ( "Unexpected error %s from IoTHub" % iothub_error )
        telemetry.send_telemetry_data(parse_iot_hub_name(), EVENT_FAILED, "Unexpected error %s from IoTHub" % iothub_error)
        return
    except KeyboardInterrupt:
        print ( "IoTHubClient stopped" )
    print_last_message_time(client)


def parse_iot_hub_name():
    m = re.search("HostName=(.*?)\.", CONNECTION_STRING)
    return m.group(1)

if __name__ == "__main__":
    print ( "\nPython %s" % sys.version )
    print ( "IoT Hub Client for Python" )
    iothub_client_run()
