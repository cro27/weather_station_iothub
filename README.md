#Send USB weather station data to Azure 

**Purpose**

Software to integrate USB weather station with Azure via IoT Hub and process the data in a lambda architecture.

**Design**

This repository contains software to send weather data from popular USB wireless weather stations that typically use the EasyWeather Windows software, to Microsoft Azure for processing and display. 

**Description**

The field solution component runs on a Raspberry Pi running Raspian and Python 2.7, and uses Jim Easterbrook's [Pywws package ](https://github.com/jim-easterbrook/pywws) to interface with the weather station to read and decode the data, and the [Microsoft IoT Client SDK for Python] (https://github.com/Azure/azure-iot-sdk-python) to send the data to Azure IoT Hub. 
The Python module azweather.py which is based on the IoT Hub Client sample run module, runs in a continuous loop on the Raspberry Pi, reading data from the weather station at typically 48 second intervals, providing error handling for null reads, and sending the data through to Azure IoT Hub.

The Azure component ingests the data stream through IoT Hub and splits it into a **hot** and **cold** path using Azure Stream Analytics. 

The **Hot** path is consumed directly by Power Bi as a Live Data Stream using Live Tiles on the dashboard for Temperature, Humidity, Relative Pressure, Wind Speed, Wind Gust and Rainfall in the last hour. The Temperature, Air Pressure and Wind Gust line charts are also based upon the hot path data stream. 

The **Cold** path is divided into 2 streams by Azure Stream Analytics. Firstly, the complete data stream is written to Azure Blob storage, partitioned on Year, Month, Day, Hour, for aggregate processing by Databricks before being written to the serving layer. The second stream consisiting of temperature, humidity and relative pressure is sent to an Azure Macine Learning Web API which returns the rain probability percentage and is added to this streams' data, which is then written directly to the serving layer.

Azure Databricks is used to aggregate the weather data on an hourly basis. A Python notebook is scheduled to run every hour in Azure Databricks using an on-demand Job Cluster. The notebook calculates the folder on Azure Blob for the previous hours data, reads it into a dataframe and temporary table, then uses Spark SQL to aggregate the data before writing it directly to the serving layer.

The serving layer is an Azure SQL Database which consists of 2 denormalised tables, (one for hourly aggregates of the weather data, and one for rainfall probability data) and a handful of views to slice the aggregate data by hour, day, week and month. 

The visualization layer is a Power BI dashboard comprised of a mixture of live and direct query tiles, charts and reports. Power BI provides a desktop and mobile view of the dashboard allowing weather data reporting from anywhere at anytime. 

The following diagram illustrates the solution architecture:

![](https://github.com/cro27/weather_station_iothub/blob/master/weather_data_Azure_IOT_Lambda.jpg)

**Requirements**

Essential: [Python](https://www.python.org/) 2.7

Essential: USB library [python-libusb1](https://pypi.org/project/libusb1/)

Essential: [Azure Iot Hub SDK](https://github.com/Azure/azure-iot-sdks)

**Install Guide**

Under Construction

**Credits**

Jim Easterbrook for the Pywws package to interface with the weather station. Although Pywws provides methods to send the acquired data to external services like Weather Underground, Twitter and AWS S3, I found it difficult to incorporate the Azure IoT Hub into the Pywws structure and so chose to incorporate Pywws Getweatherdata into the IoT Hub Client as an easier path. 

**References**

http://pywws.readthedocs.io/en/latest/

https://github.com/Azure-Samples/iot-hub-python-raspberrypi-client-app

https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-raspberry-pi-kit-python-get-started

https://docs.microsoft.com/en-us/azure/iot-hub/quickstart-send-telemetry-python

### **Field Components on Raspberry Pi**

#### Pywws package

#### IoT Hub Client SDK

#### Azweather.py

####Azureweather (shell script)

####Azureweather.service (systemd service)

####39-weather-station.rules (udev rules)

### **Azure Components**

#### IoT Hub

#### Azure Storage Account and Blob Container

#### Power BI Account

#### Stream Analytics Jobs

#### Databricks Workspace

#### Azure ML Studio Workspace













