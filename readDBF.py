#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov  8 08:08:43 2017

@author: jose-luis
"""
from dbfread import DBF
import json,requests
import numpy as np
import pandas as pd
import hashlib
import re
from itertools import compress
import os
import csv
import base64
from cryptography.fernet import Fernet
import ast
import subprocess


def getAreaDict(filename) :
    result = {}
#    print(filename)
    for record in DBF(filename) :
        name = record['name']
        #SAGA (see Thiessen polygon operations) doesn't play nice with unicode and messes up the station names
        #The following is a horrible encoding hack to accound for screwed up unicode. 
        #This is a house of cards that will break if you look at it funny
        name.replace('\\','\\\\')
        name = name[2:-1]
        name = name.encode().decode('unicode-escape').encode('latin1').decode('utf-8')
        if name in result :
            result[name] = result[name] + record['AREA']
        else :
            result[name] = record['AREA']
    return result

def getAccessToken(encryptedCredentials,password) :
    password = password.zfill(32)
    password=base64.b64encode(bytes(password,'utf-8'))
    cipher_suite=Fernet(password)
    data = ast.literal_eval(cipher_suite.decrypt(encryptedCredentials).decode('utf-8'))
    query = requests.post('https://data.met.no/auth/requestAccessToken', data=data)
    data = json.loads(query.text)
    return data['access_token']

def getStationsInPolygon(accessToken,polygon,dateInterval) :
    headers = {}
    headers['Authorization'] = 'Bearer ' + accessToken
    headers['Accept'] = 'application/vnd.no.met.data.locations-v0+json'    
    queryParameters = {}
    queryParameters['fields'] = 'name,geometry'
    queryParameters['validtime'] = dateInterval
    query = requests.get('https://data.met.no/sources/v0.jsonld', headers=headers, params=dict(polygon,**queryParameters))    
    insideStations = json.loads(query.text)
    idString =  ','.join([i['id'] for i in insideStations['data'] if i.get('name')])
    ids = [i['id'] for i in insideStations['data'] if i.get('name')]
    names = [i['name'] for i in insideStations['data'] if i.get('name')]
    coordinates = [i['geometry']['coordinates'] for i in insideStations['data'] if i.get('name')]
    stationsDict = dict(zip(ids,names))
    coordsDict = dict(zip(names,coordinates))
    return idString, stationsDict, coordsDict,insideStations

def getStationsWithTimeSeries(accessToken,stationsID,dateInterval,parameterList) :
    headers ={}
    queryParameters = {}
    headers['Accept']='application/vnd.no.met.data.observations.timeseries-v0+json'
    headers['Authorization'] = 'Bearer ' + accessToken
    queryParameters['sources'] = stationsID
    queryParameters['elements'] = ','.join(parameterList)
    queryParameters['referencetime'] = dateInterval
    
    query = requests.get('https://data.met.no/observations/availableTimeSeries/v0.jsonld', headers=headers, params=queryParameters)
    stations = json.loads(query.text)
    
    #Finding unique identifiers in order to accumulate data within a day
    for i in stations['data'] :
        myHash = hashlib.md5( str.encode( i['elementId'] + i['sourceId'] ) )
        i['hash'] = myHash.hexdigest()
#        print(i['hash'], i['elementId'] + i['sourceId'])
    
    #Going through unique hashes, keeping only values measured at 0600
    for i in set([d['hash'] for d in stations['data']]) :
        stations['data'] = [x for x in stations['data'] if x['hash']!=i or (x['hash']==i and x['timeOffset']=='PT06H')]
    for i in stations['data'] :
        i['sourceId'] = i['sourceId'].replace(':0','')
    return stations

def backspace(n):
    print('\r', end='') 

def downloadStations(accessToken,stations,stationsDict,dateInterval) :
    headers = {}
    headers['Accept'] = 'application/vnd.no.met.data.observations-v0+json'
    headers['Authorization'] = 'Bearer ' + accessToken
    dataParameters = {}
    dataParameters['referencetime'] =  dateInterval
    cnt = 0
    print('Downloading data: ') 
    for i in stations['data'] :
        dataParameters['elements'] = i['elementId']
        dataParameters['sources'] =  i['sourceId']
        dataQuery = requests.get('https://data.met.no/observations/v0.jsonld',headers=headers, params=dataParameters)
        data = json.loads(dataQuery.text)
        values = np.array([i['observations'][0]['value'] for i in data['data']])
        timestamp = np.array([i['referenceTime'] for i in data['data']],dtype='datetime64[D]')
        columnName = i['elementId'] + "\n" + stationsDict[i['sourceId']]
        displayStr = i['elementId'] + ' ' + stationsDict[i['sourceId']]
#        print("Progress: {}".format(displayStr), end="\b" * len(displayStr))
        print(displayStr)
        if cnt == 0 :
            allData = pd.DataFrame(data=values,index=timestamp,columns=[columnName])
        else :
            allData = pd.merge(allData,pd.DataFrame(data=values,index=timestamp,columns=[columnName]),left_index = True, right_index = True, how = 'outer') 
        cnt = cnt + 1
#        backspace(len(displayStr))
    precipitation = allData.filter(regex="precipitation")
    precipitation[precipitation==-1] = 0 #Need to check if -1 means no precipitation or missing measurement
    temperature = allData.filter(regex="temperature")   
    precipitation.rename(columns=lambda x : re.sub('^.*\n','',x),inplace=True)
    temperature.rename(columns=lambda x : re.sub('^.*\n','',x),inplace=True) 
    precipitation.to_pickle('precipitation')
    temperature.to_pickle('temperature')
    return allData


def getArealWeight(basinShape,thiessenBuffer,dataframe,insideStations,coordsDict,epsgCode) :
    #Creating directory to store shapefile
    shapesDir = './shapes'
    if not os.path.exists(shapesDir):
        os.makedirs(shapesDir)
    
    #Creating a dictionary with weight for the stations with data
    weightDict = {}    
    
    #Finding out the unique combinations of precipitation stations with data for any given day
    nameList = dataframe.columns.values
    #print(nameList)
    idList = [next(item['id'] for item in insideStations['data'] if item["name"] == x) for x in nameList]
    cnt = 0;
    print('Processing shape: '),
    for index, row in dataframe.notnull().drop_duplicates().iterrows() :
        #Creating csv file with geographic information and attributes for the stations that have data 
        print(str(cnt) + ' ', end='')
        csvFile = './shapes/dummy.csv'
        cf = open(csvFile, 'w')
        writer = csv.writer(cf)
        writer.writerow(('longitude','latitude','stationId','name'))
        cnt = cnt + 1;
        currentStations = list(compress(nameList, list(row)))
#        print(currentStations)
        currentIds = list(compress(idList,list(row)))
        currentCoordinates = [coordsDict[x] for x in currentStations]
        filename = os.path.join(shapesDir,'Voronoi.shp')
        for i,j,k in zip(currentStations,currentIds,currentCoordinates) :
            writer.writerow((k[0],k[1],j.encode('utf-8'),i.encode('utf-8')))
        #Increasing extent if less than three stations are available
        if (len(currentStations) < 3) :
            writer.writerow((4.0,54.0,'dummy'.encode('utf-8'),'dummy'.encode('utf-8')))
            writer.writerow((4.0,64.0,'dummy'.encode('utf-8'),'dummy'.encode('utf-8')))
            writer.writerow((14.0,54.0,'dummy'.encode('utf-8'),'dummy'.encode('utf-8')))
            writer.writerow((14.0,64.0,'dummy'.encode('utf-8'),'dummy'.encode('utf-8')))
        cf.close()
        
        #Making ESRI shapefile from csv file
        cmd = 'ogr2ogr ' + filename + ' ' + csvFile + ' -oo X_POSSIBLE_NAMES=lon* -oo Y_POSSIBLE_NAMES=lat* -oo KEEP_GEOM_COLUMNS=NO -lco ENCODING=UTF-8 -overwrite'
        subprocess.check_call(cmd, shell=True,stdout=open(os.devnull, 'wb'))
        #Setting the stations shapefile to the same project as the basin shapefile
        cmd = 'ogr2ogr shapes ' + filename + ' -t_srs EPSG:' + str(epsgCode) + ' -s_srs EPSG:4326 -a_srs EPSG:' + str(epsgCode) + ' -overwrite'
        subprocess.check_call(cmd, shell=True,stdout=open(os.devnull, 'wb'))    
        #Computing Thiessen polygons from shapefile
        cmd = 'saga_cmd shapes_points 16 -POINTS ' + filename + ' -POLYGONS ' + filename + ' -FRAME ' + str(thiessenBuffer)
        subprocess.check_call(cmd, shell=True,stdout=open(os.devnull, 'wb'))
        #Mask polygons using catchment extent
        cmd = 'saga_cmd shapes_polygons 14 -A ' + filename + ' -B ' + basinShape + '.shp -RESULT ' + filename
        #    cmd = 'saga_cmd shapes_polygons 14 -A ' + filename + ' -B /home/jose-luis/Dropbox/NIVA/Modelling_HBV_Oyvind/SRTM_data/Alna/Processed/Alna_basin.shp -RESULT ' + filename
        subprocess.check_call(cmd, shell=True,stdout=open(os.devnull, 'wb'))
        #Computing area of polygons in shapefile
        cmd= 'saga_cmd shapes_polygons 2 -POLYGONS ' + filename 
        subprocess.check_call(cmd, shell=True,stdout=open(os.devnull, 'wb'))
        #Reading area from .dbf file
        pre,ext = os.path.splitext(filename)
        result = getAreaDict(pre + '.dbf')
        myHash = hashlib.md5(np.array(row))
        weightDict[myHash.hexdigest()] =  result
        
#        print(sum(result.values()))
    return weightDict