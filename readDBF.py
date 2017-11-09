#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov  8 08:08:43 2017

@author: jose-luis
"""
from dbfread import DBF


def getAreaDict(filename) :
    result = {}
    print(filename)
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

