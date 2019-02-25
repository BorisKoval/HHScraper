#scrapy runspider hhParser.py -o quotes.json ---

import urllib.request
import json
import mysql.connector
import datetime
import sched, time

def GetStats():
    urlIT = r'https://api.hh.ru/vacancies?area=88&clusters=true&enable_snippets=true&specialization=1&from=cluster_professionalArea'
    responseIT_vacs = urllib.request.urlopen(urlIT)

    #print(responseIT_vacs.read())  

    data = json.load(responseIT_vacs)
    print(data['found'])

    mydb = mysql.connector.connect(host="192.168.0.20", user="root"
                                   , passwd="19031994", database="hhVacs")
    mycursor = mydb.cursor()
    sql = "INSERT INTO vacsStats (DateTime, Url, Found) VALUES (%s, %s, %s)"
    val = (datetime.datetime.now(), urlIT, data['found'])
    mycursor.execute(sql, val)
    mydb.commit()

    print(mycursor.rowcount, "record inserted at ", datetime.datetime.now())

def GetVacancies():
    urlIT = r'https://api.hh.ru/vacancies?area=88&clusters=true&enable_snippets=true&specialization=1&from=cluster_professionalArea'
    responseIT_vacs = urllib.request.urlopen(urlIT)

    data = json.load(responseIT_vacs)
    #print(data['found']) 
    #print(data['items']) 

    print(data['items']['id'])

    mydb = mysql.connector.connect(host="192.168.0.20", user="root"
                                   , passwd="19031994", database="hhVacs")
    mycursor = mydb.cursor()
    sql = "INSERT INTO hh_Vacancies (ID, Name, Area, Salary, Requirement, Responsibility) VALUES (%s, %s, %s, %s, %s, %s)"

    val = (data['items'][0]['id'],data['items'][0]['name'], int(data['items'][0]['area']['id']), int(data['items'][0]['salary']['to'])
           , data['items'][0]['snippet']['requirement'] if data['items'][0]['snippet']['requirement'] else ''
           , data['items'][0]['snippet']['responsibility'] if data['items'][0]['snippet']['responsibility'] else '')
    
    mycursor.execute(sql, val)
    mydb.commit()
    print(mycursor.rowcount, "record inserted at ", datetime.datetime.now())

while(1):
    #GetStats()
    GetVacancies()
    time.sleep(200)