import urllib.request
import json
import mysql.connector
import datetime
import sched, time

def GetStats():
    urlIT = r'https://api.hh.ru/vacancies?area=88&clusters=true&enable_snippets=true&specialization=1&from=cluster_professionalArea'
    responseIT_stats = urllib.request.urlopen(urlIT)

    responseBytes = responseIT_stats.read()
    data = json.loads(responseBytes.decode("utf-8"))
    
    print(data['found'])

    mydb = mysql.connector.connect(host="localhost", user="root"
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
    responseBytes = responseIT_vacs.read()
    data = json.loads(responseBytes.decode("utf-8"))

    #print(data['items']['id'])

    mydb = mysql.connector.connect(host="localhost", user="root"
                                   , passwd="19031994", database="hhVacs")
    mycursor = mydb.cursor()
    sql = "INSERT INTO hh_Vacancies (ID, Name, Area, SalaryFrom, SalaryTo, Requirement, Responsibility) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    
    vacID = data['items'][0]['id']
    name = data['items'][0]['name']
    areaID = int(data['items'][0]['area']['id']) if int(data['items'][0]['area']['id']) else ''
    salaryFrom = int(data['items'][0]['salary']['from']) if data['items'][0]['salary']['from'] != None else None
    salaryTo = int(data['items'][0]['salary']['to']) if data['items'][0]['salary']['to'] != None else None
    requirement = data['items'][0]['snippet']['requirement'] if data['items'][0]['snippet']['requirement'] else ''
    responsibility = data['items'][0]['snippet']['responsibility'] if data['items'][0]['snippet']['responsibility'] else ''
    
    
    values = (vacID, name, areaID, salaryFrom, salaryTo, requirement, responsibility)

    mycursor.execute(sql, values)
    mydb.commit()
    print(mycursor.rowcount, "record inserted at ", datetime.datetime.now())


GetStats()
GetVacancies()

""" 
while(1):
    #GetStats()
    GetVacancies()
    time.sleep(200) """