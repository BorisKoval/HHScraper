import urllib.request
import json
import mysql.connector
import datetime
import sched, time
import apscheduler

def GetStats():
    urlIT = r'https://api.hh.ru/vacancies?area=88&clusters=true&enable_snippets=true&specialization=1&from=cluster_professionalArea'
    responseIT_stats = urllib.request.urlopen(urlIT)

    responseBytes = responseIT_stats.read()
    data = json.loads(responseBytes.decode("utf-8"))
   
    mydb = mysql.connector.connect(host="localhost", user="root"
                                   , passwd="19031994", database="hhVacs")
    mycursor = mydb.cursor()
    sql = "INSERT INTO vacsStats (DateTime, Url, Found) VALUES (%s, %s, %s)"
    val = (datetime.datetime.now(), urlIT, data['found'])
    mycursor.execute(sql, val)
    mydb.commit()

    print("{0} record ({1}) inserted at {2}".format(mycursor.rowcount, data['found'], datetime.datetime.now()))

def GetVacancie():
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

def GetRequestedData(pageNum):
    #perPage = 50
    urlIT = r'https://api.hh.ru/vacancies?area=88&clusters=true&enable_snippets=true&specialization=1&from=cluster_professionalArea&page='+str(pageNum)
    responseIT_vacs = urllib.request.urlopen(urlIT)
    responseBytes = responseIT_vacs.read()
    data = json.loads(responseBytes.decode("utf-8"))
    return data

def AddVacanciesToDB():
    data = GetRequestedData(0)
    values = []
    for page in range(int(data['pages'])):
        for i in range(int(data['per_page'])):
            vacID = data['items'][i]['id']

            if (CheckVacancieInDB(vacID)): continue
            
            dateTime = datetime.datetime.now()
            name = data['items'][i]['name']
            areaID = int(data['items'][i]['area']['id']) if int(data['items'][i]['area']['id']) else ''
            if data['items'][i]['salary'] !=  None:
                salaryFrom = int(data['items'][i]['salary']['from']) if data['items'][i]['salary']['from'] != None else None
                salaryTo = int(data['items'][i]['salary']['to']) if data['items'][i]['salary']['to'] != None else None
            requirement = data['items'][i]['snippet']['requirement'] if data['items'][i]['snippet']['requirement'] else ''
            responsibility = data['items'][i]['snippet']['responsibility'] if data['items'][i]['snippet']['responsibility'] else ''
            
            value = (dateTime, vacID, name, areaID, salaryFrom, salaryTo, requirement, responsibility)
            values.append(value)
        data = GetRequestedData(page)

    mydb = mysql.connector.connect(host="localhost", user="root"
                                   , passwd="19031994", database="hhVacs")

    mycursor = mydb.cursor()
    sql = '''INSERT INTO hh_Vacancies (DateTime, ID, Name, Area, 
    SalaryFrom, SalaryTo, Requirement, Responsibility) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''

    mycursor.executemany(sql, values)
    mydb.commit()
    print(mycursor.rowcount, "records inserted at ", datetime.datetime.now())

def CheckVacancieInDB(vacID):
    mydb = mysql.connector.connect(host="localhost", user="root"
                                   , passwd="19031994", database="hhVacs")
    mycursor = mydb.cursor()
    sql = "SELECT * FROM hh_Vacancies WHERE ID = (%s)"
    mycursor.execute(sql, (vacID,))

    if mycursor.fetchall():
        return True

#GetStats()
#GetVacancie()
#AddVacanciesToDB()

""" 
while(1):
    #GetStats()
    GetVacancie()
    time.sleep(200) """


""" todo:
    +/- *добавление всех данных сразу (проход по всем страницам)
        *настройка apscheduler или подобного для запланированного запуска
        *добавить дату создания (парсить) и дату закрытия (когда скрипт увидел что она закрыта)
            -дату закрытия проверять отдельным методом (!)
        *отдельная таблица с полным описанием и требованиями
            -там добавить требуемый опыт
        *добавить работадателя
"""