import urllib.request
import json
import mysql.connector
import datetime
import logging

#параметры подключения к БД
dbConnectionParams = {'host':'localhost', 'user':'root', 'passwd':'19031994', 'database':'HHScraperData'}
#конфиг логов
logging.basicConfig(filename = 'logs/log_'+str(datetime.date.today())+'.log', filemode='a', level = logging.INFO)

#добавление кол-ва открытых вакансий
def AddStatsToDB():
    urlIT = r'https://api.hh.ru/vacancies?area=88&clusters=true&enable_snippets=true&specialization=1&from=cluster_professionalArea'
    data = GetRequestedData()
    
    mydb = mysql.connector.connect(**dbConnectionParams)

    mycursor = mydb.cursor()
    sql = "INSERT INTO Stats (DateTime, Url, Found) VALUES (%s, %s, %s)"
    val = (datetime.datetime.now(), urlIT, data['found'])
    mycursor.execute(sql, val)
    mydb.commit()
    mydb.close()
    #print("Found ({0}) open vacancies ({1})".format(data['found'], datetime.datetime.now()))
    logging.info("Found ({0}) open vacancies ({1})".format(data['found'], datetime.datetime.now()))

#добавления краткого описания вакансии в БД
def AddVacanciesToDB():
    mydb = mysql.connector.connect(**dbConnectionParams)
    mycursor = mydb.cursor()

    data = GetRequestedData()
    values = []
    tempVacIDs =[]
    for page in range(int(data['pages'])):
        for i in range(int(data['per_page'])):
            vacID = data['items'][i]['id']

            if CheckVacancieInDB(vacID, mycursor, 'Vacancies') or (vacID in tempVacIDs): continue
            
            tempVacIDs.append(vacID)

            dateTime = datetime.datetime.now() 
            name = data['items'][i]['name'] #заменить все на data.get (?), вернет none если не найдет ключ
            areaID = int(data['items'][i]['area']['id']) if int(data['items'][i]['area']['id']) else ''
            salaryFrom = None
            salaryTo = None
            if data['items'][i]['salary'] !=  None:
                salaryFrom = int(data['items'][i]['salary']['from']) if data['items'][i]['salary']['from'] != None else None
                salaryTo = int(data['items'][i]['salary']['to']) if data['items'][i]['salary']['to'] != None else None
            requirement = data['items'][i]['snippet']['requirement'] if data['items'][i]['snippet']['requirement'] else ''
            responsibility = data['items'][i]['snippet']['responsibility'] if data['items'][i]['snippet']['responsibility'] else ''
            
            value = (dateTime, vacID, name, areaID, salaryFrom, salaryTo, requirement, responsibility)
            values.append(value)
        data = GetRequestedData(page)

    sql = '''INSERT INTO Vacancies (DateTime, ID, Name, Area, 
    SalaryFrom, SalaryTo, Requirement, Responsibility) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''

    mycursor.executemany(sql, values)
    mydb.commit()
    mydb.close()
    #print(str(len(values)) + ' vacancies added | ' if len(values) > 0 else 'Null vacancies added | ', datetime.datetime.now())
    logging.info('{0} vacancies added ({1})'.format(str(len(values)) if len(values) > 0 else 'Null', str(datetime.datetime.now())))

#добавление детального описания вакансии в БД
def AddVacanciesDetailsToDB():
    mydb = mysql.connector.connect(**dbConnectionParams)
    mycursor = mydb.cursor()
    sql = "SELECT ID FROM Vacancies"
    mycursor.execute(sql)       
    vacs = mycursor.fetchall()

    vacsDetails = []
    tempVacIDs = [] #для проверки добавления задвоенных записей (непонятно откуда они...)
    for vacId in vacs:
        #проверка существует ли уже запись в БД, либо мы уже собираемся добавить ее
        if CheckVacancieInDB(vacId[0], mycursor, 'VacanciesDetails') or (vacId[0] in tempVacIDs):
            continue
        tempVacIDs.append(vacId[0])

        url = r'https://api.hh.ru/vacancies/'+str(vacId[0])
        try: #проверка, т.к. вакансия уже может быть закрыта и URL открыть не получится
            responseIT_vacs = urllib.request.urlopen(url)
            responseBytes = responseIT_vacs.read()
        except urllib.error.HTTPError as er: #логируем и ставим дату закрытия в БД
            if (er.code == 404):
                logging.info('Daleted vacancie ID is : '+str(vacId[0]))
                CloseVacancieInDB(str(vacId[0]), mycursor)
            continue
        except: #для всех других ошибок - идем дальше
            continue

        data = json.loads(responseBytes.decode("utf-8"))

        ID = data['id']
        dateTime = datetime.datetime.now() 
        createdDateTime = str(data['created_at']).replace('T', ' ')
        createdDateTime = createdDateTime[0:createdDateTime.find('+')]

        description = data['description']
        experience = data['experience']['name']
        employerName = data['employer']['name']
        employerID = data['employer']['id']

        value = [ID, dateTime, createdDateTime, description, experience, employerName, employerID]
        vacsDetails.append(value)

    sql = '''INSERT INTO VacanciesDetails (ID, DateTime, CreatedDateTime, Description, 
    Experience, EmployerName, EmployerID) 
    VALUES (%s, %s, %s, %s, %s, %s, %s)'''

    mycursor.executemany(sql, vacsDetails)
    mydb.commit()
    mydb.close()
    
    #print(str(len(vacsDetails) + ' detailed vacancies added | ') if len(vacsDetails) > 0 else 'Null detailed vacancies added | ', datetime.datetime.now())
    logging.info('{0} detailed vacancies added ({1})'.format(str(len(vacsDetails)) if len(vacsDetails) > 0 else 'Null', str(datetime.datetime.now())))

#получение данных по URLу
def GetRequestedData(pageNum=0):
    #perPage = 50
    urlIT = r'https://api.hh.ru/vacancies?area=88&clusters=true&enable_snippets=true&specialization=1&from=cluster_professionalArea&per_page=50&page='+str(pageNum)
    responseIT_vacs = urllib.request.urlopen(urlIT)
    responseBytes = responseIT_vacs.read()
    data = json.loads(responseBytes.decode("utf-8"))
    return data

#проверка, есть ли вакансия (id) в нужной таблице
def CheckVacancieInDB(vacID, dbCursor, tableName):
    sql = "SELECT * FROM {0} WHERE ID = (%s)".format(tableName)
    dbCursor.execute(sql, (vacID,))
    if dbCursor.fetchall():
        return True

#отправка даты закрытия вакансии (если запрос URL вернул по ней 404)
def CloseVacancieInDB(vacID, dbCursor):
    sql = "UPDATE VacanciesDetails SET CloseDateTime = '{0}' WHERE ID = (%s)".format(datetime.datetime.now())
    dbCursor.execute(sql, (vacID,))

#выполнение всех методов
def ParseAllData():
    print("\nStart scan:", datetime.datetime.now())
    logging.info("\nStart scan:" + str(datetime.datetime.now()))

    AddStatsToDB()
    AddVacanciesToDB()
    AddVacanciesDetailsToDB()

    print("End scan:", datetime.datetime.now())
    logging.info("End scan:" + str(datetime.datetime.now())+'\n')

ParseAllData()


