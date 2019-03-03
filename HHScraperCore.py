import urllib.request
import json
import mysql.connector
import datetime
import logging

logging.basicConfig(filename = 'log_'+str(datetime.date.today())+'.log', filemode='a', level = logging.INFO)

#добавление кол-ва открытых вакансий
def AddStatsToDB():
    urlIT = r'https://api.hh.ru/vacancies?area=88&clusters=true&enable_snippets=true&specialization=1&from=cluster_professionalArea'
    data = GetRequestedData()
    
    mydb = mysql.connector.connect(host="localhost", user="root"
                                   , passwd="19031994", database="HHScraperData")

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
    mydb = mysql.connector.connect(host="localhost", user="root"
                                   , passwd="19031994", database="HHScraperData")
    mycursor = mydb.cursor()

    data = GetRequestedData()
    values = []
    for page in range(int(data['pages'])):
        for i in range(int(data['per_page'])):
            vacID = data['items'][i]['id']

            if (CheckVacancieInDB(vacID, mycursor, 'Vacancies')): continue
            
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
    mydb = mysql.connector.connect(host="localhost", user="root"
                                   , passwd="19031994", database="HHScraperData")
    mycursor = mydb.cursor()
    sql = "SELECT ID FROM Vacancies"
    mycursor.execute(sql)       

    vacs = mycursor.fetchall()
    vacsDetails = []
    for vacId in vacs:

        if CheckVacancieInDB(vacId[0], mycursor, 'VacanciesDetails'): #проверка существует ли уже запись в БД
            continue

        url = r'https://api.hh.ru/vacancies/'+str(vacId[0])
        try: #проверка, т.к. вакансия уже быть закрыта и URL открыть не получится
            responseIT_vacs = urllib.request.urlopen(url)
            responseBytes = responseIT_vacs.read()
        except:
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
    urlIT = r'https://api.hh.ru/vacancies?area=88&clusters=true&enable_snippets=true&specialization=1&from=cluster_professionalArea&page='+str(pageNum)
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

#выполнение всех методовdatetime.datetime.now()
def ParseAllData():
    print("\nStart scan:", datetime.datetime.now())
    logging.info("\nStart scan:" + str(datetime.datetime.now()))

    AddStatsToDB()
    AddVacanciesToDB()
    AddVacanciesDetailsToDB()

    print("End scan:", datetime.datetime.now())
    logging.info("End scan:" + str(datetime.datetime.now())+'\n')

ParseAllData()



#----------------------------------
#Temporary Method
def GetVacancie():
    urlIT = r'https://api.hh.ru/vacancies?area=88&clusters=true&enable_snippets=true&specialization=1&from=cluster_professionalArea'
    responseIT_vacs = urllib.request.urlopen(urlIT)
    responseBytes = responseIT_vacs.read()
    data = json.loads(responseBytes.decode("utf-8"))

    #print(data['items']['id'])

    mydb = mysql.connector.connect(host="localhost", user="root"
                                   , passwd="19031994", database="HHScraperData")
    mycursor = mydb.cursor()
    sql = "INSERT INTO Vacancies (ID, Name, Area, SalaryFrom, SalaryTo, Requirement, Responsibility) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    
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