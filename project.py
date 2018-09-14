# -*- coding: utf-8 -*-
"""
Created on Mon Sep  3 16:50:50 2018

@author: prote
"""

import sys
import sqlite3
import re
import json
import urllib.request
import re
from datetime import datetime

from geolite2 import geolite2


keyWords = ['cart','pay', 'success_pay']


listOfLines = []


''' Определение страны по IP адресу'''

def CountryByIP(ip):
    match = geolite2.reader()
    info = match.get(ip)
    geolite2.close()
    #print (info)
    if info is None:
        return('Unknown')
    elif info.get('country') is None:
        return ("Unknown")
    else:
        return (info['country']['names']['en'])


''' Функция передачи запросов в БД'''

def callDB(expression):
        with sqlite3.connect('project.db') as conn:
            cursor = conn.cursor()
            try: 
                cursor.execute(expression)
                result = cursor.fetchall()
            except sqlite3.Error as e:
                print ("Database error: %s" % e)
                result = -1

        return(result)    


''' Функция удаления всех таблиц из БД'''
def DeleteAllTables():
    result = callDB("SELECT name FROM sqlite_master WHERE type='table'")
    tableList = []
    for a in result:
        for tablename in a:
            tableList.append(tablename)
    tableList.pop(0)
    print (tableList)

    for table in tableList:
        callDB("DROP TABLE %s" %table)

'''  Функция создания и наполнения базы данных'''
def CreateAndFillDB():

    ''' Создадим таблицы в БД'''
    ''' CatalogTable - Таблица структуры продуктов и категорий
        SurfingTable - Таблица перемещений пользователя по сайту
        IpDict - справочник IP адресов пользователей
        ScriptTable - Таблица запросов от пользователя
        ScriptCodeTable - справочник запросов пользователя'''

    # DeleteAllTables()

    result = callDB("SELECT name FROM sqlite_master WHERE type='table'")
    tableList = []
    for a in result:
        for tablename in a:
            tableList.append(tablename)
    tableList.pop(0)
    # print (tableList)

    if "SurfingTable" not in tableList:
        result = callDB(
            '''CREATE TABLE `SurfingTable` (
    	`Index`	INTEGER NOT NULL PRIMARY KEY UNIQUE,
    	`Dat`	TEXT NOT NULL,
    	`Tim`	TEXT NOT NULL,
    	`IP`	INTEGER NOT NULL,
    	`CatCode`	INTEGER NOT NULL
    	 ); '''
        )

    if "CatalogTable" not in tableList:
        result = callDB(
            '''
            CREATE TABLE 'CatalogTable' (
            `Index`	INTEGER NOT NULL PRIMARY KEY UNIQUE,
            `Category`	TEXT,
            `Goods`	TEXT);    '''
        )

    '''if "IpDict" in tableList:
        callDB("DROP TABLE IpDict")
        tableList.remove("IpDict")
    '''
    if "IpDict" not in tableList:
        result = callDB(
            '''
            CREATE TABLE 'IpDict' (
            'Index' INTEGER NOT NULL PRIMARY KEY UNIQUE,
            'IP' TEXT NOT NULL,
            'Country_Code' TEXT NOT NULL);
            '''
        )

    if "ScriptTable" not in tableList:
        result = callDB(
            '''
            CREATE TABLE 'ScriptTable' (
            'Index' INTEGER NOT NULL PRIMARY KEY UNIQUE,
           	`Dat`	TEXT NOT NULL,
        	`Tim`	TEXT NOT NULL,
    	    `IP`	INTEGER NOT NULL,
    	    'ScriptID' INTEGER NOT NULL,
    	    'IDCart' INTEGER NOT NULL,
    	    'CatCode' INTEGER NOT NULL);
            '''
        )

    if "ScriptCodeTable" not in tableList:
        result = callDB(
            '''
            CREATE TABLE 'ScriptCodeTable' (
            'Index' INTEGER NOT NULL PRIMARY KEY UNIQUE,
            'ScriptName' TEXT NOT NULL
            )
            '''
        )

    '''Заполним таблицы имеющимися данными'''

    '''Заполним справочник ScriptCodeTable'''

    result = callDB("SELECT * FROM ScriptCodeTable")

    if result == []:
        print("Заполним таблицу ScriptCodeTable")

        for code in keyWords:
            print(code)
            result = callDB("INSERT INTO ScriptCodeTable (ScriptName) VALUES ('%s')" % code)
        print(callDB("SELECT * FROM ScriptCodeTable"))

    # result = callDB("DELETE FROM 'MainTable'")
    # print (result)

    ''' Заполним таблицу CatalogTable - структуру категорий и товаров '''

    result = callDB("SELECT * FROM CatalogTable")
    if result == []:
        print("Заполним таблицу CatalogTable")
        for good in uniqGoods:
            result = callDB("INSERT INTO CatalogTable (Category,Goods) VALUES ('%s','%s')" % (good[1], good[2]))

        print(callDB("SELECT * FROM CatalogTable"))

    ''' Заполним таблицу IpDict'''

    # print (len(uniqIp))

    result = callDB("SELECT * FROM IpDict")
    if result == []:
        i = 0
        print("Заполним таблицу IpDict")
        for ip in uniqIp:
            country_code = CountryByIP(ip)
            result = callDB("INSERT INTO IpDict (IP,Country_code) VALUES ('{0}','{1}')".format(ip, country_code))
            # print (country_code)
            if i % 10 == 0:
                print("Обработано %d из таблицы IpDict" % i)
            i += 1
        print(callDB("SELECT * FROM IpDict LIMIT 3"))

    ''' Разобьем  распарсенный список на 2 части (серфинг и скрипты)'''

    surfList = []
    scriptList = []

    for line in listOfLines:
        if line[3] not in keyWords:
            surfList.append(line)
        else:
            scriptList.append(line)

    # print (len(surfList))
    # print (len(scriptList))

    ''' Заполним SurfingTable'''

    # callDB("DELETE FROM SurfingTable")
    result = callDB("SELECT * FROM SurfingTable")

    if result == []:
        print("Заполним таблицу SurfingTable")
        i = 0

        for surfLine in surfList:
            keyCatalog = callDB(
                "SELECT rowid FROM CatalogTable WHERE Category = '%s' AND Goods = '%s'" % (surfLine[4], surfLine[5]))[
                0][0]
            keyIP = callDB("SELECT rowid FROM IpDict WHERE IP = '%s'" % (surfLine[2]))[0][0]
            result = callDB("INSERT INTO SurfingTable (Dat,Tim,IP,CatCode) VALUES ('%s','%s','%d','%d')" % (
                surfLine[0], surfLine[1], keyIP, keyCatalog))
            i += 1
            if i % 100 == 0:
                print("Обработано %d из таблицы SurfingTable" % i)
        print(callDB("SELECT * FROM SurfingTable LIMIT 3"))

    ''' Заполним таблицу ScriptTable'''
    # result = callDB("DELETE FROM ScriptTable")

    result = callDB("SELECT * FROM ScriptTable")

    if result == []:
        i = 0
        print("Заполним таблицу ScriptTable")
        for scriptLine in scriptList:
            keyScriptCode = callDB("SELECT rowid FROM ScriptCodeTable WHERE ScriptName = '%s'" % (scriptLine[3]))[0][0]
            keyIP = callDB("SELECT rowid FROM IpDict WHERE IP = '%s'" % (scriptLine[2]))[0][0]
            # print(scriptLine)
            result = callDB(
                "INSERT INTO ScriptTable (Dat,Tim,IP,ScriptID, IDCart, CatCode) VALUES ('%s','%s','%d','%d', '%d' ,'%s')" % (
                    scriptLine[0], scriptLine[1], keyIP, keyScriptCode, int(scriptLine[4]), scriptLine[5]))
            i += 1
            if i % 100 == 0:
                print("Обработано %d из таблицы ScriptTable" % i)
        print(callDB("SELECT * FROM ScriptTable LIMIT 3"))




''' Вопрос 1: Найдем сколько запросов из какой страны больше всего приходит'''
def Question1():

    print ("Вопрос1")

    ''' Сформируем сначала список запросов по всем адресам'''
    CountryCodeDict = {}
    IPs = []
    for table in ["SurfingTable","ScriptTable"]:
        ip = callDB("SELECT IP FROM {0}".format(table))
        IPs.extend(ip)

    #print (len(IPs))

    i = 0

    ''' Создадим словарь по частоте запросов с каждой страны'''
    for ip in IPs:
        #print (ip)
        #print (ip[0])
        if i % 1000 == 0:
            print ("Обработано {0} записей".format(i))
        i+=1
        country = callDB("SELECT Country_code from IpDict WHERE rowid = {0}".format(ip[0]))[0][0]
        if CountryCodeDict.get(country) is None:
            CountryCodeDict[country] = 1
        else:
            CountryCodeDict[country] += 1

    print ("Вопрос 1: Запросы из стран в порядке убывания {0} ".format(sorted(CountryCodeDict, key =CountryCodeDict.get,
                                                                             reverse = True)))




''' Вопрос 2: Посетители из какой страны чаще всего интересуются товарами из категории fresh_fish'''
def Question2():


    # Найдем коды категории freshfish

    freshFishCodes = callDB("SELECT rowid FROM CatalogTable WHERE Category = '{}'".format('fresh_fish'))
    #print(len(freshFishCodes))
    freshFishIPs = []
    for code in freshFishCodes:
        freshFishIPs.extend(callDB("SELECT IP FROM SurfingTable WHERE CatCode = {}".format(code[0])))
    CountryCodeDict = {}

    for ipCode in freshFishIPs:
        # print (ipCode[0])
        # print (callDB("SELECT Country_code FROM IpDict WHERE rowid = {}".format(ipCode[0])))
        country = callDB("SELECT Country_code FROM IpDict WHERE rowid = {}".format(ipCode[0]))[0][0]
        if CountryCodeDict.get(country) is None:
            CountryCodeDict[country] = 1
        else:
            CountryCodeDict[country] += 1
    # print (sorted(CountryCodeDict, key =CountryCodeDict.get, reverse = True))
    print("Вопрос 2: Список запросов по категории fresh_fish: {0}".format(
        sorted(CountryCodeDict, key=CountryCodeDict.get, reverse=True)))


''' Вопрос3: В какое время суток чаще всего просматривают категорию frozen_fish  '''

def Question3():
    result = callDB("SELECT rowid FROM CatalogTable WHERE Category = '{}'".format('frozen_fish'))
    frozenFishCodes = [i[0] for i in result]
    rangeHours = [('Ночь', '00', '05'), ('Утро', '06', '11'), ('День', '12', '17'), ('Вечер', '18', '23')]

    viewHours = {}

    #print (frozenFishCodes)

    string = []

    for i in frozenFishCodes:
        string.append("'{}'".format(i))
    requestString = ','.join(string)

    #print (callDB("SELECT Dat, Tim FROM SurfingTable WHERE strftime('%H', Tim) BETWEEN 06 AND 11".format(requestString)))


    for hours in rangeHours:
        viewHours[hours[0]] = len(callDB("SELECT * FROM SurfingTable WHERE CatCode IN ({0}) AND strftime('%H', Tim) BETWEEN '{1}' AND '{2}'".
            format(requestString, hours[1], hours[2])))




    print("Вопрос 3: Больше всего просмотров категории frozen_fish {0}".format(sorted(viewHours, key=viewHours.get, reverse=True)[0]))


''' Вопрос 4: Какое максимальное число завпросов на сайт за астрономический час'''

def Question4():
    datesSurfing = callDB("SELECT Date(Dat) FROM SurfingTable")
    allDates = []
    for dates in datesSurfing:
        for date in dates:
            allDates.append(date)

    datesScripts = callDB("SELECT Date(Dat) FROM ScriptTable")
    for dates in datesScripts:
        for date in dates:
            allDates.append(date)
    allDates.sort()

    allDates = list(set(allDates))
    allDates.sort()

    dateStart = datetime.strptime(allDates[0], '%Y-%m-%d')
    dateEnd = datetime.strptime(allDates[len(allDates) - 1], '%Y-%m-%d')

    requestDict = {}

    for date in allDates:
        for hour in range(0, 23):
            currentDate = datetime.strptime(date, "%Y-%m-%d")
            #print (currentDate.day)
            requests = len(callDB(
                "SELECT * FROM SurfingTable WHERE strftime('%H', Tim) = '{0:02d}' AND strftime('%d', Dat) = '{1:02d}'"
                    .format(hour, currentDate.day)))
            requests += len(callDB(
                "SELECT * FROM ScriptTable WHERE strftime('%H', Tim) = '{0:02d}' AND strftime('%d', Dat) = '{1:02d}'".format(
                    hour, currentDate.day)))
            if requestDict.get(hour) is None:
                requestDict[hour] = requests
            elif requestDict[hour] < requests:
                requestDict[hour] = requests

    print("Вопрос 4: Больше всего запросов в {0} час  в количестве {1}".format(
        sorted(requestDict, key=requestDict.get, reverse=True)[0],
        requestDict[sorted(requestDict, key=requestDict.get, reverse=True)[0]]))



''' Вопрос 5: Товары из какой категории чаще всего покупают совместно с товаром из категории semi_manufactures'''

def Question5():


    result = callDB("SELECT rowid FROM CatalogTable WHERE Category = 'semi_manufactures' AND Goods != ''")
    semi_manufacturesCode = [i[0] for i in result]

    result = callDB("SELECT rowid, Category FROM CatalogTable WHERE Category NOT IN ('semi_manufactures','')")
    #print(result)

    '''Создадим словарь соответствия кодов товаров и названий категорий'''

    dictCategories = {}
    for line in result:
        if dictCategories.get(line[1]) is None:
            dictCategories[line[1]] = [line[0]]
        else:
            tempList = dictCategories[line[1]]
            tempList.append(line[0])
            dictCategories[line[1]] = tempList

    # print (dictCategories)

    ''' Найдем коды соответствующие действиям положить в корзину и оплатить ее'''

    result = callDB("SELECT rowid FROM ScriptCodeTable WHERE ScriptName IN ('cart','pay')")
    scriptCodes = [i[0] for i in result]
   # print(scriptCodes)

    '''Запрос в таблицу ScriptTable где найдем записи с кодами положить в корзину и оплатить ее, с сортировкой по IP адресу и времени'''

    result = callDB(
        "SELECT IP, ScriptID, IDCart, CatCode FROM ScriptTable WHERE ScriptID IN ('{0}','{1}') ORDER BY IP,Dat,Tim".format(
            scriptCodes[0], scriptCodes[1]))

    tempList = []
    resultList = []
    #print (result)


    ''' Составляем кортежи товаров положенных в корзину в одно время и затем оплаченных'''

    i = 0
    for line in result:
        if line[1] == scriptCodes[0] and result[i + 1][2] == line[2]:
            tempList.append(line[3])
        elif line[1] == scriptCodes[1]:
            if tempList != []:
                resultList.append(tempList)
            tempList = []
        elif result[i+1][2] != line[2]:
            tempList = []

        i += 1
    #print (resultList)

    ''' Формируем результирующий словарь частоты совместных покупок'''

    resultDict = {}

    for line in resultList:
        for goods in semi_manufacturesCode:
            if goods in line:
                for buys in line:
                    if goods != buys:
                        for key in dictCategories.keys():
                            if buys in dictCategories[key]:
                                if resultDict.get(key) is None:
                                    resultDict[key] = 1
                                else:
                                    resultDict[key] += 1
    #print (resultDict)

    print("Вопрос 5: Больше всего покупали товаров покупали из категории {0}  вместе с товарами из категории semi_manufactures".format(
        sorted(resultDict, key=resultDict.get, reverse=True)[0]))

''' Вопрос 6: Сколько брошенных (не оплаченных) корзин имеется?'''

def Question6():


    result = callDB("SELECT rowid FROM ScriptCodeTable WHERE ScriptName IN ('cart','success_pay')")
    keyScriptCodeList = [i[0] for i in result]
    # print (keyScriptCodeList)

    result = callDB("SELECT ScriptID, IDCart FROM ScriptTable WHERE ScriptID IN ('{0}','{1}') ORDER BY IDCart".
                    format(keyScriptCodeList[0], keyScriptCodeList[1]))

    abandonedCarts = 0
    cartID = None
    # print (result)
    for line in result:
        if line[0] == keyScriptCodeList[0] and cartID != line[1] or cartID is None:
            cartID = line[1]
            abandonedCarts += 1
        elif line[0] == keyScriptCodeList[1]:
            abandonedCarts -= 1

    print("Вопрос 6: Количество брошенных корзин = {0}".format(abandonedCarts))


''' Вопрос 7: Какое количество пользователей совершали повторные покупки?'''

def Question7 ():


    result = callDB("SELECT rowid FROM ScriptCodeTable WHERE ScriptName IN ('success_pay')")
    keyScriptCodeList = [i[0] for i in result]

    result = callDB("SELECT IP,ScriptID, IDCart FROM ScriptTable WHERE ScriptID IN ({0}) ORDER BY IP".
                    format(keyScriptCodeList[0]))

    # print(result)

    repurchaseDict = {}

    for line in result:
        if repurchaseDict.get(line[0]) is None:
            repurchaseDict[line[0]] = 1
        else:
            repurchaseDict[line[0]] += 1

    repurchaseCount = 0
    # print (repurchaseDict)

    for key in repurchaseDict.keys():
        if repurchaseDict[key] >= 2:
            repurchaseCount += 1

    print("Вопрос 7: Количество повторных покупок = {0}".format(repurchaseCount))


####### Открываем файл############
with open("logs.txt") as file:
    for line in file:

        
####### Построчно разбираем на регулярное выражение: дата, время, IP - адрес, адрес просмотра 2 уровня (или скриптовое ключевое слово), 3 уровень либо сам запрос
        
        a = re.findall(r'\| (\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}) \[\w+\] \w+: (\d{0,3}.\d{0,3}.\d{0,3}.\d{0,3}) \w+://all_to_the_bottom.com/(\w*)[?/]?([\w+\d+=&]+)?', line)
        a= list(a[0])
        #print (a)
 
######### Парсим успешную оплату, разбивая на 2 части и пишем в 3 и 4 столбцы списка
        if (re.match(r'success_pay',a[3])):
            result = re.findall(r'(success_pay)_(\d+)', a[3])
            result = list(result[0])
            a[3]=result[0]
            a[4]=result[1]
            a.append('')
            #print (a)
            
            
########### Парсим скриптовую часть строки с целью выделить id корзины
        elif a[3] == 'cart':
            result = re.findall(r'goods_id=(\d+)&amount=\d+&cart_id=(\d+)',a[4])
            result = list(result[0])
           # print (a)
          #  print (result)
            a[4] = result[1]
            a.append(result[0])

        elif a[3] == 'pay':
            result = re.findall(r'user_id=(\d+)&cart_id=(\d+)',a[4])
            result = list(result[0])
            a[4] = result[1]
            a.append(result[0])
        else:
            a.insert(3,'surfing')
       
        
        
        listOfLines.append(a)


'''Сформируем список категорий сайта и товаров
   category - категории товара
   goods - товары на сайте
   uniqIp - IP адреса пользователей зашедших на сайт'''
#for line in listOfLines:
 #   print (line)
codeRequests = []
#goods = []
category = []
uniqIp = []
i=0
Goods = []
for line in listOfLines:
    if line [2] not in uniqIp:
        uniqIp.append(line[2])
    if line[3] == 'cart':
        for j in range(i-1,0,-1):
            if listOfLines[j][2] == line[2] and listOfLines[j][3] == 'surfing':
                Goods.append((int (line[5]),listOfLines[j][4],listOfLines[j][5]))
                break

    if line [3] == 'surfing':
        category.append(line[4])
        #goods.append(line[5])

    i+=1

category.append("")
category = list(set(category))
#goods = list(set(goods))
uniqIp = list(set(uniqIp))

''' Оставим только уникальные кортежи товаров (ID, категория, название)'''

uniqGoods = []

#print (Goods)

for line in Goods:
    if line not in uniqGoods:
        uniqGoods.append(line)
#print(category)

uniqGoods.sort()

j = 0  #Счетчик для движения по категории
for i in range(len(uniqGoods),len(uniqGoods)+len(category),1):
    uniqGoods.append((i+1,category[j],""))
    j+=1

''' Наполним базу данных'''
CreateAndFillDB()

'''Вопрос 1 '''

Question1()

''' Вопрос 2'''

Question2()

''' Вопрос 3'''

Question3()

''' Вопрос 4'''

Question4()

''' Вопрос 5: Товары из какой категории чаще всего покупают совместно с товаром из категории semi_manufactures'''

Question5()

''' Вопрос 6: Сколько брошенных (не оплаченных) корзин имеется?'''

Question6()

''' Вопрос 7: Какое количество пользователей совершали повторные покупки?'''

Question7()




