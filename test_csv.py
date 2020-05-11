#!/usr/bin/python3
# -*- coding: utf-8 -*-

'''
Recoge el último dato guardado de un archivo csv y lo guarda en una BBDD
'''

# --------------------------------------------------------------------------- #
# configure BBDD
# instalar como administrador: pip install mysql-connector-python (https://pypi.org/project/mysql-connector-python/)
# conector: https://dev.mysql.com/downloads/connector/python/8.0.html
# documentación: https://dev.mysql.com/doc/connector-python/en/
# --------------------------------------------------------------------------- #
import mysql.connector as my_dbapi

import csv
import time
import datetime

f = open("C:\tablas\dat_pm.txt", "r")
f2 = open("C:\tablas\dat_pm2.txt", "r")
lol = csv.reader(f, delimiter='\t') #list of lines
lol2 = csv.reader(f2, delimiter='\t') #list of lines

#solo tomo la última linea del fichero
*_, last = lol # for a better understanding check PEP 448
*_, last2 = lol2 # for a better understanding check PEP 448

print(last)
print(last2)

last[1] = datetime.datetime.strptime(last[1], '%d/%m/%Y').strftime('%Y-%m-%d') #Adapto formato de fecha a MySQL

#campos en el csv
campos = ["huellatiempo","fecha","hora","energia_activa","energia_reactiva","potencia_activa","potencia_reactiva","factor_potencia",\
         "frecuencia","tension_12","tension_23","tension_31","intensidad_1","intensidad_2","intensidad_3","tension_ln","intensidad_ln"]

datos = {}
i = 0

#elimino espacios y cambio , por . en los decimales
for dato in last:
    dato = dato.strip()
    dato = dato.replace(",",".")
    datos[campos[i]] = dato
    i = i + 1

j = 0
for dato in last2:  
    if j < 3:   #los 3 primeros datos son iguales
        j = j +1
        continue;
    dato = dato.strip()
    dato = dato.replace(",",".")
    datos[campos[i]] = dato
    i = i + 1

#campos en cada fichero
#datos: timestamp - 10 años, fecha, hora, Energia Activa KWh, Energia Reactiva KVARh, Potencia Activa kW, Potencia Reactiva kVAR, Factor de Potencia FP, frecuencia Hz
#datos2: timestamp - 10 años, fecha, hora, Tension 1-2 V, Tensión 2-3 V, Tensión 3-1 V, Intensidad 1 A, intensidad 2 A, Intensidad 3 A, Tensión L-N V, Intensidad L-N A

#conexión a BBDD
cnx_my = my_dbapi.connect(user='usuario', password='contraseña', host='db_server', database='pm')
cursor_my = cnx_my.cursor()

query_my = "INSERT INTO dat_pm (" + ",".join(campos) + ") VALUES ('" + "','".join([datos[a] for a in campos]) + "')"

#INSERT INTO `pm`.`dat_pm` (`huellatiempo`, `fecha`, `hora`, `energia_activa`, `energia_reactiva`, `potencia_activa`, `potencia_reactiva`, `factor_potencia`, `frecuencia`, `tension_12`, `tension_23`, `tension_31`, `intensidad_1`, `intensidad_2`, `intensidad_3`, `tension_ln`, `intensidad_ln`) VALUES (NULL, CURRENT_TIMESTAMP, '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '');

cursor_my.execute(query_my)
cnx_my.commit()
cnx_my.close()