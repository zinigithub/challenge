# -*- coding: utf-8 -*-
import argparse
import csv
import sys
import json
from datetime import datetime
import smtplib
from email.message import EmailMessage
import sqlite3
#Procesa lo que es JSON y CSV
#Argumentos necesarios : archivoJSON,archivoCSV ,  sys.argv
#Datos del JSON : nombre_db,email_owner,clasificacion_db
#Array de Datos : 
"""
[{"nombre_db":"clientes","email_owner":"juan@dba.com","clasificacion_db":"alta"},{"nombre_db":"usuarios","email_owner":"juan@dba.com","clasificacion_db":"media"},{"nombre_db":"imagenes","email_owner":"roberto@dba.com","clasificacion_db":"baja"}]
"""


#Datos del CSV: email_manager

#Tabla de la db

"""
CREATE TABLE IF NOT EXISTS revalidas (
    id integer PRIMARY KEY,
    db_name text ,
    email_owner text,
    clasificacion_db text
);
CREATE TABLE IF NOT EXISTS managers (
    email_manager NOT NULL UNIQUE
);

"""

if __name__ == '__main__':
    
    #Inserta los valores del JSON las revalidas
    def insertar_revalidas(conn, valores):
        sql = ''' INSERT INTO revalidas(db_name,email_owner,clasificacion_db)
                  VALUES(?,?,?) '''
        cur = conn.cursor()
        cur.execute(sql, valores)
        conn.commit()
    
    #Inserta los managers que vienen en el CSV
    def insertar_managers(conn, valores):
        try:
            sql = ''' INSERT INTO managers(email_manager)
                      VALUES(?) '''
            cur = conn.cursor()
            cur.execute(sql, valores)
            conn.commit()
        except Exception as e:
            pass

    #Scripts para crear una tabla.
    def create_table(conn, create_table_sql):
        """ create a table from the create_table_sql statement
        :param conn: Connection object
        :param create_table_sql: a CREATE TABLE statement
        :return:
        """
        try:
            c = conn.cursor()
            c.execute(create_table_sql)
        except Exception as e:
            print(e)

    #Crea la conexión a la base de datos.
    def create_connection(db_file):
        """ create a database connection to a SQLite database """
        conn = None
        try:
            conn = sqlite3.connect(db_file)
            print(sqlite3.version)
        except Exception as e:
            print(e)
        return conn

    #Script que permite enviar correos electronicos, se debe instalar sendmail
    def enviarmail(correo,contenido):
        # Import the email modules we'll need

        msg = EmailMessage()
        msg.set_content("\n".join(contenido))

        # me == the sender's email address
        # you == the recipient's email address
        msg['Subject'] ="contenido"
        msg['From'] = "alerta@localhost"
        msg['To'] = correo

        # Send the message via our own SMTP server.
        s = smtplib.SMTP('localhost')
        s.send_message(msg)
        s.quit()

    """#Script que permite leer los archivos, guardarlos en una base de datos y enviar las
    Los nombres de las bases de datos de clasificacion alta a los managers del archivo.
    """
    def procesar_achivos(archivocsv,archivojson):
        #Iniciamos la conexión a la base de datos, se crea si no existe
        conn = create_connection("database.db")
        #Creamos la tabla de la base de datos, se crea si no existe.
        create_table(conn,"""CREATE TABLE IF NOT EXISTS revalidas (id integer PRIMARY KEY,
                db_name text ,
                email_owner text,
                clasificacion_db text
            );""")
        #Creamos la tabla con los usuarios managers
        create_table(conn,"""CREATE TABLE IF NOT EXISTS managers (
            email_manager text UNIQUE
            );""")
        
        #Lectura del JSON
        #Variable donde se almacenaran las BD altas para ser enviadas al manager
        revalidas_alta = ()
        with open(archivojson, 'r') as f:
            datos_json = json.load(f)
        for rowjson in datos_json:
            revalidas = (rowjson['nombre_db'], rowjson['email_owner'],rowjson['clasificacion_db'])
            
            insertar_revalidas(conn,revalidas)
            if rowjson['clasificacion_db'].lower() == "alta":
                revalidas_alta=revalidas_alta+("\nNombre DB : "+rowjson['nombre_db'],"\n Email Owner:"+ rowjson['email_owner'],"\n Criticidad Base de datos "+rowjson['clasificacion_db'])
            # Debug .. print(rowjson['email_owner'])
        #Lectura del archivo CSV
        #print((revalidas_alta))
        #Variable donde se almacenaran los managers
        managers = ()
        with open(archivocsv, mode='r') as csv_file:
            datos_csv = csv.DictReader(csv_file)
            line_count = 0
            for row in datos_csv:
                if line_count == 0:
                    print(f'Column names are {", ".join(row)}')
                    line_count += 1
                print('DEBUG : \t'+row["user_manager"])
                managers = (row['user_manager'],)
                #print (managers)
                insertar_managers(conn,managers)
                if len(revalidas_alta) >0:
                    enviarmail(row['user_manager'],revalidas_alta)
                line_count += 1
            print(f'Processed {line_count} lines.')
        #Debug print(len(row))

    parser = argparse.ArgumentParser(description="Buscador de Patrones.")
    parser.add_argument('-archivocsv', help="Archivo CSV a procesar", default=None)
    parser.add_argument('-archivojson', help="Archivo JSON a procesar", default=None)

    args = parser.parse_args()
    if args.archivocsv and args.archivojson:
        procesar_achivos(args.archivocsv,args.archivojson)
    else:
        sys.exit("Debe agregar los archivos con el parametro -archivojson y -archivocsv")