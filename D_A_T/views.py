import os
import dask
import mathfilters
import D_A_T.API_FUNCS.APIMain as api_automation
import pytz
from numpy import ma
import D_A_T.UDI.read_smx_sheet.populate_from_csv as pop
from django.core.mail import send_mail
import mysql
from django.utils.timezone import now
import teradatasql
from django.contrib import messages
from django.http import HttpResponse
from django.shortcuts import render
from sqlalchemy import create_engine
import datetime
import numpy as np
# from sqlalchemy.dialects import mysql
import  mysql.connector
import D_A_T.DQ_checks_runner as dq
import D_A_T.db_connections as db
import pandas as pd
import D_A_T.tables_checks as tables
import D_A_T.table_checks as table
import D_A_T.column_checks as column
import csv
import getpass
from django.contrib.auth.models import User , auth
import psycopg2
import D_A_T.models
import json
from django.conf import settings
import D_A_T.batching as batcher
# Create your views here.
import D_A_T.sources_session as ss
import D_A_T.test_cases_session as tcs
import D_A_T.data_quality_sessions as dqs
import D_A_T.stats_sessions as sts
import threading
import D_A_T.UDI.read_smx_sheet.UDi_funcs as udi_retrieve
import D_A_T.UDI.read_smx_sheet.UDI_class as udi_class
import D_A_T.UDI.read_smx_sheet.generate_scripts as gs


dbHost = settings.DATABASES['default']['HOST']
dbUsername = settings.DATABASES['default']['USER']
dbPassword = settings.DATABASES['default']['PASSWORD']
dbName = settings.DATABASES['default']['NAME']



#loading login page Function
def login(request):
    return render(request, 'login.html')

#logout function
def logout(request):
    auth.logout(request)
    return login(request)


#login controller checking if the user is authenticated to login or not then calling index function
def login_controller(request):
    if request.method == 'POST':
        username = request.POST["name"]
        password = request.POST["password"]

        user = auth.authenticate(username=username,password=password)

        if user is not None:

            auth.login(request,user)

            return index(request)
        else:
            # messages.info(request,"Invalid username or password")
            # return login(request)
            return HttpResponse(' <head>  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css"><script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script><script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/js/bootstrap.min.js"></script><meta http-equiv = "refresh" content = "3; url = "index.html" " /></head><div class="alert alert-danger">Wrong username or password</div>')


    else:
        return login(request)
    return HttpResponse("User Doesn't exist")


# Loading Dashboard view and passing all stats and charts        data to index.html
def index(request):
    context = {}
    select_query = 'SELECT database_type, ip_address, username, password, id, connection_name FROM public."D_A_T_user_connection" WHERE user_id_id = '+ str(request.user.id) +';'
    connection = psycopg2.connect(user=dbUsername,
                                  password=dbPassword,
                                  host=dbHost,
                                  database=dbName)
    cursor = connection.cursor()
    cursor.execute(select_query)
    connection.commit()
    result = cursor.fetchall()
    context['result'] = result
    if len(result) == 0:
        context['data'] = "-1"
    else:
        context['data'] = "1"
    for i in range (0,len(context['result'])):
        context['result'][i] = list(context['result'][i])
        if context['result'][i][0] == 1:
            context['result'][i].append("Mysql")
        elif context['result'][i][0] == 2:
            context['result'][i].append("Teradata")
        elif context['result'][i][0] == 5:
            context['result'][i].append("Postgres")
        elif context['result'][i][0] == 4:
            context['result'][i].append(" ") #TODO
        else:
            context['result'][i].append(" ") #TODO
    note_select_query = 'SELECT note,id FROM public."D_A_T_user_note" WHERE user_id_id = ' + str(request.user.id) + ';'
    cursor.execute(note_select_query)
    connection.commit()
    note_result = cursor.fetchall()

    notes = []
    for item in note_result:
        notes.append(item)
    context['notes'] = notes





    saved_conns_query = 'SELECT COUNT(*)  FROM public."D_A_T_user_connection" WHERE user_id_id = '+ str(request.user.id) +';'
    cursor.execute(saved_conns_query)
    connection.commit()
    saved_connections = cursor.fetchall()

    mysql_query = 'SELECT COUNT(*)  FROM public."D_A_T_user_connection" WHERE user_id_id = ' + str(request.user.id) + ' and database_type = 1;'
    cursor.execute(mysql_query)
    connection.commit()
    saved_connections_mysql = cursor.fetchall()

    td_query = 'SELECT COUNT(*)  FROM public."D_A_T_user_connection" WHERE user_id_id = ' + str(request.user.id) + ' and database_type = 2;'
    cursor.execute(td_query)
    connection.commit()
    td = cursor.fetchall()

    oracle_query = 'SELECT COUNT(*)  FROM public."D_A_T_user_connection" WHERE user_id_id = ' + str(
        request.user.id) + ' and database_type = 3;'
    cursor.execute(oracle_query)
    connection.commit()
    oracle = cursor.fetchall()

    sql_server_query = 'SELECT COUNT(*)  FROM public."D_A_T_user_connection" WHERE user_id_id = ' + str(
        request.user.id) + ' and database_type = 4;'
    cursor.execute(sql_server_query)
    connection.commit()
    sql_server = cursor.fetchall()

    postgres_query = 'SELECT COUNT(*)  FROM public."D_A_T_user_connection" WHERE user_id_id = ' + str(
        request.user.id) + ' and database_type = 5;'
    cursor.execute(postgres_query)
    connection.commit()
    postgres = cursor.fetchall()

    context["saved_connections"] = saved_connections[0]
    context["mysql_count"] = saved_connections_mysql[0]
    context["td"] = td[0]
    context["oracle"] = oracle[0]
    context["sql_server"] = sql_server[0]
    context["postgres"] = postgres[0]
    try:
        chart_data_file_path = ss.chart_data_path(request)
        size = os.stat(chart_data_file_path).st_size
        if os.stat(chart_data_file_path).st_size != 0:
            with open(chart_data_file_path) as json_file:
                result = json.load(json_file)
                data_length = []
                for i in range(0, len(result['table'])):
                    data_length.append(i)
            context["dict"]  =  result
            context["data_length"] =  data_length
        context["size"]=size
    except:
        size =0
        context["size"] = size

    try:
        last_dq_file_path = ss.dq_charts_last_file_path(request)
        dq_size=os.stat(last_dq_file_path).st_size
        if os.stat(last_dq_file_path).st_size != 0:
            source_path_file = ss.get_file_path(request,'S')
            source_name = open(source_path_file, 'r').read()
            source_len = len(source_name)
            context["source_name"] = source_name
            context["source_len"] = source_len
            json_file_ = open(last_dq_file_path,'r')
            result_dq = json.load(json_file_)
            data_length_dq = []
            for i in range(len(result_dq['category'])):
                if i % 2 == 0:
                    data_length_dq.append(i)

            if len(data_length_dq) % 2 == 1:
                data_length_dq.append(-1)
            context["dict_dq"]  =  result_dq
            context["data_length_dq"] =  data_length_dq
        context["dq_size"]=dq_size
    except:
        dq_size =0
        context["dq_size"] = dq_size

    connection_id_query = 'SELECT id FROM public."D_A_T_user_connection" WHERE user_id_id = ' +str(request.user.id)+ ';'
    cursor.execute(connection_id_query)
    connection.commit()
    result = cursor.fetchall()
    connection_ids = []
    for con in result:
        connection_ids.append(con[0])

    if len(connection_ids) == 0: #no connection
        connection_ids = [-1]

    saved_sources_query = 'SELECT A.id, source, connection_name,database, tables, database_type, username, password, ip_address FROM public."D_A_T_user_source" A JOIN ' \
                          'public."D_A_T_user_connection" B ON A.connection_id_id = B.id  WHERE connection_id_id in ' + str(connection_ids).replace('[','(').replace(']',')')+ ';'
    cursor.execute(saved_sources_query)
    connection.commit()
    saved_sources = cursor.fetchall()
    if len(saved_sources) == 0:
        context['is_source'] = '-1'
    else:
        context['is_source'] = '1'
    context['saved_sources'] = saved_sources
    connection.close()
    # return HttpResponse(data_length_dq)
    return render(request , 'index.html', context)


# connecting to a certain database and selecting all the dbs of a selected  connection
def connection_action(request):
    action = request.POST['action']
    data = str(request.POST['data']).split(',')
    context = {}
    input_data = {}
    db_list = []
    db_type = data[0]
    ip = data[1]
    username = data[2]
    password = data[3]
    col_pk = data[4]
    connection_name = data[5]

    if action == "Connect":
        input_data['db_type'] = db_type
        input_data['ip'] = ip
        input_data['username'] = username
        input_data['password'] = password

        connection_file_path = ss.get_file_path(request,'C')

        file2 = open(connection_file_path, "w")
        file2.write(connection_name)
        file2.close()
        user_input_file_path = ss.user_input_path(request)
        file1 = open(user_input_file_path, "w")
        for key in input_data:
            file1.write(str(key) + " : " + str(input_data[key]) + "\n")
        connect = ""

        if db_type == "1":
            try:
                connect = db.myssql_connection(ip, username, password)
                query = pd.read_sql('SELECT schema_name FROM information_schema.schemata', connect)
                for x in range(0, len(query)):
                    db_list.append(query.loc[x, 'schema_name'])
            except:
                return HttpResponse(' <head><link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css"><script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script><script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/js/bootstrap.min.js"></script><meta http-equiv = "refresh" content = "3; url =   home  " /></head><div class="alert alert-danger">Mysql Is not responding, Something went wrong with your connection</div>')

        elif db_type == "2":

            try:
                with teradatasql.connect(host=ip, user=username, password=password , encryptdata=True) as connect:
                    query = pd.read_sql('select DatabaseName from dbc.Databases', connect)
                    for x in range(0, len(query)):
                        db_list.append(query.loc[x, 'DatabaseName'])
            except:
                return HttpResponse(' <head><link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css"><script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script><script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/js/bootstrap.min.js"></script><meta http-equiv = "refresh" content = "3; url =   home  " /></head><div class="alert alert-danger">Teradata Is not responding, Something went wrong with your connection</div>')


        elif db_type == "5":
            try:
                connect = db.postgres_connection(ip, username, password)
                query = pd.read_sql('SELECT schema_name FROM information_schema.schemata', connect)
                for x in range(0, len(query)):
                    db_list.append(query.loc[x, 'schema_name'])
            except:
                return HttpResponse(' <head><link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css"><script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script><script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/js/bootstrap.min.js"></script><meta http-equiv = "refresh" content = "3; url =   home  " /></head><div class="alert alert-danger">PostgreSQL Is not responding, Something went wrong with your connection</div>')


        context['dbs'] = db_list
        context['list_length'] = range(len(db_list))
        context['db_type'] = db_type

        return render(request, 'Databases.html', context)
    elif action == "Delete":
        connection = psycopg2.connect(user=dbUsername,
                                          password=dbPassword,
                                          host=dbHost,
                                          database=dbName)

        cursor = connection.cursor()
        delete_sources_query = 'DELETE FROM public."D_A_T_user_source" WHERE connection_id_id = '+ col_pk
        cursor.execute(delete_sources_query)
        connection.commit()

        delete_connection_query = 'DELETE FROM public."D_A_T_user_connection" WHERE id = ' + col_pk
        cursor.execute(delete_connection_query)
        connection.commit()
        connection.close()
        return index(request)



# loading connection lists 
def db_connection(request):
    connection = psycopg2.connect(user=dbUsername,
                                  password=dbPassword,
                                  host=dbHost,
                                  database=dbName)

    cursor = connection.cursor()
    connection_names_query = 'SELECT  connection_name FROM public."D_A_T_user_connection" WHERE user_id_id = '+ str(request.user.id) +' ;'
    cursor.execute(connection_names_query)
    result = cursor.fetchall()
    connection_names = json.dumps(result)
    return  render(request , 'db_connection.html', {"connection_names": connection_names})


# List the dbs of the selected connection type
def db_connection_controller(request):
    context = {}
    input_data = {}
    db_list = []
    db_type = request.POST['db_type']
    ip = request.POST['ip']
    username = request.POST['username']
    password = request.POST['password']
    connection_name = request.POST['connectionName']
    try:
        is_save = request.POST['save']
    except:
        is_save = 0
    success = False

    input_data['db_type'] = db_type
    input_data['ip'] = ip
    input_data['username'] = username
    input_data['password'] = password
    connection_file_path = ss.get_file_path(request, 'C')

    file1 = open(connection_file_path, "w")
    file1.write(connection_name)

    user_input_file_path = ss.user_input_path(request)
    file1 = open(user_input_file_path, "w")
    for key in input_data:
        file1.write(str(key) + " : " + str(input_data[key]) + "\n")
    connect = ""

    if db_type == "1":
        try:
            connect = db.myssql_connection(ip,username,password)
            query = pd.read_sql('SELECT schema_name FROM information_schema.schemata', connect)
            for x in range(0,len(query)):
                db_list.append(query.loc[x,'schema_name'])
            success = True
        except Exception as e:
            return HttpResponse(' <head><link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css"><script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script><script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/js/bootstrap.min.js"></script><meta http-equiv = "refresh" content = "3; url =   db_connection  " /></head><div class="alert alert-danger">Mysql Is not responding, Something went wrong with your connection </div> <div class="alert alert-info"> Exception is : '+str(e)+'</div>')

    elif db_type == "2":
        try:

            with teradatasql.connect(host=ip, user=username, password=password, encryptdata=True) as connect:
                query = pd.read_sql('select DatabaseName from dbc.Databases', connect)
                for x in range(0, len(query)):
                    db_list.append(query.loc[x, 'DatabaseName'])
                success = True
        except:
            return HttpResponse(' <head><link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css"><script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script><script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/js/bootstrap.min.js"></script><meta http-equiv = "refresh" content = "3; url =   db_connection  " /></head><div class="alert alert-danger">Teradata Is not responding, Something went wrong with your connection</div>')

    # elif db_type == "4":
    #     #hthth

    elif db_type == "5":
        # try:
        connect = db.postgres_connection(ip,username,password)
        query = pd.read_sql('SELECT schema_name FROM information_schema.schemata', connect)
        for x in range(0, len(query)):
            db_list.append(query.loc[x, 'schema_name'])
        success = True
        # except:
        #     return HttpResponse(' <head><link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css"><script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script><script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/js/bootstrap.min.js"></script><meta http-equiv = "refresh" content = "3; url =   db_connection  " /></head><div class="alert alert-danger">PostgreSQL Is not responding, Something went wrong with your connection</div>')

    context['dbs'] = db_list
    context['list_length'] = range(len(db_list))
    context['db_type'] = db_type

    try:

        if is_save =="1" and success:
            connection = psycopg2.connect(user=dbUsername,
                                          password=dbPassword,
                                          host=dbHost,
                                          database=dbName)
            cursor = connection.cursor()
            select_query = 'SELECT * FROM public."D_A_T_user_connection" WHERE user_id_id = ' + str(
                request.user.id) + " AND username ='" + username + "' AND ip_address = '" + ip + "' AND database_type = " + db_type

            cursor.execute(select_query)
            connection.commit()
            result = cursor.fetchall()
            if len(result) == 0:
                postgres_insert_query = 'INSERT INTO public."D_A_T_user_connection"(user_id_id,database_type,username,password,ip_address,connection_name) VALUES (' + str(request.user.id) + "," + db_type + ",'" + username +"','"+ password+"','" + ip + "','" + connection_name + "') "
                cursor.execute(postgres_insert_query)
                connection.commit()
            connection.close()
    except:
        return HttpResponse(' <head><link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css"><script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script><script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/js/bootstrap.min.js"></script><meta http-equiv = "refresh" content = "3; url =   db_connection  " /></head><div class="alert alert-danger">Unable to save your connection,Please check username , ip or password of your connection</div>')

    return render(request, 'Databases.html', context)

def tables_connection(request):
    read_template_file_path = ss.read_template_path_file(request)
    with open(read_template_file_path,"w") as outfile:
        outfile.write("0")
        outfile.close()

    context = {}
    db_selected = request.POST['dbs']
    list = db.read_connections(request)
    db_type = list[0]
    ip = list[1]
    username = list[2]
    password = list[3]
    connect = ""
    table_list = []

    db_file_path = ss.db_path(request)
    file1 = open(db_file_path, "w")
    file1.write(db_selected)



    if db_type == "1":
        connect = db.myssql_connection(ip,username,password)
        query = "SELECT table_name FROM information_schema.tables where table_schema  = '"+db_selected+"';"
        df = pd.read_sql(query,connect)
        for x in range(0,len(df)):
            table_list.append(df.loc[x,'table_name'])
        context['tables'] = table_list

    elif db_type == "5":
        connect = db.postgres_connection(ip, username, password)
        query = "SELECT table_name FROM information_schema.tables where table_schema  = '" + db_selected + "';"
        df = pd.read_sql(query, connect)
        for x in range(0, len(df)):
            table_list.append(df.loc[x, 'table_name'])
        context['tables'] = table_list

    elif db_type == "2":
        with teradatasql.connect(host=ip, user=username, password=password , encryptdata=True) as connect:
            query = "select TableName from dbc.tables where databasename = '" + db_selected + "' UNION SELECT  TableName from dbc.tablesV where databasename = '" + db_selected + "' ;"
            df = pd.read_sql(query, connect)
            for x in range(0, len(df)):
                table_list.append(df.loc[x, 'TableName'])
            context['tables'] = table_list
            # table_list.append('ADS_ECONOMICS_360_AI')

    context["save_checkbox"] = 1
    return render(request , 'tables.html',context)

def form(request):
    return render(request , 'form.html')

def file(request):
   _file= request.POST ['CSV_FILE']
   f = open("D_A_T/csv_path.txt",mode='w')
   f.write(_file)
   f.close()
   context = {}
   context['connection_type'] = '0'
   return  render(request , 'checks.html',context)





def apply_checks(request):
    checks = request.POST.getlist('check')
    file_path = open("D_A_T/csv_path.txt",'r+')
    path = file_path.readline()
     # r'C:\Users\nh250031\OneDrive - Teradata\Desktop\Modeling\ACA\Tool\template.csv'
    scripts = []
    splitted= []
    for check in checks:
        script = ""
        if check == "1":
            splitted.append("-- *** RI *** ")
            script = tables.tables.ref_tables_checks(path,0,1)
            lst = script.split(';')
            for x in lst:
                splitted.append(x + ';')
        elif(check == "2"):
            splitted.append("-- *** Total Rows ***")
            script = table.table.total_rows(path,0,1)
            lst = script.split(';')
            for x in lst:
                splitted.append(x + ';')
        elif (check == "3"):
            splitted.append("-- *** Total Columns ***")
            script += table.table.total_columns(path, 0)
            lst = script.split(';')
            for x in lst:
                splitted.append(x + ';')
        elif (check == "4"):
            splitted.append("-- *** Foreign key data type compatibility ***")
            script += table.table.pk_col(path, 0,1)
            lst = script.split(';')
            for x in lst:
                splitted.append(x + ';')
        elif (check == "5"):
            splitted.append("-- *** Distinct values ***")
            script += column.columns.distinct_values(path, 0)
            lst = script.split(';')
            for x in lst:
                splitted.append(x + ';')
        elif (check == "6"):
            splitted.append("-- *** Null percentage ***")
            script += column.columns.active_columns(path, 0)
            lst = script.split(';')
            for x in lst:
                splitted.append(x + ';')
        elif (check == "7"):
            splitted.append("-- *** Number of null values ***")
            script += column.columns.null_columns(path, 0)
            lst = script.split(';')
            for x in lst:
                splitted.append(x + ';')
        elif (check == "8"):
            splitted.append("-- *** Date validation ***")
            script += column.columns.date_validate(path, 0)
            lst = script.split(';')
            for x in lst:
                splitted.append(x + ';')
        elif (check == "9"):
            splitted.append("-- *** PK validation ***")
            script += column.columns.pk_stats(path,1)
            lst = script.split(';')
            for x in lst:
                splitted.append(x + ';')

    context = {'scripts':splitted}
    return render(request, 'display_scripts.html',context)

def display_editable_tables(request):
    context = {}
    lst = []
    big_dict = {}
    table_list = request.POST["sel_values"]

    clean_tables = table_list.replace("[","").replace("]","").replace("\'","")
    final_table_list = clean_tables.split(",")

    file_path = ss.templates_path(request)

    data = pd.read_csv(file_path, header=None)
    for index, row in data.iterrows():
        if index != 0:
            lst.append(row)

    total_column_list = []
    df = pd.read_csv(file_path)
    counter = 0
    table_list = []





    for table in final_table_list:

        col_lst = []
        for df_table in df["TABLE"]:
            if str(table).strip() == str(df_table).strip():

                col_lst.append(df["COLUMN"][counter])
                total_column_list.append(df["COLUMN"][counter])
                counter+=1
            else:
                continue

        big_dict[table] = col_lst

    context["well_formed_tables"] = big_dict
    context["constraints"] = ["FK","PK","NONE"]
    context["cols"] = total_column_list
    context["data"] = lst
    # return HttpResponse(big_dict["category"])


    return render(request, 'editable_table.html', context)

def csv_clean():



    username = getpass.getuser()
    # file = "C:\\Users\\"+username+"\\Downloads\\data.csv"
    file = "template.csv"
    data = pd.read_csv(file)
    return HttpResponse(data)
    list = []
    context = {}
    with open('template.csv', 'w') as csvfile:
        filewriter = csv.writer(csvfile, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
        filewriter.writerow(["DATABASE", "TABLE", "COLUMN" , "DATA_TYPE" , "NULLABLE" , "CONSTRAINT_TYPE" , "REFERENCE_TABLE_NAME" , "REFERENCE_COLUMN_NAME" , "DATAWAREHOUSE_TYPE"])
        for x in range(0, len(data)):
            const_type = str(data.loc[x,"CONSTRAINT_TYPE"]).split(">",1)
            const_type_clean = const_type[-1].split("<")
            const_type_clean[0] = const_type_clean[0].replace('Empty',' ')

            REF_TABLE = str(data.loc[x, "REFERENCE_TABLE_NAME"]).split(">", 1)
            REF_TABLE_clean = REF_TABLE[-1].split("<")
            REF_TABLE_clean[0] = REF_TABLE_clean[0].replace('Empty',' ')

            REF_COL = str(data.loc[x, "REFERENCE_COLUMN_NAME"]).split(">", 1)
            REF_COL_clean = REF_COL[-1].split("<")
            REF_COL_clean[0] = REF_COL_clean[0].replace('Empty',' ')

            # DWH_TYPE = str(data.loc[x, "DWH_TYPE"]).split(">", 1)
            # DWH_TYPE_clean = DWH_TYPE[-1].split("<")
            # DWH_TYPE_clean[0] = DWH_TYPE_clean[0].replace('Empty',' ')

            filewriter.writerow([str(data.loc[x,"DATABASE"]),str(data.loc[x,"TABLE"]),str(data.loc[x,"COLUMN"]),str(data.loc[x,"DATA_TYPE"]),str(data.loc[x,"NULLABLE"]),const_type_clean[0],REF_TABLE_clean[0],REF_COL_clean[0]])
            # list.append(str(data.loc[x,"REFERENCE_TABLE_NAME"]))


    context['connection_type'] = '1'
    return context


def db_checks(request):
    list = db.read_connections(request)
    db_type = list[0]
    ip = list[1]
    username = list[2]
    password = list[3]
    db_file_path = ss.db_path(request)
    file = open(db_file_path, "r+")
    database = file.read()
    checks = request.POST.getlist('check')
    checks_names = []
    context = {}
    scripts = []
    batch_ids = request.POST["batch_ids"]
    batch_ids = str(batch_ids).split(",")
    new_batch = []
    ui_output = []
    ui_result_list = []
    execution_date = datetime.datetime.now()
    # d = datetime.now()
    pacific = pytz.timezone('Etc/GMT+2')
    execution_date = pacific.localize(execution_date)
    for batch in batch_ids:
        new_batch.append(batch.replace("'","").replace("[","").replace("]","").replace(" ",""))

    file_path = ss.templates_path(request)
    total_batches_count_from_file = ss.total_sessions(request)
    try :
        with open(total_batches_count_from_file, "r")  as total_number_of_batches:
            batches_count = total_number_of_batches.read()
    except:
        batches_count = 0

    for check in checks:
        if check == "1":
            scripts.append(tables.tables.ref_tables_checks(file_path, 0,db_type))
            checks_names.append('Referential Integrity')
        elif (check == "2"):
            scripts.append(table.table.total_rows(file_path, 0))
            checks_names.append('Total rows')
        elif (check == "3"):
            scripts.append(table.table.total_columns(file_path, 0))
            checks_names.append('Total columns')
        elif (check == "4"):
            scripts.append(table.table.pk_col(file_path, 0,db_type))
            checks_names.append('Foreign key data type compatibility')
        elif (check == "5"):
            scripts.append(column.columns.distinct_values(file_path, 0))
            checks_names.append('Distinct values')
        elif (check == "6"):
            scripts.append(column.columns.active_columns(file_path, 0))
            checks_names.append('Null percentage')
        elif (check == "7"):
            scripts.append(column.columns.null_columns(file_path, 0))
            checks_names.append('Number of nulls')
        elif (check == "8"):
            scripts.append(column.columns.date_validate(file_path, 0))
            checks_names.append('Date validation')
        elif (check == "9"):
            scripts.append(column.columns.pk_stats(file_path,db_type))
            checks_names.append('PK validation')
    file = pd.read_csv(file_path)
    table_name = file["TABLE"][0]
    last_modified_batches_list = []
    batched_scripts = []

    try:
        batched_scripts = batcher.batching(scripts,new_batch,request)
    except :
        batched_scripts = scripts
    # return HttpResponse(batched_scripts)
    for sc in batched_scripts:
        if "join" in sc.lower():
            last_modified_batches_list.append(sc.replace("batch_id",table_name + ".batch_id"))
        else:
            last_modified_batches_list.append(sc)
    # return HttpResponse(scripts)
    # last_modified_batches_list = scripts







    if db_type == "1":
        engine = db.myssql_connection(ip, username, password)
        with engine.connect() as connection:
            # for script in scripts:
            #     result = connection.execute(script)
            big_list = []
            n = 0
            temp = []
            # return HttpResponse(last_modified_batches_list)
            for script in scripts:
                script = script.replace('@Empty', '')
                script_splitted = script.split(';')
                temp.append(script)

                for x in script_splitted:
                    if x == "":
                        continue
                    else:

                        output = []
                        txt = str(x).lower().split("from")[1].split()[0]
                        table_name = txt.split(".")[-1]
                        join_split = x.split('JOIN')
                        if len(join_split) > 1:
                            on_split = join_split[-1].split('ON')
                            ref_table = (on_split[0].split('.'))[-1]
                            table_name += ', ' + ref_table
                        output.append(checks_names[n])
                        output.append(table_name)
                        result = pd.read_sql(str(x) + ';', connection)
                        column_names = result.head(0)
                        temp_ = ""

                        for i in range(0, len(result)):

                            for y in column_names:
                                temp_ += str(y) + " : " + str(result.loc[i, y]) + " "
                                # output.append(y + " : " + str(result.loc[i, y]))
                            # return HttpResponse(temp)
                            temp_ += ";"
                        output.append(temp_.split(";", 2))

                        if "true" in temp_ or len(temp_) == 0:
                            output.append("Passed")
                        else:
                            output.append("Failed")
                        big_list.append(output)
                n = n + 1


    elif db_type == "2":
        with teradatasql.connect(host=ip, user=username, password=password, encryptdata=True) as connect:
            big_list = []
            n = 0
            temp = []


            for script in scripts:
                script = script.replace('@Empty','')
                script_splitted = script.split(';')
                temp.append(script)
                # return  HttpResponse(last_modified_batches_list)



                for x in script_splitted:
                    if x == "":
                        continue
                    else:

                        output = []
                        ui_output = []
                        # return HttpResponse(str(x).lower().split("from")[1].split()[0].split(".")[-1])
                        try:

                            txt = str(x).lower().split("from")[1].split()[0]
                        except:
                            print(str(x))
                        table_name = txt.split(".")[-1]

                        # return HttpResponse(table_name)
                        join_split = x.split('JOIN')
                        if len(join_split) > 1:
                            on_split = join_split[-1].split(' ON')
                            ref_table = (on_split[0].split('.'))[1].split(" ")[0].replace("ON","")
                            # return HttpResponse(ref_table)
                            table_name += ', ' + ref_table
                            # return HttpResponse(on_split[0].split('.')[2])

                        ui_output.append(execution_date)
                        ui_output.append(len(batch_ids))
                        ui_output.append(batches_count)
                        ui_output.append(checks_names[n])
                        ui_output.append(table_name)
                        output.append(checks_names[n])
                        output.append(table_name)
                        # return HttpResponse(x)
                        result = pd.read_sql(str(x) + ';', connect)
                        column_names = result.head(0)
                        temp_ = ""

                        for i in range(0, len(result)):

                            for y in column_names:
                                temp_ += str(y) + " : " + str(result.loc[i, y]) + " "
                                # output.append(y + " : " + str(result.loc[i, y]))
                        # return HttpResponse(temp)
                            temp_+=";"
                        output.append(temp_.split(";",2))
                        ui_output.append(temp_.split(";", 2))

                        if "true" in temp_ or len(temp_) == 0:
                            output.append("Passed")
                            ui_output.append("Passed")
                        else:
                            output.append("Failed")
                            ui_output.append("Failed")
                        ui_result_list.append(ui_output)
                        big_list.append(output)
                n = n + 1
    try:
        tcs.main_insertion(request,big_list)
    except:
        x =0
    # context['result'] = big_list
    context['result'] = ui_result_list
    # return HttpResponse(big_list)
    return render(request, 'display_results.html', context)
# def db_checks(request):
#     list = db.read_connections(request)
#     db_type = list[0]
#     ip = list[1]
#     username = list[2]
#     password = list[3]
#     db_file_path = ss.db_path(request)
#     file = open(db_file_path, "r+")
#     database = file.read()
#     checks = request.POST.getlist('check')
#     checks_names = []
#     context = {}
#     scripts = []
#     batch_ids = request.POST["batch_ids"]
#     batch_ids = str(batch_ids).split(",")
#     new_batch = []
#     ui_output = []
#     ui_result_list = []
#     execution_date = datetime.datetime.now()
#     # d = datetime.now()
#     pacific = pytz.timezone('Etc/GMT+2')
#     execution_date = pacific.localize(execution_date)
#     for batch in batch_ids:
#         new_batch.append(batch.replace("'","").replace("[","").replace("]","").replace(" ",""))
#
#     file_path = ss.templates_path(request)
#     total_batches_count_from_file = ss.total_sessions(request)
#     try :
#         with open(total_batches_count_from_file, "r")  as total_number_of_batches:
#             batches_count = total_number_of_batches.read()
#     except:
#         batches_count = 0
#
#
#     for check in checks:
#         if check == "1":
#             scripts.append(tables.tables.ref_tables_checks(file_path, 0,db_type))
#             checks_names.append('Referential Integrity')
#         elif (check == "2"):
#             scripts.append(table.table.total_rows(file_path, 0))
#             checks_names.append('Total rows')
#         elif (check == "3"):
#             scripts.append(table.table.total_columns(file_path, 0))
#             checks_names.append('Total columns')
#         elif (check == "4"):
#             scripts.append(table.table.pk_col(file_path, 0,db_type))
#             checks_names.append('Foreign key data type compatibility')
#         elif (check == "5"):
#             scripts.append(column.columns.distinct_values(file_path, 0))
#             checks_names.append('Distinct values')
#         elif (check == "6"):
#             scripts.append(column.columns.active_columns(file_path, 0))
#             checks_names.append('Null percentage')
#         elif (check == "7"):
#             scripts.append(column.columns.null_columns(file_path, 0))
#             checks_names.append('Number of nulls')
#         elif (check == "8"):
#             scripts.append(column.columns.date_validate(file_path, 0))
#             checks_names.append('Date validation')
#         elif (check == "9"):
#             scripts.append(column.columns.pk_stats(file_path,db_type))
#             checks_names.append('PK validation')
#     file = pd.read_csv(file_path)
#     table_name = file["TABLE"][0]
#     last_modified_batches_list = []
#
#
#     batched_scripts = batcher.batching(scripts,new_batch,request)
#     # return HttpResponse(batched_scripts)
#     for sc in batched_scripts:
#         if "join" in sc.lower():
#             last_modified_batches_list.append(sc.replace("batch_id",table_name + ".batch_id"))
#         else:
#             last_modified_batches_list.append(sc)
#
#
#     # return HttpResponse(last_modified_batches_list)
#
#
#
#     if db_type == "1":
#         engine = db.myssql_connection(ip, username, password)
#         with engine.connect() as connection:
#             # for script in scripts:
#             #     result = connection.execute(script)
#             big_list = []
#             n = 0
#             temp = []
#             for script in last_modified_batches_list:
#                 script = script.replace('@Empty', '')
#                 script_splitted = script.split(';')
#                 temp.append(script)
#
#                 for x in script_splitted:
#                     if x == "":
#                         continue
#                     else:
#
#                         output = []
#                         txt = str(x).lower().split("from")[1].split()[0]
#                         table_name = txt.split(".")[-1]
#                         join_split = x.split('join')
#                         if len(join_split) > 1:
#                             on_split = join_split[-1].split('ON')
#                             ref_table = (on_split[0].split('.'))[-1]
#                             table_name += ', ' + ref_table
#                         output.append(checks_names[n])
#                         output.append(table_name)
#                         result = pd.read_sql(str(x) + ';', connection)
#                         column_names = result.head(0)
#                         temp_ = ""
#
#                         for i in range(0, len(result)):
#
#                             for y in column_names:
#                                 temp_ += str(y) + " : " + str(result.loc[i, y]) + " "
#                                 # output.append(y + " : " + str(result.loc[i, y]))
#                             # return HttpResponse(temp)
#                             temp_ += ";"
#                         output.append(temp_.split(";", 2))
#
#                         if "true" in temp_ or len(temp_) == 0:
#                             output.append("Passed")
#                         else:
#                             output.append("Failed")
#                         big_list.append(output)
#                 n = n + 1
#
#
#     elif db_type == "2":
#         with teradatasql.connect(host=ip, user=username, password=password, encryptdata=True) as connect:
#             big_list = []
#
#             n = 0
#             temp = []
#             for script in last_modified_batches_list:
#                 script = script.replace('@Empty','')
#                 script_splitted = script.split(';')
#                 temp.append(script)
#
#                 for x in script_splitted:
#                     if x == "":
#                         continue
#                     else:
#
#                         output = []
#                         ui_output = []
#                         txt = str(x).lower().split("from")[1].split()[0]
#                         table_name = txt.split(".")[-1]
#                         join_split = x.split('JOIN')
#                         if len(join_split) > 1:
#                             on_split = join_split[-1].split('ON')
#                             ref_table = (on_split[0].split('.'))[1]
#                             # return HttpResponse(str(ref_table.split("\n")[0]))
#                             table_name += ', ' + str(ref_table.split("\n")[0])
#
#                         # output.append(execution_date)
#                         ui_output.append(execution_date)
#                         ui_output.append(len(batch_ids))
#                         ui_output.append(batches_count)
#                         ui_output.append(checks_names[n])
#                         ui_output.append(table_name)
#                         output.append(checks_names[n])
#                         output.append(table_name)
#                         # return HttpResponse(x)
#                         result = pd.read_sql(str(x) + ';', connect)
#                         column_names = result.head(0)
#                         temp_ = ""
#
#                         for i in range(0, len(result)):
#
#                             for y in column_names:
#                                 temp_ += str(y) + " : " + str(result.loc[i, y]) + " "
#                                 # output.append(y + " : " + str(result.loc[i, y]))
#                         # return HttpResponse(temp)
#                             temp_+=";"
#                         output.append(temp_.split(";",2))
#                         ui_output.append(temp_.split(";",2))
#
#                         if "true" in temp_ or len(temp_) == 0:
#                             output.append("Passed")
#                             ui_output.append("Passed")
#                         else:
#                             output.append("Failed")
#                             ui_output.append("Failed")
#                         big_list.append(output)
#                         ui_result_list.append(ui_output)
#                 n = n + 1
#
#     tcs.main_insertion(request,big_list)
#
#     context['result'] = ui_result_list
#     return render(request, 'display_results.html', context)

def test_cases(request):
   batch_id_list = request.POST.getlist("check")
   table = request.POST["table_values"]
   list = table.split(",")
   temp = []
   big_list = []
   context = {}
   table_list = request.POST["selected"]
   context["table_list"] = table_list
   context["batch_ids"] = batch_id_list
   batch_id_list = request.POST.getlist("check")
   batch_file_path = ss.batch_file_path(request)
   batch_file = open(batch_file_path, "w")
   batch_file.write(str(batch_id_list))

   return render(request, 'test_cases.html' ,context)

def statistics(request):
    batch_id_list = request.POST.getlist("check")
    context = {}
    context['result'] = []
    result_list = []
    table_list = request.POST["selected"]
    content_list = request.POST["table_values"]
    final_content_list = content_list.split(",")
    context["table_list"] = table_list
    big_list = []
    list = db.read_connections(request)
    db_type = list[0]
    ip = list[1]
    username = list[2]
    password = list[3]

    batch_id_list = request.POST.getlist("check")
    batch_file_path = ss.batch_file_path(request)
    batch_file = open(batch_file_path, "w")
    batch_file.write(str(batch_id_list))
    # return HttpResponse(batch_id_list)


    file_path = ss.templates_path(request)
    result = table.table.total_columns(file_path, 0)
    table_names = result['tables']
    # return HttpResponse(table_names)
    col_no = result['columns']
    dbs = result['dbs']
    # dbs_number = []
    ui_output = []
    result_ui =[]


    row_no = []
    temp = table.table.total_rows(file_path, 0,db_type).replace('@Empty','')
    temp_write = []
    scripts = temp.split(';')
    batched_scripts = []
    try:
        batched_scripts = batcher.batching(scripts, batch_id_list,request)
    except:
        batched_scripts = scripts

    total_batches_count_from_file = ss.total_sessions(request)
    try :
        with open(total_batches_count_from_file, "r")  as total_number_of_batches:
            batches_count = total_number_of_batches.read()
    except:
        batches_count = 0

    execution_date = datetime.datetime.now()
    # d = datetime.now()
    pacific = pytz.timezone('Etc/GMT+2')
    execution_date = pacific.localize(execution_date)



    if db_type == "2":
        with teradatasql.connect(host=ip, user=username, password=password, encryptdata=True) as connect:

            for script in batched_scripts:
                result = pd.read_sql(str(script) + ';', connect)
                column_names = result.head(0)
                for i in range(0, len(result)):
                    for y in column_names:
                        row_no.append(str(result.loc[i, y]))
            # return HttpResponse(row_no)


    elif db_type == "1":
        # with mysql.connector.connect(
        #         host=ip,
        #         user=username,
        #         password=password
        # ) as connection:

        connection = mysql.connector.connect(
                host=ip,
                user=username,
                password=password
        )
        for script_counter in range(0, len(batched_scripts)):
            mycursor = connection.cursor()
            mycursor.execute(batched_scripts[script_counter])
            myresult = mycursor.fetchone()

            row_no.append(str(myresult).replace("(", "").replace(",", "").replace(")", ""))
    # return HttpResponse(len(row_no))
    decremental_counter = 0
    for i in range(0,len(table_names)):

        temp_ =[]
        ui_output=[]
        # ui_output.append(str(batch_id_list).replace("[","").replace("]",""))
        ui_output.append(execution_date)
        ui_output.append(len(batch_id_list))
        ui_output.append(batches_count)
        try:
            ui_output.append(dbs[i])
        except:
            # decremental_counter +=1
            # critical_counter = i-decremental_counter
            ui_output.append(dbs[0])
        ui_output.append(table_names[i])
        temp_.append(table_names[i])
        ui_output.append(col_no[i])
        temp_.append(col_no[i])
        try:
            ui_output.append(row_no[i])
            temp_.append(row_no[i])
        except:
            continue
        result_list.append(temp_)
        result_ui.append(ui_output)


    context["result"] = result_list
    context["result_ui"] = result_ui
    # return HttpResponse(result_ui)
    # sts.main_insertion_for_table(request,result_list)
    # return HttpResponse(context["result"])
    context["batch_ids"] = batch_id_list
    context["dbs"] = dbs


    return render(request, 'statistics.html', context)

def statistics_table(request):
    list = db.read_connections(request)
    context = {}
    db_type = list[0]
    ip = list[1]
    username = list[2]
    password = list[3]
    selected_tables = request.POST.getlist('check')
    chart_data = {}
    chart_data_tables = []
    chart_data_columns = []
    chart_data_total_rows = []
    chart_data_nulls = []
    chart_data_distinct = []
    big_list = []
    # batch_ids = request.POST["batch_ids"]
    new_batch = []
    batch_file_path = ss.batch_file_path(request)
    file = open(batch_file_path,"r")

    batch_ids = file.read()

    print(str(batch_ids))
    db_hide = request.POST["db_hide"]
    if str(batch_ids).strip() == "['']" or str(batch_ids) == '' or str(batch_ids) == "[]":
        print("No Batch_ids")
        new_batch = []
    else:
        print("THERE ARE BATCH IDS")
        batch_ids = str(batch_ids).split(",")
        for batch in batch_ids:
            new_batch.append(batch.replace("'", "").replace("[", "").replace("]", "").replace(" ", ""))

    # file_path = dask.delayed(ss.templates_path)(request)
    # temp_path = dask.delayed(ss.temp_path)(request)
    # scripts = dask.delayed(pop.code_replace)(file_path, temp_path, selected_tables ,db_type)
    # batched_scripts =dask.delayed(batcher.batching)(scripts,new_batch,request)

    file_path = ss.templates_path(request)
    temp_path = ss.temp_path(request)
    scripts = pop.code_replace(file_path, temp_path, selected_tables, db_type)
    batched_scripts = []

    try:
        batched_scripts = batcher.batching(scripts, new_batch, request)
    except:
        batched_scripts = scripts

    if db_type == "2":
        print("TERADATA_CHOICE")
        with teradatasql.connect(host=ip, user=username, password=password, encryptdata=True) as connect:

            tuple_csv_data_organized = dask.delayed(pop.populate)(request,connect,batched_scripts,len(batch_ids),db_hide)

            # tuple_csv_data_organized.compute()

            big_list = tuple_csv_data_organized.compute()[0]
            ui_list = tuple_csv_data_organized.compute()[1]
            for element in big_list:
                chart_data_tables.append(element[0])
                chart_data_columns.append(element[1])
                chart_data_total_rows.append(element[4])
                chart_data_distinct.append(element[2])
                chart_data_nulls.append(element[3])



            # chart_data_tables = tuple_csv_data_organized.compute()[1]
            # chart_data_columns = tuple_csv_data_organized.compute()[2]
            # chart_data_total_rows = tuple_csv_data_organized.compute()[3]
            # chart_data_distinct = tuple_csv_data_organized.compute()[4]
            # chart_data_nulls = tuple_csv_data_organized.compute()[5]

            # tuple_csv_data_organized = pop.populate(connect, batched_scripts)
            # big_list = tuple_csv_data_organized[0]
            # chart_data_tables = tuple_csv_data_organized[1]
            # chart_data_columns = tuple_csv_data_organized[2]
            # chart_data_total_rows = tuple_csv_data_organized[3]
            # chart_data_distinct = tuple_csv_data_organized[4]
            # chart_data_nulls = tuple_csv_data_organized[5]

    elif db_type == "1":
        connect = db.myssql_connection(ip, username, password)
        output = []
        n = 0
        
        for script in batched_scripts:
            script = script.replace('@Empty', '')
            txt = str(script).split(".", 1)[-1]
            table_name = txt.split(" ", 1)[0].replace(";","")
            table_name = table_name.replace('WHERE', '').strip()  # for unknown behaviour

            if table_name not in str(output):
                output.append(table_name)
            result = pd.read_sql(str(script) + ';', connect)
            column_names = result.head(0)

            for i in range(0, len(result)):
                for y in column_names:
                    if y not in output:
                        output.append(y)
                    output.append(str(result.loc[i, y]).replace("b'","").replace("'",""))
                    n = n + 1
            if n == 5:
                big_list.append(output)
                chart_data_tables.append(output[0])
                chart_data_columns.append(output[1])

                try:
                    chart_data_total_rows.append(output[2].split('out of')[1])
                    chart_data_distinct.append(output[2].split('out of')[0])
                except:
                    return HttpResponse(column_names)
                chart_data_nulls.append(output[4])
                output = []
                n = 0


    chart_data['table'] = chart_data_tables
    chart_data['column'] = chart_data_columns
    chart_data['total_rows'] = chart_data_total_rows
    chart_data['distinct'] = chart_data_distinct
    chart_data['null'] = chart_data_nulls
    chart_data_file_path = ss.chart_data_path(request)
    with open(chart_data_file_path, 'w') as outfile:
        json.dump(chart_data, outfile)
    context['result'] = big_list
    context["chart_dictionary"] = chart_data
    context['ui_result'] = ui_list
    # return HttpResponse(big_list[0])
    sts.main_insertion_in_more_stats(big_list)
    return render(request, 'statistics_table.html', context)

def save_template(request):
    table_list = request.POST.getlist('tables')

    try:
        save_source = request.POST['save_source']
        source_name = request.POST['source']
        source_file_path = ss.get_file_path(request,'S')

        with open(source_file_path, 'w') as file:
            file.write(source_name)
            file.close()

    except:

        save_source = "0"

    list = db.read_connections(request)
    db_type = list[0]
    ip = list[1]
    username = list[2]
    password = list[3]
    db_file_path = ss.db_path(request)
    db_selected = open(db_file_path,'r').read()
    connection_file_path = ss.get_file_path(request, 'C')
    connection_name = open(connection_file_path,'r').read()


    if save_source == '1':
        connection = psycopg2.connect(user=dbUsername,password=dbPassword,host=dbHost,database=dbName)
        cursor = connection.cursor()

        connection_exist_query = 'SELECT id FROM public."D_A_T_user_connection" WHERE database_type = '+db_type+" and username = '"+username+"' and ip_address = '"+ ip+"' and password = '"+ password +"' and connection_name = '"+ connection_name +"' and user_id_id = " + str(request.user.id) + ';'
        cursor.execute(connection_exist_query)
        connection.commit()
        result = cursor.fetchall()

        if len(result) == 0: # connection doesn't exist
            create_connection_query = 'INSERT INTO public."D_A_T_user_connection"(database_type, username, password, ip_address, connection_name, user_id_id) ' \
                                      'VALUES ('+ db_type +", '"+ username +"','"+ password +"','" + ip +"','" + connection_name+"',"+ str(request.user.id) + ');'

            cursor.execute(create_connection_query)
            connection.commit()
            cursor.execute(connection_exist_query)
            connection.commit()
            result = cursor.fetchall()

        connection_id = result[0][0]
        insert_query = 'INSERT INTO public."D_A_T_user_source"(connection_id_id,database,source,tables) VALUES('+ \
                       str(connection_id) +",'"+db_selected+"','"+source_name+"','"+str(table_list).replace("'","")+"')"
        cursor.execute(insert_query)
        connection.commit()
    
    big_dict = {}
    read_template_file_path = ss.read_template_path_file(request)
    read_template = open(read_template_file_path,"r").read()
    file_path = ss.templates_path(request)

    if read_template == "0":
        with open(file_path, 'w') as csvfile:
            filewriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            filewriter.writerow(["DATABASE", "TABLE", "COLUMN", "DATA_TYPE", "NULLABLE", "CONSTRAINT_TYPE", "REFERENCE_TABLE_NAME","REFERENCE_COLUMN_NAME"])
            if db_type == "2":
               with teradatasql.connect(host=ip, user=username, password=password, encryptdata=True) as connect:
                    for table_ in table_list:
                        query = "SELECT TableName,ColumnName,Nullable FROM dbc.COLUMNS WHERE DatabaseName = '" + db_selected.strip() + "' AND TableName ='" + table_.strip() + "';"
                        df = pd.read_sql(query, connect)
                        for i in range(0, len(df)):
                            query_type = "SELECT TOP 1 TYPE(" + str(df.loc[i, 'ColumnName']).strip() + ") as type_" + " FROM " + db_selected.strip() + "." + table_.strip() + ";"
                            result = pd.read_sql(query_type, connect)
                            filewriter.writerow((str(db_selected).strip(), str(df.loc[i, 'TableName']).strip(), str(df.loc[i, 'ColumnName']).strip(),str(result.loc[0, 'type_']).strip().replace(",","*/"), df.loc[i, 'Nullable'], " ", " ", " "))

            elif db_type == "1":
                connect =db.myssql_connection(ip,username,password)
                for table_ in table_list:
                    # query = "SELECT TableName,ColumnName,Nullable FROM dbc.COLUMNS WHERE DatabaseName = '" + db_selected + "' AND TableName ='" + table_ + "';"
                    query = "SELECT COLUMNS.COLUMN_NAME , COLUMNS.TABLE_NAME,COLUMNS.COLUMN_TYPE, COLUMNS.TABLE_SCHEMA,COLUMNS.IS_NULLABLE, KEY_COLUMN_USAGE.CONSTRAINT_NAME as CONSTRAINT_NAME, KEY_COLUMN_USAGE.REFERENCED_TABLE_NAME as REFERENCED_TABLE_NAME , KEY_COLUMN_USAGE.REFERENCED_COLUMN_NAME as REFERENCED_COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS LEFT JOIN  INFORMATION_SCHEMA.KEY_COLUMN_USAGE ON(COLUMNS.COLUMN_NAME=KEY_COLUMN_USAGE.COLUMN_NAME AND COLUMNS.TABLE_NAME=KEY_COLUMN_USAGE.TABLE_NAME AND COLUMNS.TABLE_SCHEMA=KEY_COLUMN_USAGE.TABLE_SCHEMA )WHERE COLUMNS.TABLE_SCHEMA ='" + str(db_selected) + "' AND COLUMNS.TABLE_NAME = '" +str(table_) + "';"

                    df = pd.read_sql(query, connect)

                    for i in range(0, len(df)):


                        filewriter.writerow((db_selected, df.loc[i, 'TABLE_NAME'], df.loc[i, 'COLUMN_NAME'],
                                             str(df.loc[0, 'COLUMN_TYPE']).strip(), df.loc[i, 'IS_NULLABLE'], " ", " ", " "))

    context =  {}
    data = pd.read_csv(file_path, header=None)
    lst = []
    for index, row in data.iterrows():
        if index != 0:
            lst.append(row)
    total_column_list = []
    context["data"] = lst
    df = pd.read_csv(file_path)
    counter = 0

    for table in table_list:
        col_lst = []
        for df_table in df["TABLE"]:
            if str(table).strip() == str(df_table).strip():
                col_lst.append(str(df["COLUMN"][counter]).strip())
                total_column_list.append(df["COLUMN"][counter])
                counter+=1
            else:
                continue
        big_dict[str(table).strip()] = col_lst

        col_lst = []
        # col_lst.clear()
        # return HttpResponse(big_dict)
    context["well_formed_tables"] = big_dict
    context["constraints"] = ["FK","PK","nan"]
    context["cols"] = total_column_list
    context["sel_tables"] = table_list


    return render(request, 'editable_table.html', context)

def charts(request):
    chart_dict = request.POST["chart_dict"]
    chart_data_file_path = ss.chart_data_path(request)
    with open(chart_data_file_path) as json_file:
        result = json.load(json_file)


    data_length = []
    for i in range(0,len(result['table'])):
        data_length.append(i)

    return render(request, 'pie_chart.html', {
        'dict' : chart_dict,
        'data_length' : data_length,

    })
def save_note(request):
    note = request.POST['note_in']
    select_query = 'SELECT note FROM public."D_A_T_user_note" WHERE user_id_id = ' + str(request.user.id)

    insert_query = 'INSERT INTO public."D_A_T_user_note"(user_id_id,note,note_date) VALUES (' + str(request.user.id) + ",'"+ str(note) + "','"+ str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M')) +"' ) "

    connection = psycopg2.connect(user=dbUsername,
                                          password=dbPassword,
                                          host=dbHost,
                                          database=dbName)
    cursor = connection.cursor()
    cursor.execute(select_query)
    connection.commit()
    result = cursor.fetchall()
    flag = 1
    for item in list(result):
        if note in item:
            flag = 0
            break
    if flag == 1:
        cursor.execute(insert_query)
        connection.commit()
    connection.close()
    return index(request)


def save_csv(request):
    table = request.POST["table_values"]

    list = table.split(",")
    temp = []
    big_list = []
    context = {}
    table_list = request.POST["selected"]
    context["table_list"] = table_list

    counter = 0
    file_path = ss.templates_path(request)

    with open(file_path, 'w') as csvfile:
        filewriter = csv.writer(csvfile, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)

        for x in list:

            if counter == 8:
                counter = 0
                filewriter.writerow(temp)
                big_list.append(temp)
                temp = []
                if x == " " or  x== "nan":
                    temp.append(" ")
                else:
                    temp.append(str(x).strip())
                counter += 1
            else:
                if x == " " or x == "":
                    temp.append(" ")
                else:
                    temp.append(str(x).strip())
                counter += 1
        filewriter.writerow(temp)
    big_list.append(temp)
    context["retrieved_table"] = big_list[1:len(big_list)]
    read_template_file_path = ss.read_template_path_file(request)
    with open(read_template_file_path, 'w') as outfile:
        outfile.write("1")
    outfile.close()

    return render(request, 'final_template.html', context)


def chart_shot(request):
    chart_dict = request.POST["chart_dict"]
    chart_data_file_path = ss.chart_data_path(request)
    with open(chart_data_file_path) as json_file:
        result = json.load(json_file)

    data_length = []
    for i in range(0, len(result['table'])):
        data_length.append(i)

    return render(request, 'index.html', {
        'dict': chart_dict,
        'data_length': data_length,


    })
def edit_delete_notes(request):
    connection = psycopg2.connect(user=dbUsername,
                                          password=dbPassword,
                                          host=dbHost,
                                          database=dbName)
    cursor = connection.cursor()
    date = str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M'))

    try:
        id = request.POST["save_edit"]
        text_area = request.POST["text_area"+id]
        update_query = 'UPDATE public."D_A_T_user_note" set note = '+"'"+text_area+"'"+' , note_date = '+"'"+date+"' "+' WHERE id = ' + id+';'
        cursor.execute(update_query)
        connection.commit()



    except:
        id = request.POST["delete"]
        delete_query = 'DELETE FROM public."D_A_T_user_note" WHERE id = ' + id
        cursor.execute(delete_query)
        connection.commit()
    return index(request)

def columns_selection(request):
    batch_id_list = request.POST.getlist("check")
    batch_file_path = ss.batch_file_path(request)
    batch_file = open(batch_file_path, "w")
    batch_file.write(str(batch_id_list))

    batch_file.close()
    context = {}
    lst = []
    big_dict = {}
    # table_list = request.POST["tables"]
    table_list = []
    file_path = ss.templates_path(request)
    getting_tables = data = pd.read_csv(file_path)
    for table in getting_tables["TABLE"]:
        table_formatted = str(table).strip()
        if table_formatted not in table_list:
            table_list.append(table_formatted)
        else:
            continue
        # if table not in table_list:
        #     table_list.append[str(table)]
        # else:
        #     continue

    
    try:
        save_source = request.POST['save_source']
        source_name = request.POST['source']

    except:
        save_source = "0"

    list = db.read_connections(request)
    db_type = list[0]
    ip = list[1]
    username = list[2]
    password = list[3]
    db_file_path = ss.db_path(request)
    db_selected = open(db_file_path, 'r').read()

    if save_source == '1':
        connection = psycopg2.connect(user=dbUsername,password=dbPassword,host=dbHost,database=dbName)
        cursor = connection.cursor()
        insert_query = 'INSERT INTO public."D_A_T_user_source"(user_id_id,database_type,username,password,ip_address,database,source,tables) VALUES('+ \
                       str(request.user.id)+','+str(db_type)+",'"+str(username)+"','"+str(password)+"','"+str(ip)+"','"+db_selected+"','"+source_name+"','"+str(table_list).replace("'","")+"')"
        cursor.execute(insert_query)
        connection.commit()

    # clean_tables = table_list.replace("[", "").replace("]", "").replace("\'", "")
    # final_table_list = clean_tables.split(",")

    data = pd.read_csv(file_path, header=None)
    for index, row in data.iterrows():
        if index != 0:
            lst.append(row)

    total_column_list = []
    df = pd.read_csv(file_path)
    counter = 0
    table_list = []

    for table in table_list:

        col_lst = []
        for df_table in df["TABLE"]:
            if str(table).strip() == str(df_table).strip():

                col_lst.append(df["COLUMN"][counter])
                total_column_list.append(df["COLUMN"][counter])
                counter += 1
            else:
                continue

        big_dict[table] = col_lst

    context["well_formed_tables"] = big_dict
    context["cols"] = total_column_list
    context["data"] = lst


    return render(request, 'columns.html', context)

def create_dq_checks_template(request):
    context = {}
    columns = request.POST.getlist("cols")
    dq_path_template = ss.dq_template_path(request)

    with open(dq_path_template, 'w') as csvfile:
        filewriter = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        filewriter.writerow(("Database","Table","Column"))
        for row in columns :
            row_list = row.split(",")
            filewriter.writerow((row_list[0],row_list[1],row_list[2]))



    return render(request, 'table_of_checks.html', context)

def table_of_results(request):
    print("Loading")

    list = db.read_connections(request)
    db_type = list[0]
    ip = list[1]
    username = list[2]
    password = list[3]
    db_file_path = ss.db_path(request)
    file = open(db_file_path, "r+")
    check_description = request.POST.get("check_description")
    query = request.POST.get("query")
    check_name = request.POST.get("check_name")
    category = request.POST.get("category")
    # col_list = request.POST["selected"]
    checks = request.POST.getlist("check")
    context = {}
    context["check"] = checks
    # context["col_list"] = col_list
    context["check_description"] = check_description
    context["query"] = ""
    context["head"] = ""
    context["check_name"] = check_name
    context["category"] = category
    query_output = []
    query_head = []
    query_result = []
    queries_html_tables_list = []
    total_batches_count_from_file = ss.total_sessions(request)
    print("BATCHING")
    try:
        with open(total_batches_count_from_file, "r")  as total_number_of_batches:
            batches_count = total_number_of_batches.read()
    except:
        batches_count = 0
    execution_date = datetime.datetime.now()
    # d = datetime.now()
    pacific = pytz.timezone('Etc/GMT+2')
    execution_date = pacific.localize(execution_date)

    # insert_query = 'INSERT INTO public."D_A_T_user_query"(user_id_id,query,category,check_name,query_date) VALUES (' + str(
    #     request.user.id) + ",'" + str(query) + "','" + str(category) + "','" + str(check_name) + "','" + str(
    #     check_description) + "','" + str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M')) + "' ) "
    #
    # connection = psycopg2.connect(user=dbUsername,
    #                               password=dbPassword,
    #                               host=dbHost,
    #                               database=dbName)

    if db_type == "2" and query != "":
        print("QUERY WRITTEN IN TERADATA")

        with teradatasql.connect(host=ip, user=username, password=password, encryptdata=True) as connect:

            try:
                splitted_queries = str(query).split(";")
                for single_query in splitted_queries:
                    sql_query_runner = pd.read_sql(single_query, connect)
                    geeks_object = sql_query_runner.to_html()
                    queries_html_tables_list.append(geeks_object)


                context["query"] = queries_html_tables_list
                context["head"] = query_head



            except:
                return HttpResponse(
                    ' <head>  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css"><script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script><script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/js/bootstrap.min.js"></script><meta http-equiv = "refresh" content = "3; url = "table_of_checks.html" " /></head><div class="alert alert-danger">Query is incorrect</div>')

    elif db_type == "1" and query != "":
        splitted_queries = str(query).split(";")

        engine = db.myssql_connection(ip, username, password)
        with engine.connect() as connection1:
            try:
                for single_query in splitted_queries:
                    sql_query_runner = pd.read_sql(single_query, connection1)
                    geeks_object = sql_query_runner.to_html()
                    queries_html_tables_list.append(geeks_object)



                # return HttpResponse(geeks_object)

                # context["query"] = geeks_object
                context["query"] = queries_html_tables_list
                context["head"] = query_head
            except:
                return HttpResponse(
                    ' <head>  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css"><script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script><script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/js/bootstrap.min.js"></script><meta http-equiv = "refresh" content = "3; url = "table_of_checks.html" " /></head><div class="alert alert-danger">Query is incorrect</div>')

    print("OUT OF PERMODA TRIANGLE")
    path = ss.dq_template_path(request)

    scripts_category_dictionary = dq.run_dq_checks(path, checks,db_type, request)
    columns = pd.read_csv(path)
    columns_selected = []
    print("BEFORE GETTING COLUMNS")
    for col in columns["Column"]:
        columns_selected.append(str(col).strip())
        print(col)


    context["NID_Accuracy"] = 0
    context["mobile_Accuracy"] = 0
    context["null_percentage_Completeness"] = 0
    context["Transaction_uniqueness_Consistency"] = 0
    context["lookups_uniqueness"] = 0
    context["NID_Comformty"] = 0
    context["Mobile_Comformty"] = 0
    title_list = []
    scores_list = []
    ui_scores_list = []
    batch_ids = batcher.string_to_list(request)
    accuracy_score = []

    for check in checks:
        print(check)
        if check == "2.1.0" or check == "4.2.0":
            continue

        # return  HttpResponse(scripts_category_dictionary.values())
        ui_scores_list.append(execution_date)
        ui_scores_list.append(len(batch_ids))
        ui_scores_list.append(batches_count)

        if check == "1.1.0":
            # title_list.append("NID Correctness")
            # scores_list.append(accuracy_score)
            print("NID CHECK")
            accuracy_score = dq.accuracy_percentage_check(scripts_category_dictionary, check, ip, username, password,
                                                          "NID Correctness","Accuracy & Correctness",db_type)
            print(accuracy_score)


        if check == "1.2.0":
            # title_list.append("phone number Correctness")
            # scores_list.append(accuracy_score)
            # accuracy_score.append("")
            accuracy_score = dq.accuracy_percentage_check(scripts_category_dictionary, check, ip, username, password,
                                                          "phone number Correctness","Accuracy & Correctness",db_type)

        if check == "3.1.0":
            # title_list.append("Nulls percentage in a row")
            # scores_list.append(accuracy_score)
            # accuracy_score.append("")
            accuracy_score = dq.accuracy_percentage_check(scripts_category_dictionary, check, ip, username, password,
                                                          "Nulls percentage in a row","Completeness",db_type)

        if check == "4.1.0":
            # title_list.append("Transaction Table uniqueness")
            # scores_list.append(accuracy_score)
            # accuracy_score.append("Transaction Table uniqueness")
            accuracy_score = dq.accuracy_percentage_check(scripts_category_dictionary, check, ip, username, password,
                                                          "Transaction Table uniqueness","Consistency",db_type)
        if check == "5.1.0":
            # title_list.append("lookups uniqueness")
            # scores_list.append(accuracy_score)
            # accuracy_score.append("lookups uniqueness")
            accuracy_score = dq.accuracy_percentage_check(scripts_category_dictionary, check, ip, username, password,
                                                          "lookups uniqueness","uniqueness",db_type)

        if check == "6.1.0":
            # title_list.append("NID formats")
            # scores_list.append(accuracy_score)
            # accuracy_score.append("NID formats")
            accuracy_score = dq.accuracy_percentage_check(scripts_category_dictionary, check, ip, username, password,
                                                          "NID formats","Comformaty",db_type)
        if check == "6.2.0":
            # title_list.append("Mobile formats")
            # scores_list.append(accuracy_score)
            # accuracy_score.append("Mobile formats")
            accuracy_score = dq.accuracy_percentage_check(scripts_category_dictionary, check, ip, username, password,
                                                          "Mobile formats","Comformaty",db_type)

        # else:
        #     if db_type == "1":
        #         engine = db.myssql_connection(ip, username, password)
        #         with engine.connect() as connection1:
        #             sql_query_runner = pd.read_sql(check, engine)
        #             geeks_object = sql_query_runner.to_html()
        #             context["query"] = geeks_object
        #             context["head"] = query_head



        scores_list.append(accuracy_score)
        print(scores_list)

    filtered_list = filter(None, scores_list)
    # return HttpResponse(filtered_list)
    # dqs.main_insertion(request,filtered_list)




    context["title"] = title_list
    # context["checks_values"] = scores_list
    context["result_data"] = scores_list
    length=len(scores_list)
    chart_category={}
    chart_result={}
    last_dq_run = {}
    chart_dq_data={}
    chart_final_result={}
    table_name=[]
    column_name=[]
    results=[]
    categories=[]
    temp_list=[]
    temp_list_final=[]
    for i in range (length):
        for j in range (len(scores_list[i])):
            table_name.append(scores_list[i][j][1])
            column_name.append(scores_list[i][j][2])
            results.append(scores_list[i][j][3])
            categories.append(scores_list[i][j][5])
            if scores_list[i][j][5] in chart_category.keys():
                chart_category[scores_list[i][j][5]]+=int(scores_list[i][j][3])
                chart_result[scores_list[i][j][5]]+=1
            else:
                chart_category[scores_list[i][j][5]] =int( scores_list[i][j][3])
                chart_result[scores_list[i][j][5]] = 1
    chart_data=chart_category.keys()
    for key in chart_data:
        temp_list.append(key)
        temp_list_final.append(chart_category[key]/chart_result[key])

    chart_dq_data['tables']=table_name
    chart_dq_data['columns']=column_name
    chart_dq_data['categories']=categories
    chart_dq_data['results']=results
    chart_dq_data_file_path = ss.chart_dq_data_file_path(request)
    with open(chart_dq_data_file_path,'w') as file:
        json.dump(chart_dq_data,file)
    context['chart_dq_data']= chart_dq_data

    chart_final_result['category']=(temp_list)
    chart_final_result['result']=(temp_list_final)
    last_dq_file_path = ss.dq_charts_last_file_path(request)
    with open(last_dq_file_path, 'w') as outfile:
        json.dump(chart_final_result, outfile)
    context["chart_data_dq"] = chart_final_result


    # f.write(str(chart_final_result))
    # f.close()

    context["length"] = len(scores_list)
    context["date_exec"] = execution_date
    context["Tbatches"] = batches_count
    context["Rbatches"] = len(batch_ids)

    # return  HttpResponse(scores_list)
    return render(request, 'DQ_results.html', context)


def source_actions(request):
    action = request.POST['action']
    if action == "Delete":
        source_id = request.POST['saved_source_data']
        delete_query = 'DELETE FROM public."D_A_T_user_source" WHERE ID = ' + str(source_id) + ';'
        connection = psycopg2.connect(user=dbUsername,
                                      password=dbPassword,
                                      host=dbHost,
                                      database=dbName)
        cursor = connection.cursor()
        cursor.execute(delete_query)
        connection.commit()
    elif action == "Connect":
        read_template_file_path = ss.read_template_path_file(request)
        with open(read_template_file_path, "w") as outfile:
            outfile.write("0")
            outfile.close()
        user_id = ss.user_session(request)


        data = str(request.POST['saved_source_data']).split(',', 7)
        connection_name = data[0]
        source_name=data[6]
        ss.source_log(user_id, source_name)
        ss.connection_log(user_id,connection_name)

        db_name = data[1].strip()
        source_tables = data[7].replace('[','').replace(']','').split(',')
        source_tables = [x.strip().lower() for x in source_tables]
        context = {}
        input_data = {}
        db_type = data[2].strip()
        ip = data[5].strip()
        username = data[3].strip()
        password = data[4].strip()

        input_data['db_type'] = db_type
        input_data['ip'] = ip
        input_data['username'] = username
        input_data['password'] = password
        user_input_file_path = ss.user_input_path(request)
        file1 = open(user_input_file_path, "w")
        for key in input_data:
            file1.write(str(key) + " : " + str(input_data[key]) + "\n")
        context = {}
        db_selected = db_name
        connect = ""
        table_list = []
        db_file_path = ss.db_path(request)
        file1 = open(db_file_path, "w")
        file1.write(db_selected)

        if db_type == "1":
            connect = db.myssql_connection(ip, username, password)
            query = "SELECT table_name FROM information_schema.tables where table_schema  = '" + db_selected + "';"
            df = pd.read_sql(query, connect)
            for x in range(0, len(df)):
                table_list.append(str(df.loc[x, 'table_name']).lower().strip())
            for item in source_tables:
                if item not in table_list:
                    source_tables.remove(item)
            context['tables'] = source_tables

        elif db_type == "5":
            connect = db.postgres_connection(ip, username, password)
            query = "SELECT table_name FROM information_schema.tables where table_schema  = '" + db_selected + "';"
            df = pd.read_sql(query, connect)
            for x in range(0, len(df)):
                table_list.append(str(df.loc[x, 'TableName']).lower().strip())
            for item in source_tables:
                if item not in table_list:
                    source_tables.remove(item)
            context['tables'] = source_tables

        elif db_type == "2":
            with teradatasql.connect(host=ip, user=username, password=password, encryptdata=True) as connect:
                query = "select TableName from dbc.tables where databasename = '" + db_selected + "';"
                df = pd.read_sql(query, connect)
                for x in range(0, len(df)):
                    table_list.append(str(df.loc[x, 'TableName']).lower().strip())
                for item in source_tables:
                    if item not in table_list:
                        source_tables.remove(item)
                context['tables'] = source_tables
                
        context["save_checkbox"] = 0
        # return HttpResponse (source_name)
        return render(request, 'tables.html', context)

    else: #~Edit source
        data = str(request.POST['saved_source_data']).split(',', 7)
        source_name = data[1].strip()
        source_id = data[0].strip()
        db_selected = data[2].strip()
        source_tables = data[7].replace('[', '').replace(']', '').split(',')
        source_tables = [x.strip().lower() for x in source_tables]
        context = {}
        input_data = {}
        db_type = data[3].strip()
        ip = data[6].strip()
        username = data[4].strip()
        password = data[5].strip()
        table_list = []

        if db_type == "1":
            connect = db.myssql_connection(ip, username, password)
            query = "SELECT table_name FROM information_schema.tables where table_schema  = '" + db_selected + "';"
            df = pd.read_sql(query, connect)
            for x in range(0, len(df)):
                table_ = str(df.loc[x, 'table_name']).strip()
                table_checkbox = []
                table_checkbox.append(table_)
                if table_.lower() in source_tables:
                    table_checkbox.append("checked")
                    table_checkbox.append("checked") # used to hide/unhide checkbox
                else:
                    table_checkbox.append(" ")
                    table_checkbox.append("unchecked") # used to hide/unhide checkbox

                table_list.append(table_checkbox)
            context['tables'] = table_list

        elif db_type == "5":
            connect = db.postgres_connection(ip, username, password)
            query = "SELECT table_name FROM information_schema.tables where table_schema  = '" + db_selected + "';"
            df = pd.read_sql(query, connect)
            for x in range(0, len(df)):
                table_ = str(df.loc[x, 'table_name']).strip()
                table_checkbox = []
                table_checkbox.append(table_)
                if table_.lower() in source_tables:
                    table_checkbox.append("checked")
                    table_checkbox.append("checked")  # used to hide/unhide checkbox
                else:
                    table_checkbox.append(" ")
                    table_checkbox.append("unchecked")  # used to hide/unhide checkbox
                table_list.append(table_checkbox)
            context['tables'] = table_list

        elif db_type == "2":
            with teradatasql.connect(host=ip, user=username, password=password, encryptdata=True) as connect:
                query = "select TableName from dbc.tables where databasename = '" + db_selected + "';"
                df = pd.read_sql(query, connect)
                for x in range(0, len(df)):
                    table_ = str(df.loc[x, 'TableName']).strip()
                    table_checkbox = []
                    table_checkbox.append(table_)
                    if table_.lower() in source_tables:
                        table_checkbox.append("checked")
                        table_checkbox.append("checked")  # used to hide/unhide checkbox
                    else:
                        table_checkbox.append(" ")
                        table_checkbox.append("unchecked")  # used to hide/unhide checkbox
                    table_list.append(table_checkbox)

        context['tables'] = table_list
        context['id'] = source_id
        context['name'] = source_name
        return render(request, "edit_source.html", context)

    return index(request)


def save_query(request):
    list = db.read_connections(request)
    db_type = list[0]
    ip = list[1]
    username = list[2]
    password = list[3]
    db_file_path = ss.db_path(request)
    file = open(db_file_path, "r+")
    check_description = request.POST.get('check_description')
    query = request.POST.get('query')
    check_name = request.POST.get('check_name')
    category = request.POST.get('category')
    # col_list = request.POST["selected"]
    checks = request.POST.getlist("check")
    context = {}
    context["check"] = checks
    # context["col_list"] = col_list
    context["check_description"] = check_description
    context["query"] = ""
    context["head"] = ""
    context["check_name"] = check_name
    context["category"] = category
    query_output = []
    query_head = []
    query_result = []
    db_file_path = ss.db_path(request)
    db_name = open(db_file_path, 'r').read()
    connection_file_path = ss.get_file_path(request, 'C')
    connection_name = open(connection_file_path, 'r').read()



    connection = psycopg2.connect(user=dbUsername, password=dbPassword, host=dbHost, database=dbName)
    cursor = connection.cursor()

    connection_exist_query = 'SELECT id FROM public."D_A_T_user_connection" WHERE database_type = ' + db_type + " and username = '" + username + "' and ip_address = '" + ip + "' and password = '" + password + "' and connection_name = '" + connection_name + "' and user_id_id = " + str(
        request.user.id) + ';'
    cursor.execute(connection_exist_query)
    connection.commit()
    result = cursor.fetchall()

    if len(result) == 0:  # connection doesn't exist
        create_connection_query = 'INSERT INTO public."D_A_T_user_connection"(database_type, username, password, ip_address, connection_name, user_id_id) ' \
                                  'VALUES (' + db_type + ", '" + username + "','" + password + "','" + ip + "','" + connection_name + "'," + str(
            request.user.id) + ');'

        cursor.execute(create_connection_query)
        connection.commit()
        cursor.execute(connection_exist_query)
        connection.commit()
        result = cursor.fetchall()

    connection_id = result[0][0]






    select_query = 'SELECT * FROM public."D_A_T_user_query" WHERE user_id_id = ' + str(
       request.user.id) + " AND query ='" + str(query) + "' AND category = '" + str(category) + "' AND check_name = '" +str( check_name )+ "' AND check_description = '" + str(check_description)+"';"



    if db_type == "2" and query != "":
        with teradatasql.connect(host=ip, user=username, password=password, encryptdata=True) as connect:
            try:
                query_without_newline = str(query).replace("\n", "")
                query_splitted = query_without_newline.split(";")
                engine = db.myssql_connection(ip, username, password)
                for single_query in query_splitted:

                    insert_query = 'INSERT INTO public."D_A_T_user_query"(user_id_id,query,category,check_name,check_description,query_date,connection_id,db_name) VALUES (' + str(
                        request.user.id) + ",'" + str(single_query) + "','" + str(category) + "','" + str(
                        check_name) + "','" + str(
                        check_description) + "','" + str(
                        datetime.datetime.now().strftime('%Y-%m-%d %H:%M')) + "','" + str(
                        connection_id) + "','" + str(db_name) + "' ) "

                    cursor = connection.cursor()
                    cursor.execute(select_query)
                    connection.commit()
                    result = cursor.fetchall()
                    if len(result) == 0:
                        cursor.execute(insert_query)
                        connection.commit()

                connection.close()
            except:
                return HttpResponse(
                    ' <head>  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css"><script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script><script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/js/bootstrap.min.js"></script><meta http-equiv = "refresh" content = "3; url = "table_of_checks.html" " /></head><div class="alert alert-danger">Query is incorrect</div>')

            # try:
            #     pd.read_sql(query, connect)
            #     cursor = connection.cursor()
            #     cursor.execute(select_query)
            #     connection.commit()
            #     result = cursor.fetchall()
            #     if len(result) == 0:
            #         cursor.execute(insert_query)
            #         connection.commit()
            #     connection.close()
            # except:
            #     return HttpResponse(
            #         ' <head>  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css"><script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script><script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/js/bootstrap.min.js"></script><meta http-equiv = "refresh" content = "3; url = "table_of_checks.html" " /></head><div class="alert alert-danger">Query is incorrect</div>')

    elif db_type == "1" and query != "":
        try :
            query_without_newline = str(query).replace("\n","")
            query_splitted = query_without_newline.split(";")
            engine = db.myssql_connection(ip, username, password)
            for single_query in query_splitted:

                insert_query = 'INSERT INTO public."D_A_T_user_query"(user_id_id,query,category,check_name,check_description,query_date,connection_id,db_name) VALUES (' + str(
                    request.user.id) + ",'" + str(single_query) + "','" + str(category) + "','" + str(
                    check_name) + "','" + str(
                    check_description) + "','" + str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M')) + "','" + str(
                    connection_id) + "','" + str(db_name) + "' ) "

                cursor = connection.cursor()
                cursor.execute(select_query)
                connection.commit()
                result = cursor.fetchall()
                if len(result) == 0:
                    cursor.execute(insert_query)
                    connection.commit()


            connection.close()
        except:
            return HttpResponse(' <head>  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css"><script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script><script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/js/bootstrap.min.js"></script><meta http-equiv = "refresh" content = "3; url = "table_of_checks.html" " /></head><div class="alert alert-danger">Query is incorrect</div>')

    # return HttpResponse(select_query)
    return create_dq_checks_template(request)

def dq_charts(request):
    chart_dict_dq = request.POST["chart_dict_dq"]
    chart_dq_data_file_path = ss.chart_dq_data_file_path(request)
    with open(chart_dq_data_file_path) as json_file:
        result = json.load(json_file)
    data_length = []

    for i in range(len(result['categories'])):
        if i%2==0:
            data_length.append(i)

    if len(data_length)%2==1:
        data_length.append(-1)

    # return HttpResponse(chart_dict_dq)
    return render(request, 'dq_charts.html', {
        'dict' : chart_dict_dq,
        'data_length' : data_length,

    })
def dq_charts_shot(request):
    chart_dict_dq = request.POST["chart_dict_dq"]
    last_dq_file_path = ss.dq_charts_last_file_path(request)
    with open(last_dq_file_path) as json_file:
        result = json.load(json_file)
    data_length = []

    for i in range(len(result['category'])):
        if i % 2 == 0:
            data_length.append(i)

    if len(data_length) % 2 == 1:
        data_length.append(-1)

    # return HttpResponse(chart_dict_dq)
    return render(request, 'index.html', {
        'dict_dq' : chart_dict_dq,
        'data_length_dq' : data_length,

    })

def save_edited_source(request):
    source_id = request.POST['id']
    source_tables = request.POST.getlist('tables')

    connection = psycopg2.connect(user=dbUsername, password=dbPassword, host=dbHost, database=dbName)
    cursor = connection.cursor()
    update_query = 'UPDATE public."D_A_T_user_source" SET tables = ' + "'" + str(source_tables).replace("'","").strip() + "' WHERE id = " + str(source_id) + ';'
    cursor.execute(update_query)
    connection.commit()
    return index(request)

def display_user_query(request):
    context={}
    list = db.read_connections(request)
    db_type = list[0]
    ip = list[1]
    username = list[2]
    password = list[3]
    db_file_path = ss.db_path(request)
    db_name = open(db_file_path, 'r').read()
    connection_file_path = ss.get_file_path(request, 'C')
    connection_name = open(connection_file_path, 'r').read()

    connection = psycopg2.connect(user=dbUsername, password=dbPassword, host=dbHost, database=dbName)
    cursor = connection.cursor()

    connection_exist_query = 'SELECT id FROM public."D_A_T_user_connection" WHERE database_type = ' + db_type + " and username = '" + username + "' and ip_address = '" + ip + "' and password = '" + password + "' and connection_name = '" + connection_name + "' and user_id_id = " + str(
        request.user.id) + ';'
    cursor.execute(connection_exist_query)
    connection.commit()
    result1 = cursor.fetchall()
    connection_id =result1[0][0]
    select_query = 'SELECT DISTINCT user_id_id,query,category,check_name,check_description,query_date,id,connection_id,db_name FROM public."D_A_T_user_query" WHERE user_id_id = ' + str(
        request.user.id) + " and connection_id = '" + str(connection_id) + "' and db_name = '" + str(db_name) + "';"

    cursor = connection.cursor()
    cursor.execute(select_query)
    connection.commit()
    result = cursor.fetchall()
    context['result'] = result

    # return HttpResponse(select_query)
    return render(request,"user_query.html",context)

def rur_edit_delete_query(request):
    action = request.POST['action']
    list = db.read_connections(request)
    db_type = list[0]
    ip = list[1]
    username = list[2]
    password = list[3]
    db_file_path = ss.db_path(request)
    file = open(db_file_path, "r+")
    query_head = []
    context={}

    if action == "Delete":
        query_id = request.POST['saved_query_data']
        delete_query = 'DELETE FROM public."D_A_T_user_query" WHERE ID = ' + str(query_id) + ';'
        connection = psycopg2.connect(user=dbUsername,
                                      password=dbPassword,
                                      host=dbHost,
                                      database=dbName)
        cursor = connection.cursor()
        cursor.execute(delete_query)
        connection.commit()


    if action == "Run":
        data = request.POST.getlist("query")
        check_names = []
        queries = []
        results = []
        for item in data:
            item_split = item.split(';;', 2)
            queries.append(item_split[1].strip())
            check_names.append(item_split[0].strip())

        if db_type == "2":
            with teradatasql.connect(host=ip, user=username, password=password, encryptdata=True) as connect:
                for i in range(0,len(queries)):
                    try:
                        sql_query_runner = pd.read_sql(queries[i], connect)
                        results.append(sql_query_runner.to_html())


                    except:
                        return HttpResponse(
                            ' <head>  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css"><script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script><script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/js/bootstrap.min.js"></script><meta http-equiv = "refresh" content = "3; url = "table_of_checks.html" " /></head><div class="alert alert-danger">Query is incorrect</div>')

        elif db_type == "1":
            engine = db.myssql_connection(ip, username, password)
            for query in queries:
                try:
                    sql_query_runner = pd.read_sql(query, engine)
                    results.append(sql_query_runner.to_html())

                except:
                    return HttpResponse(
                        ' <head>  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css"><script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script><script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/js/bootstrap.min.js"></script><meta http-equiv = "refresh" content = "3; url = "table_of_checks.html" " /></head><div class="alert alert-danger">Query is incorrect</div>')
        context['check_names'] = check_names
        context['results'] = results

        return render(request, 'user_query_result.html', context)

    if action == "Edit":
        data = request.POST['query']
        # return HttpResponse(data)
        query = data[0].strip()
        check_name =data[2].strip()
        category=data[1].strip()
        check_description=data[3].strip()
        query_id=data[4].strip()
        context['query_id']=query_id
        context['category'] = category
        context['query'] = query
        context['check_name'] = check_name
        context['check_description'] = check_description
        
        return render(request, "edit_query.html", context)

    return display_user_query(request)

def save_edited_query(request):
    query_id = request.POST['query_id']
    query= request.POST['query']
    category=request.POST.get('category')
    check_name=request.POST['check_name']
    check_description=request.POST['check_description']

    connection = psycopg2.connect(user=dbUsername, password=dbPassword, host=dbHost, database=dbName)
    cursor = connection.cursor()
    update_query = 'UPDATE public."D_A_T_user_query" SET query = ' + "'" + str(query).strip() + "'"+' , category = '+"'"+str(category).strip()+"' "+' , check_name = '+"'"+str(check_name).strip()+"' "+', check_description = '+"'"+str(check_description).strip()+"'  WHERE id = " + str(query_id) + ';'
    cursor.execute(update_query)
    connection.commit()
    return display_user_query(request)

def checks_document(request):
    return render (request,"checks_doc.html")

def support(request):
    return render(request,"support.html")

def support_controller(request):
    subject = request.POST["subject"]
    message = request.POST["message"]
    email = request.POST["email"]
    name = request.POST["name"]
    support_template_message = "Hi " + name + ", \n Thanks for contacting us. One of our development team will contact you as soon as possible. \n Best Regards, \n Teradata Modeling Team"

    flag = send_mail(subject,message, "dqtool2021@gmail.com",['dqtool2021@gmail.com'],fail_silently=False)
    flag_1 = send_mail(subject,support_template_message,email,[email],fail_silently=False)
    if flag == 1 and flag_1 == 1:
        return HttpResponse(
            ' <head>  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css"><script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script><script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/js/bootstrap.min.js"></script><meta http-equiv = "refresh" content = "5; url = home " /></head><div class="alert alert-success">Your Problem is successfully sent to our Technical support Team</div>')
    else:
        return HttpResponse(
            ' <head>  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css"><script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script><script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/js/bootstrap.min.js"></script><meta http-equiv = "refresh" content = "3; url = home " /></head><div class="alert alert-danger">Something went wrong please try again later/div>')


def data_filter(request):
    list = db.read_connections(request)
    db_type = list[0]
    ip = list[1]
    username = list[2]
    password = list[3]
    db_file_path = ss.db_path(request)
    database = open(db_file_path, "r").read()

    selected_tables = request.POST.get('selected').replace('[', '').replace(']', '').replace("'", '').split(',')
    select_query = "SELECT DISTINCT BATCH_ID AS BATCH_ID FROM " + database + ".TABLE"
    union_query = ""
    context = {}
    result = []

    for i in range(0, len(selected_tables)-1):
        union_query += select_query.replace('TABLE', selected_tables[i]) + ' UNION '
    # for last element in list
    union_query += select_query.replace('TABLE', selected_tables[-1])
    order_by_query = "SELECT BATCH_ID FROM(" + union_query + ")x ORDER BY BATCH_ID DESC;"
    # return HttpResponse (order_by_query)
    if db_type == "2":
        try:
            with teradatasql.connect(host=ip, user=username, password=password, encryptdata=True) as connect:
                df = pd.read_sql(order_by_query, connect)
                column_names = df.head(0)
                tmp = []
                for i in range(0, len(df)):
                    for y in column_names:
                        tmp.append(str(df.loc[i, y]))
                    result.append(tmp)
                    tmp = []
                sql_totla_batches_count =  "SELECT count( distinct BATCH_ID ) as count_batches FROM " + database+ "."+ selected_tables[0] + ";"
                batches_res = pd.read_sql(sql_totla_batches_count,connect)
                for iq in range(0,len(batches_res)):
                    value_count = batches_res.loc[iq,"count_batches"]
                    file_batches_count = ss.total_sessions(request)
                    with open(file_batches_count,"w") as file_writer_batches :
                        file_writer_batches.write(str(value_count))




        except Exception as e:
            print(e)
            context["no_filter"] = "1"

    elif db_type == "1":
        try:
            connection = mysql.connector.connect(
                    host=ip,
                    user=username,
                    password=password
            )

            mycursor = connection.cursor()
            mycursor.execute(order_by_query)
            df = mycursor.fetchone()
            column_names = df.head(0)
            tmp = []

            for i in range(0, len(df)):
                for y in column_names:
                    tmp.append(str(df.loc[i, y]))
                result.append(tmp)
                tmp = []
        except:
            context["no_filter"] = "1"

    context["result"] = result
    return render(request, "data_filter.html", context)

def UDI_Render(request):
    context = {}
    context['file'] = udi_retrieve.retrieve_config_file()
    return render(request, "UDI.html",context)
def UDI_TRIGGER(request):
    config_file = request.POST["config_file"]
    testing_value = request.POST.get("testing")
    UDI_value = request.POST.get("UDI")
    scripts_flag = ""
    if UDI_value == "1" and testing_value == "1":
        scripts_flag = "All"
    elif UDI_value == "1" and testing_value is None:
        scripts_flag = "UDI"
    elif  testing_value == "1" and UDI_value is None:
        scripts_flag = "Testing"
    elif UDI_value == "0" and testing_value == "0":
        scripts_flag = 0
    try:
        config_file_path = udi_retrieve.write_in_session(request, config_file)
        dics = udi_class.get_config_file_values(config_file_path)
        dics["scripts_flag"] = scripts_flag
        g = gs.GenerateScripts(None, dics)
        udi_class.generate_scripts_thread(request, config_file , dics)
        # return HttpResponse(dics["scripts_flag"])
        return HttpResponse(
            ' <head>  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css"><script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script><script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/js/bootstrap.min.js"></script><meta http-equiv = "refresh" content = "3; url = home " /></head><div class="alert alert-success">Scripts Generated Successfully</div>')
    except Exception as e:
        return HttpResponse(
            ' <head>  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css"><script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script><script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/js/bootstrap.min.js"></script><meta http-equiv = "refresh" content = "3; url = home " /></head><div class="alert alert-danger"> '+ e +'</div>')

    # return HttpResponse(dics["scripts_flag"])
    # return render(request)

def UDI_inputs(request):
    return render(request,"UDI_inputs.html")

def UDI_RENDERING_FROM_INPUTS(request):
    output_path = request.POST["output_path"]
    smx_path = request.POST["smx_path"]
    templates_path = request.POST["templates_path"]
    online_source_t = request.POST["online_source_t"]
    offline_source_t = request.POST["offline_source_t"]
    db_prefix = request.POST["db_prefix"]
    scripts_flag = request.POST["scripts_flag"]
    model_testing = request.POST["model_testing"]
    gcfr_ctl_Id = request.POST["gcfr_ctl_Id"]
    gcfr_stream_key = request.POST["gcfr_stream_key"]
    gcfr_system_name = request.POST["gcfr_system_name"]
    gcfr_stream_name = request.POST["gcfr_stream_name"]
    etl_process_table = request.POST["etl_process_table"]
    SOURCE_TABLES_LKP_table = request.POST["SOURCE_TABLES_LKP_table"]
    SOURCE_NAME_LKP_table = request.POST["SOURCE_NAME_LKP_table"]
    history_tbl = request.POST["history_tbl"]
    gcfr_bkey_process_type = request.POST["gcfr_bkey_process_type"]
    gcfr_snapshot_txf_process_type = request.POST["gcfr_snapshot_txf_process_type"]
    gcfr_insert_txf_process_type = request.POST["gcfr_insert_txf_process_type"]
    gcfr_others_txf_process_type = request.POST["gcfr_others_txf_process_type"]
    read_sheets_parallel = request.POST["read_sheets_parallel"]
    Data_mover_flag = request.POST["Data_mover_flag"]
    ip = request.POST["ip"]
    username = request.POST["username"]
    password = request.POST["password"]
    scripts_status = request.POST["scripts_status"]
    # return HttpResponse(scripts_status)
    # return HttpResponse(ip + username + password)
    testing_categories = ""
    source_name = ""
    core_table_name = ""



    if scripts_flag == "Testing":
        testing_categories = request.POST.getlist("catgs")

    source_name = request.POST["source_name"]
    core_table_name = request.POST["core_table_name"]


    config_file = udi_retrieve.write_the_config_file(output_path,smx_path,templates_path,source_name,core_table_name,online_source_t, offline_source_t,db_prefix,scripts_flag , gcfr_ctl_Id , gcfr_stream_key ,gcfr_system_name, gcfr_stream_name , etl_process_table ,SOURCE_TABLES_LKP_table,SOURCE_NAME_LKP_table,history_tbl,gcfr_bkey_process_type,gcfr_snapshot_txf_process_type,gcfr_insert_txf_process_type,gcfr_others_txf_process_type,read_sheets_parallel,Data_mover_flag,str(testing_categories),str(ip),str(username),str(password),str(scripts_status))
    # return HttpResponse(config_file)
    try:
        config_file_path = udi_retrieve.write_in_session(request, config_file)
        dics = udi_class.get_config_file_values(config_file_path)
        dics["scripts_flag"] = scripts_flag
        g = gs.GenerateScripts(None, dics )
        udi_class.generate_scripts_thread(request, config_file, dics)

        return HttpResponse(
            ' <head>  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css"><script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script><script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/js/bootstrap.min.js"></script><meta http-equiv = "refresh" content = "3; url = home " /></head><div class="alert alert-success">Scripts Generated Successfully</div>')
    except Exception as e:
        return HttpResponse(e)
        # return HttpResponse(
        #     ' <head>  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css"><script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script><script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/js/bootstrap.min.js"></script><meta http-equiv = "refresh" content = "3; url = home " /></head><div class="alert alert-danger"> ' + str(e) + '</div>')

def APIAutomation(request):
    return render(request, "APIAutomation.html")

def APIAutomationController(request):
    api_mapping_sheet_path = request.POST["output_path"]
    ip = request.POST["ip"]
    username = request.POST["username"]
    password = request.POST["password"]
    connect = teradatasql.connect(host=ip, user=username, password=password, encryptdata=True)
    api_automation.APIMain(connect,api_mapping_sheet_path)
    return HttpResponse("done")

def UDIScripts(request):
    return render(request,"UDIScripts.html")


def UDI_Scripts_Renderer(request):
    output_path = request.POST["output_path"]
    smx_path = request.POST["smx_path"]
    templates_path = request.POST["templates_path"]
    online_source_t = request.POST["online_source_t"]
    offline_source_t = request.POST["offline_source_t"]
    db_prefix = request.POST["db_prefix"]
    scripts_flag = request.POST["scripts_flag"]
    gcfr_ctl_Id = request.POST["gcfr_ctl_Id"]
    gcfr_stream_key = request.POST["gcfr_stream_key"]
    gcfr_system_name = request.POST["gcfr_system_name"]
    gcfr_stream_name = request.POST["gcfr_stream_name"]
    etl_process_table = request.POST["etl_process_table"]
    SOURCE_TABLES_LKP_table = request.POST["SOURCE_TABLES_LKP_table"]
    SOURCE_NAME_LKP_table = request.POST["SOURCE_NAME_LKP_table"]
    history_tbl = request.POST["history_tbl"]
    gcfr_bkey_process_type = request.POST["gcfr_bkey_process_type"]
    gcfr_snapshot_txf_process_type = request.POST["gcfr_snapshot_txf_process_type"]
    gcfr_insert_txf_process_type = request.POST["gcfr_insert_txf_process_type"]
    gcfr_others_txf_process_type = request.POST["gcfr_others_txf_process_type"]
    read_sheets_parallel = request.POST["read_sheets_parallel"]
    Data_mover_flag = request.POST["Data_mover_flag"]
    source_name = request.POST["source_name"]
    core_table_name = request.POST["core_table_name"]

    config_file = udi_retrieve.write_the_config_file(output_path, smx_path, templates_path, source_name,
                                                     core_table_name, online_source_t, offline_source_t, db_prefix,
                                                     scripts_flag, gcfr_ctl_Id, gcfr_stream_key, gcfr_system_name,
                                                     gcfr_stream_name, etl_process_table, SOURCE_TABLES_LKP_table,
                                                     SOURCE_NAME_LKP_table, history_tbl, gcfr_bkey_process_type,
                                                     gcfr_snapshot_txf_process_type, gcfr_insert_txf_process_type,
                                                     gcfr_others_txf_process_type, read_sheets_parallel,
                                                     Data_mover_flag, str(0), str(0), str(0),
                                                     str(0), str(0))
    config_file_path = udi_retrieve.write_in_session(request, config_file)
    dics = udi_class.get_config_file_values(config_file_path)
    dics["scripts_flag"] = scripts_flag
    g = gs.GenerateScripts(None, dics)
    udi_class.generate_scripts_thread(request, config_file, dics)

    return HttpResponse(config_file)






