import teradatasql
import pymysql
from sqlalchemy import create_engine
import pg8000
import D_A_T.sources_session as ss

def myssql_connection(ip,username,password):
    e = create_engine("mysql+pymysql://"+username+":"+password+"@"+ip+"")
    return e

def sql_server_connection(ip,username,password):
    e = create_engine("mssql+pymssql://"+username+":"+password+"@"+ip+"")
    return e

def teradata_connection(ip,username,password):
    connect =  teradatasql.connect(host=ip, user=username, password=password)
    return connect

def oracle_connection(ip,username,password):
    connect = create_engine("oracle+cx_oracle://"+username+":"+password+"@"+ip+"")
    return connect

def postgres_connection(ip,username,password):
    engine = create_engine("postgresql+pg8000://"+username+":"+password+"@"+ip+"")
    return engine


def read_connections(request):
    list = []
    user_input_path = ss.user_input_path(request)
    file = open(user_input_path, "r+")
    for x in file.readlines() :
        list.append(x.replace("\n","").split(" : " , 2)[1])
    return list



