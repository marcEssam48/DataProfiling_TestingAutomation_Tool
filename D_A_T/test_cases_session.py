import D_A_T.sources_session as ss
import time
import psycopg2
from django.conf import settings

dbHost = settings.DATABASES['default']['HOST']
dbUsername = settings.DATABASES['default']['USER']
dbPassword = settings.DATABASES['default']['PASSWORD']
dbName = settings.DATABASES['default']['NAME']

def get_source_id(request):
    source_file_path = ss.get_file_path(request,'S')
    source_name = ''
    with open(source_file_path) as file :
        source_name = file.read()
        file.close()
    sel = 'SELECT id FROM public."D_A_T_user_source" WHERE source = '
    select_query =  sel + "'" + str(source_name) + "'"
    connection = psycopg2.connect(user=dbUsername,
                                  password=dbPassword,
                                  host=dbHost,
                                  database=dbName)
    cursor = connection.cursor()
    cursor.execute(select_query)
    connection.commit()
    result = cursor.fetchall()
    return result



def get_connection_id(request):
    Connection_file_path = ss.get_file_path(request, 'C')

def test_case_insertion(request):
    user_id = ss.user_session(request)
    source_id = str(get_source_id(request)).replace("(","").replace(")","").replace(",","").replace("[","").replace("]","")
    timestamp = time.time()

    ins = 'Insert into public."D_A_T_test_cases"(date,source_id_id,user_id_id)'
    insert_sql = ins + " values ('"+str(timestamp)+"',"+str(source_id)+","+str(user_id)+")"
    connection = psycopg2.connect(user=dbUsername, password=dbPassword, host=dbHost, database=dbName)
    cursor = connection.cursor()
    cursor.execute(insert_sql)
    connection.commit()

def test_case_id():
    sels ='select id from  public."D_A_T_test_cases"'
    select_sql = sels + " order by id desc limit 1"
    connection = psycopg2.connect(user=dbUsername, password=dbPassword, host=dbHost, database=dbName)
    cursor = connection.cursor()
    cursor.execute(select_sql)
    connection.commit()
    result = cursor.fetchall()
    return result


def results_insertion(big_list):
    test_case = str(test_case_id()).replace("(","").replace(")","").replace(",","").replace("[","").replace("]","")
    for list in big_list:
        check_name = str(list[0])
        table_name = str(list[1])
        result = str(list[2]).replace("(","").replace(")","").replace(",","").replace("[","").replace("]","")
        status = str(list[3])

        inst = 'Insert into public."D_A_T_test_case_result"( check_name, table_name, result, status , test_case_id_id)  VALUES ('
        insert_stmt = inst + " '"+check_name+"', '"+table_name+"', '"+str(result).replace("'","").replace("\"","")+"', '"+status+"',"+str(test_case)+")"

        connection = psycopg2.connect(user=dbUsername, password=dbPassword, host=dbHost, database=dbName)
        cursor = connection.cursor()
        cursor.execute(insert_stmt)
        connection.commit()

def main_insertion(request,big_list):
    test_case_insertion(request)
    results_insertion(big_list)


