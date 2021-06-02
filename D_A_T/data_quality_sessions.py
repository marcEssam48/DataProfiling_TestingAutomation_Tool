import D_A_T.sources_session as ss
import time
import psycopg2
from django.conf import settings
import D_A_T.test_cases_session as tcs

dbHost = settings.DATABASES['default']['HOST']
dbUsername = settings.DATABASES['default']['USER']
dbPassword = settings.DATABASES['default']['PASSWORD']
dbName = settings.DATABASES['default']['NAME']

def insert_in_dq_main_table(request):
    user_id_id = ss.user_session(request)
    sources_id_id = str(tcs.get_source_id(request)).replace("(","").replace(")","").replace(",","").replace("[","").replace("]","")
    timestamp = time.time()
    sql_insert = 'INSERT INTO public."D_A_T_data_quality"(date, source_id_id, user_id_id)VALUES ('
    insert_stmt = sql_insert + "' " +str(timestamp)+"  ' ,' " + str(sources_id_id) +"', '" +str(user_id_id)+"' )"
    connection = psycopg2.connect(user=dbUsername, password=dbPassword, host=dbHost, database=dbName)
    cursor = connection.cursor()
    cursor.execute(insert_stmt)
    connection.commit()

def data_quality_id():
    sels ='select id from  public."D_A_T_data_quality"'
    select_sql = sels + " order by id desc limit 1"
    connection = psycopg2.connect(user=dbUsername, password=dbPassword, host=dbHost, database=dbName)
    cursor = connection.cursor()
    cursor.execute(select_sql)
    connection.commit()
    result = cursor.fetchall()
    return result

def insert_dq_results(data_quality_results_list):
    data_quality = str(data_quality_id()).replace("(", "").replace(")", "").replace(",", "").replace("[", "").replace("]", "")
    counter =0
    for dq_single_result in data_quality_results_list:
        for ls in dq_single_result:

            table_name = str(ls[1])
            column_name = str(ls[2])
            result = str(ls[3])
            check_name = str(ls[4])
            category_name = str(ls[5])

            select_stm = 'INSERT INTO public."D_A_T_data_quality_results"( table_name, column_name, category_name, check_name, result, data_quality_id_id)VALUES ('
            param = select_stm + "'" + table_name + "' ,'" + column_name + "', '" + category_name + "' ,'" + check_name + "' ,'" + result + "' ,'" + str(
                data_quality) + "' )"
            connection = psycopg2.connect(user=dbUsername, password=dbPassword, host=dbHost, database=dbName)
            cursor = connection.cursor()
            cursor.execute(param)
            connection.commit()

            counter += 1





def main_insertion(request,data_quality_results_list):
    insert_in_dq_main_table(request)
    insert_dq_results(data_quality_results_list)

