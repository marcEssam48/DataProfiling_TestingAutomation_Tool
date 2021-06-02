import D_A_T.sources_session as ss
import time
import psycopg2
from django.conf import settings
import D_A_T.test_cases_session as tcs

dbHost = settings.DATABASES['default']['HOST']
dbUsername = settings.DATABASES['default']['USER']
dbPassword = settings.DATABASES['default']['PASSWORD']
dbName = settings.DATABASES['default']['NAME']

def insert_in_stats_main_table(request):
    user_id_id = ss.user_session(request)
    sources_id_id = str(tcs.get_source_id(request)).replace("(", "").replace(")", "").replace(",", "").replace("[","").replace( "]", "")
    timestamp = time.time()
    sql_insert = 'INSERT INTO public."D_A_T_statistics"(date, source_id_id, user_id_id)VALUES ('
    insert_stmt = sql_insert + "' " + str(timestamp) + "  ' ,' " + str(sources_id_id) + "', '" + str(user_id_id) + "' )"
    connection = psycopg2.connect(user=dbUsername, password=dbPassword, host=dbHost, database=dbName)
    cursor = connection.cursor()
    cursor.execute(insert_stmt)
    connection.commit()

def stats_id():
    sels = 'select id from  public."D_A_T_statistics"'
    select_sql = sels + " order by id desc limit 1"
    connection = psycopg2.connect(user=dbUsername, password=dbPassword, host=dbHost, database=dbName)
    cursor = connection.cursor()
    cursor.execute(select_sql)
    connection.commit()
    result = cursor.fetchall()
    return result

def insert_in_stats_nums(result_list):
    stats = str(stats_id()).replace("(", "").replace(")", "").replace(",", "").replace("[", "").replace("]", "")
    for single_list in result_list:
        table_name = single_list[0]
        no_of_cols = single_list[1]
        no_of_rows =single_list[2]

        ins_stmt = 'INSERT INTO public."D_A_T_statistics_results"( table_name, no_of_columns, no_of_rows, statistics_id_id)VALUES ('
        param = ins_stmt + " ' " +table_name+" ',  ' " +str(no_of_cols)+ " ' , ' " +str(no_of_rows)+ " ' , '" + str(stats) + "' )"
        connection = psycopg2.connect(user=dbUsername, password=dbPassword, host=dbHost, database=dbName)
        cursor = connection.cursor()
        cursor.execute(param)
        connection.commit()

def main_insertion_for_table(request,result_list):
    insert_in_stats_main_table(request)
    insert_in_stats_nums(result_list)

def insert_in_more_stats(big_list):
    stats = str(stats_id()).replace("(", "").replace(")", "").replace(",", "").replace("[", "").replace("]", "")
    for single_list in big_list:
        table_name = single_list[0]
        column_name = single_list[1]
        distinct_value = single_list[2]
        null_values = single_list[3]
        null_percentage = single_list[4]
        minimum_value = single_list[5]
        maximum_value = single_list[6]

        ins_stmt = 'INSERT INTO public."D_A_T_more_statistics_results"(table_name, column_name, null_percentage, distinct_values, null_values, maximum, minimum, statistics_id_id)VALUES ('
        param= ins_stmt + " ' " + table_name + "' ,  ' " + column_name + "' , '" +null_percentage+ "' , '"+distinct_value+"' , '" +null_values+ "' , '"+maximum_value+"', '"+minimum_value+"' , '"+str(stats)+"' )"
        connection = psycopg2.connect(user=dbUsername, password=dbPassword, host=dbHost, database=dbName)
        cursor = connection.cursor()
        cursor.execute(param)
        connection.commit()
def main_insertion_in_more_stats(big_list):
    insert_in_more_stats(big_list)




