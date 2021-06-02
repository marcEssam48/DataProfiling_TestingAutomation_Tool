import pandas as pd
import teradatasql
import pytz
import datetime

def insert_testing_logs(script , parent_table_name , child_table_name ,test_case_category ,test_case_name , src , test_case_category_type , db_name,cf,connect, script_connect_run):
    execution_date = '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
    # pacific = pytz.timezone('Etc/GMT+2')
    # execution_date = pacific.localize(execution_date)

    result = pd.read_sql(script, script_connect_run)
    length = str(len(result))
    DATABASE = str(db_name).split(" ,")

    status = ""
    dwh_type = "CITIZEN"
    if test_case_category_type != "STG_ACCEPTED_COUNT" or test_case_category_type != "STG_ALL_COUNT":

        if length == "0":
            status = "Passed"
        else :
            status = "Failed"

    else:
        try:
            diff = str(result.loc[0,"DIFF"])
            if diff == "0":
                status = "Passed"
                length = "0"
            else:
                status = "Failed"
                length = diff
        except:
            try:
                diff = str(result.loc[0, "COUNT_DIFF"])
                print(diff)
                if diff == "0":
                    status = "Passed"
                    length = "0"
                else:
                    status = "Failed"
                    length = diff
            except Exception as e:
                print(e)

    try:
        # print("tryyyyy")
        select_total_row_count = "select count(*) as row_count_total from "+ DATABASE[0]+ "." + parent_table_name
        # # print(select_total_row_count)
        # + " where process_name in (select process_name from GPROD1T_GCFR.ETL_PROCESS WHERE SOURCE_NAME = '" + src + "')"
        count_result = pd.read_sql(select_total_row_count,script_connect_run)
        total_count = str(count_result.loc[0,"row_count_total"])
        # total_count = "0"

        insert_script = 'insert into GDEV1T_gcfr.UDI_TESTING_SCRIPTS_RESULT_STATUS (TEST_CATEGORY_NAME, TEST_CATEGORY_SUBTYPE_NAME, PARENT_TABLE , CHILD_TABLE  , TEST_RUN_STATUS , TEST_RUN_DATE  , SOURCE_NAME , DB_NAME , TEST_FAILED_ROW_COUNT , DWH_TYPE , TEST_TOTAL_ROW_COUNT)'
        insert_script+= "values('" + str(test_case_category) + "','" +str(test_case_category_type) + "','" + str(parent_table_name) + "','" + str(child_table_name) + "','" + str(status) + "','"+ str(execution_date)+ "','"+str(src) + "','" + str(db_name)+ "','" + length + "' , '"+str(dwh_type)+  "','" +str(total_count)+ "');"


        # print(insert_script)
        result = pd.read_sql(insert_script, connect)

        SELECT_SCRIPT = "SELECT TOP 1 TEST_RUN_ID FROM GDEV1T_gcfr.UDI_TESTING_SCRIPTS_RESULT_STATUS where test_run_date = '"+str(execution_date)+ "' ORDER BY TEST_RUN_DATE DESC  ;"
        # print(SELECT_SCRIPT)
        RESULT_SELECT  = pd.read_sql(SELECT_SCRIPT, connect)

        test_run_id = str(RESULT_SELECT.loc[0,"TEST_RUN_ID"])

        insert_scripts_data = "insert into GDEV1T_gcfr.UDI_TESTING_SCRIPTS(TEST_RUN_ID , SCRIPT_BODY , SCRIPT_DESCRIPTION) values("
        insert_scripts_data += " '" + test_run_id + "' , '" + str(script).replace("'","''") + "' , '" + test_case_name + "');"

        result_scripts_data = pd.read_sql(insert_scripts_data , connect)
        print(test_case_category_type)
        print(script)
    except Exception as e:
        print(e)
def execute_query(script):
    try:
        connect = teradatasql.connect(host="172.19.3.150", user="Data_profiling_Tool", password="P@ssw0rd", encryptdata=True)
        result = pd.read_sql(script, connect)
    except Exception as e:
        print(e)


