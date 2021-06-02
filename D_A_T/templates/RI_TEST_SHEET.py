from D_A_T.app_Lib import functions as funcs
from D_A_T.app_Lib import TransformDDL
from D_A_T.Logging_Decorator import Logging_decorator
import  D_A_T.testing_logging as test_logger


@Logging_decorator
def ri_check(cf, source_output_path, table_mapping, RI_relations,connect, script_connect_run):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    count = 1
    core_tables_list = TransformDDL.get_src_core_tbls(table_mapping)
    scripts_status = str(cf.scripts_status).lower()
    for src in cf.source_names:
        for table_name in core_tables_list:
            for ri_table_index,ri_table_row in RI_relations.iterrows():
                RI_line = "---RI_Test_Case_" + str(count) + "---"
                if ri_table_row['CHILD TABLE'] == table_name :
                    call_line1 = "SELECT DISTINCT CHILD_TABLE"  '.' + ri_table_row['CHILD COLUMN']
                    call_line2 = " FROM " + cf.base_DB + '.' + ri_table_row['CHILD TABLE'] + " CHILD_TABLE LEFT JOIN " + cf.base_DB + '.' + ri_table_row['PARENT TABLE'] + " PARENT_TABLE "
                    call_line3 = " ON CHILD_TABLE." + ri_table_row['CHILD COLUMN']
                    call_line4 = " = PARENT_TABLE." + ri_table_row['PARENT COLUMN']
                    call_line5 = " WHERE PARENT_TABLE." + ri_table_row['PARENT COLUMN'] + " IS NULL"
                    call_line6 = " AND CHILD_TABLE." + ri_table_row['CHILD COLUMN'] + " IS NOT NULL"
                    call_line7 = " AND PARENT_TABLE.END_TS IS NOT NULL ;"
                    call_line8 = ""
                    if src == "Null":
                        call_line8 = ""
                    else:
                        call_line8 = " AND CHILD_TABLE.PROCESS_NAME LIKE '%"+src+"%';"
                        # call_line8 = " AND CHILD_TABLE.PROCESS_NAME IN (select Process_Name from gdevp1t_gcfr.etl_process where source_name = '" +src+ "')"

                    call_exp = RI_line+"\n"+call_line1+'\n'+call_line2 +'\n'+ call_line3+call_line4+'\n'+call_line5+'\n'+call_line8+'\n\n\n'
                    script_to_be_sent = call_line1 + call_line2 + call_line3 + call_line4 + call_line5 + call_line8

                    if scripts_status == "automated" or scripts_status == "all":
                        test_logger.insert_testing_logs(script_to_be_sent ,ri_table_row['PARENT TABLE']  , ri_table_row['CHILD TABLE'] , "RI TEst" , RI_line , src , "RI TEST" , cf.base_DB,cf,connect, script_connect_run)

                    if scripts_status == "generated" or scripts_status == "all":
                        f.write(call_exp)


                count = count + 1
    f.close()
