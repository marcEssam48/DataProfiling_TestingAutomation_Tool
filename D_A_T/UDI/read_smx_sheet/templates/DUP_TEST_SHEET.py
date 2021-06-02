from D_A_T.UDI.read_smx_sheet.app_Lib import functions as funcs
from D_A_T.UDI.read_smx_sheet.app_Lib import TransformDDL
from D_A_T.UDI.read_smx_sheet.Logging_Decorator import Logging_decorator
import  D_A_T.UDI.read_smx_sheet.testing_logging as test_logger

@Logging_decorator
def duplicates_check(cf, source_output_path, table_mapping, core_tables,connect, script_connect_run):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    count = 0
    core_tables_list= TransformDDL.get_src_core_tbls(table_mapping)
    scripts_status = str(cf.scripts_status).lower()
    for src in cf.source_names:
        for table_name in core_tables_list:
            count = count+1
            core_table_pks = TransformDDL.get_trgt_pk(core_tables, table_name)
            dup_line = "---DUP_Test_Case_" + str(count) + "---"+'\n'
            dup_test_case_exp_line1 = 'SEL ' + core_table_pks + ' FROM ' + cf.base_DB + '.' + table_name
            dup_test_case_exp_line2 =  " GROUP BY "+ core_table_pks + ' HAVING COUNT(*)>1 ;'+'\n'+'\n'
            dup_test_case_exp_line3 = ""
            if src == "Null":
                dup_test_case_exp_line3 = ""
            else:
                dup_test_case_exp_line3 = ' WHERE' + cf.base_DB + '.' + table_name + ".PROCESS_NAME LIKE '%" + src + "%' "


            script_to_be_sent = dup_test_case_exp_line1 + dup_test_case_exp_line3 + dup_test_case_exp_line2.replace("\n", "")
            if scripts_status == "automated" or scripts_status == "all":
                test_logger.insert_testing_logs(script_to_be_sent, table_name,
                                                "", "DUPLICATE TEST", dup_line, src,
                                                "DUPLICATE_TEST",
                                                cf.base_DB,cf,connect, script_connect_run)
            if scripts_status == "generated" or scripts_status == "all":
                f.write(dup_line + dup_test_case_exp_line1 + dup_test_case_exp_line3 + dup_test_case_exp_line2 )
    f.close()
