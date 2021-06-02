from D_A_T.app_Lib import functions as funcs
from D_A_T.Logging_Decorator import Logging_decorator
import  D_A_T.testing_logging as test_logger

@Logging_decorator
def compare_views_check(cf, source_output_path,core_Table_mapping,comparison_flag,connect, script_connect_run):
    count = 1
    file_name = comparison_flag
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    test_exp = ''
    parent_table = ""
    child_table = ""
    scripts_status = str(cf.scripts_status).lower()
    for src in cf.source_names:
        for table_mapping_index,table_mapping_row in core_Table_mapping.iterrows():
            if comparison_flag == 'FROM_TESTING_TO_UDI':
                test_exp = 'SEL * FROM '+cf.INPUT_VIEW_DB+'.TXF_CORE_'+table_mapping_row['Mapping name']+'_IN_TESTING'+'\n'
                test_exp = test_exp + 'MINUS \n' + 'SEL * FROM '+cf.INPUT_VIEW_DB+'.TXF_CORE_'+table_mapping_row['Mapping name']+'_IN'
                parent_table = 'TXF_CORE_'+table_mapping_row['Mapping name']+'_IN_TESTING'
                child_table = 'TXF_CORE_'+table_mapping_row['Mapping name']+'_IN'

            elif comparison_flag == 'FROM_UDI_TO_TESTING':
                test_exp = 'SEL * FROM ' + cf.INPUT_VIEW_DB + '.TXF_CORE_' + table_mapping_row['Mapping name'] + '_IN' + '\n'
                test_exp = test_exp + 'MINUS \n' + 'SEL * FROM ' + cf.INPUT_VIEW_DB + '.TXF_CORE_' + table_mapping_row['Mapping name'] + '_IN_TESTING'
                child_table = 'TXF_CORE_' + table_mapping_row['Mapping name'] + '_IN_TESTING'
                parent_table = 'TXF_CORE_' + table_mapping_row['Mapping name'] + '_IN'

            input_view_check_line = "---Input_view_check_test_Case_" + str(count) + "---"
            call_exp = input_view_check_line+'\n'+test_exp+'\n\n\n'
            script_name = "TESTING_INPUT_VIEW" + "_" + comparison_flag
            # print(test_exp)
            script_to_be_sent = test_exp+";"
            if scripts_status == "automated" or scripts_status == "all":
                test_logger.insert_testing_logs(script_to_be_sent, parent_table,
                                                child_table, "TEST_INPUT_VIEWS", input_view_check_line, src, script_name,
                                                cf.INPUT_VIEW_DB,cf,connect, script_connect_run)

            if scripts_status == "generated" or scripts_status == "all":
                f.write(call_exp)
            count = count + 1
    f.close()
