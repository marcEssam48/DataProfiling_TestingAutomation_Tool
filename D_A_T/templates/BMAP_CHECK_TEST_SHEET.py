from D_A_T.app_Lib import functions as funcs
from D_A_T.app_Lib import TransformDDL
from D_A_T.Logging_Decorator import Logging_decorator
import  D_A_T.testing_logging as test_logger


@Logging_decorator
def bmap_check(cf, source_output_path, table_mapping, core_tables,BMAP_VALUES,connect, script_connect_run):
    # file_name = ""
    # # f = ""
    # if cf.scripts_status == "generated" or cf.scripts_status == "all":
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    scripts_status = str(cf.scripts_status).lower()
    # print(scripts_status)

    core_tables_look_ups = core_tables[core_tables['Is lookup'] == 'Y']
    core_tables_look_ups = core_tables_look_ups[core_tables_look_ups['Column name'].str.endswith(str('_CD'))]
    core_tables = core_tables[core_tables['Is lookup'] != 'Y']
    count = 1
    core_tables_list = TransformDDL.get_src_core_tbls(table_mapping)
    code_set_names = TransformDDL.get_code_set_names(BMAP_VALUES)
    for src in cf.source_names:
        for table_name in core_tables_list:
            for core_table_index, core_table_row in core_tables[(core_tables['Table name'] == table_name)].iterrows():
                for code_set_name in code_set_names:
                    for core_tables_look_ups_index, core_tables_look_ups_row in core_tables_look_ups.iterrows():
                        if str(core_tables_look_ups_row['Table name'])==code_set_name:
                            if core_tables_look_ups_row['Column name'] == core_table_row['Column name'] and core_table_row['PK'] == 'Y':
                                target_model_table = str(core_table_row['Table name'])
                                target_model_column = str(funcs.get_model_col(core_tables, target_model_table))
                                lookup_table_name = str(core_tables_look_ups_row['Table name'])
                                target_column_key = str(core_tables_look_ups_row['Column name'])

                                # call_line1 = "SEL " + cf.base_DB + "." + target_model_table + "."+target_column_key
                                # call_line2 = ","+cf.base_DB+ "." + target_model_table + "."+target_model_column +'\n'
                                # call_line3 = " FROM " + cf.base_DB + "." + target_model_table +  " LEFT JOIN " + cf.base_DB+ "." + lookup_table_name + '\n'
                                # call_line4 = " ON " + cf.base_DB + "." + target_model_table + "."+target_column_key + '=' + cf.base_DB+ "." + lookup_table_name + "."+target_column_key + '\n'
                                # call_line5 = " WHERE " + cf.base_DB+ "." + lookup_table_name + "."+target_column_key + " IS NULL;\n\n\n"
                                # bmap_check_name_line = "---bmap_check_Test_Case_" + str(count) + "---"

                                call_line1 = "SEL child_table"  "." + target_column_key
                                call_line2 = ", child_table." + target_model_column + '\n'
                                call_line3 = " FROM " + cf.base_DB + "." + target_model_table + " child_table LEFT JOIN " + cf.base_DB + "." + lookup_table_name + '\n'
                                call_line4 = " ON child_table." + target_column_key + '=' + cf.base_DB + "." + lookup_table_name + "." + target_column_key + '\n'
                                call_line5 = " WHERE " + cf.base_DB + "." + lookup_table_name + "." + target_column_key + " IS NULL"
                                call_line6 = ""
                                if src == "Null":
                                    call_line6 = ""
                                else:
                                  call_line6 = " AND child_table.Process_Name LIKE'%"+src+"%'"+";\n\n\n"
                                  # call_line6 = " AND child_table.Process_Name in (select Process_Name from gdevp1t_gcfr.etl_process where source_name = '" +src+ "')" + ";\n\n\n"
                                bmap_check_name_line = "---bmap_check_Test_Case_" + str(count) + "---"

                                call_exp = bmap_check_name_line + "\n" + call_line1 + call_line2 + call_line3 + call_line4 + call_line5 + call_line6
                                # print(call_exp)
                                script_to_be_sent = call_line1 + call_line2.replace("\n", "") +  call_line3.replace("\n", "") + call_line4.replace("\n", "") + call_line5.replace("\n", "").replace("\n", "") + call_line6.replace("\n","")
                                if scripts_status == "automated" or scripts_status == "all":
                                    # print("automated")
                                    test_logger.insert_testing_logs(script_to_be_sent, target_model_table,
                                                                    lookup_table_name, "BMAP TESTS ", bmap_check_name_line, src,
                                                                    "BMAP_CHECK_TEST",
                                                                    cf.base_DB,cf,connect, script_connect_run)


                                if scripts_status == "generated" or scripts_status == "all":
                                    f.write(call_exp)
                                count = count + 1
    f.close()
