from D_A_T.app_Lib import functions as funcs
from D_A_T.app_Lib import TransformDDL
from D_A_T.Logging_Decorator import Logging_decorator
import  D_A_T.testing_logging as test_logger

@Logging_decorator
def bmap_null_check(cf, source_output_path, table_mapping, core_tables,BMAP_values,connect, script_connect_run):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    core_tables_look_ups = core_tables[core_tables['Is lookup'] == 'Y']
    count = 1
    lookup_tables_list = TransformDDL.get_src_lkp_tbls(table_mapping, core_tables)
    code_set_names = TransformDDL.get_code_set_names(BMAP_values)
    scripts_status = str(cf.scripts_status).lower()
    for src in cf.source_names:
        for code_set_name in code_set_names:
            for table_name in lookup_tables_list:
                if table_name==code_set_name:
                    CD_column = ''
                    DESC_column = ''
                    for core_table_index, core_table_row in core_tables_look_ups.iterrows():
                        if core_table_row['Table name'] == table_name:
                            if str(core_table_row['Column name']).endswith(str('_CD')) and core_table_row['PK'] == 'Y':
                                CD_column = core_table_row['Column name']
                            if str(core_table_row['Column name']).endswith(str('_DESC')):
                                DESC_column = core_table_row['Column name']
                    bmap_check_name_line = "---bmap_null_check_Test_Case_" + str(count) + "---"
                    call_line1 = "SEL * FROM " + cf.base_DB + "." + table_name
                    call_line2 = " WHERE " + CD_column + " IS NULL" + " OR " + DESC_column + " IS NULL"
                    call_line3 = ""
                    # if src == "Null":
                    #     call_line3 = ""
                    # else:
                        # call_line3 = " AND "+cf.base_DB + "." + table_name+".Process_Name LIKE'%" + src + "%'" + ";\n\n\n"
                        # call_line3 = " AND "+cf.base_DB + "." + table_name+".Process_Name in (select Process_Name from gdevp1t_gcfr.etl_process where source_name = '" +src+ "') "
                    call_exp = bmap_check_name_line + "\n" + call_line1 + call_line2 + call_line3

                    script_to_be_sent = call_line1 + call_line2.replace("\n", "") + call_line3
                    if scripts_status == "automated" or scripts_status == "all":
                        test_logger.insert_testing_logs(script_to_be_sent, table_name,
                                                        "", "BMAP TESTS ", bmap_check_name_line, src,
                                                        "BMAP_NULL_TEST",
                                                        cf.base_DB,cf,connect, script_connect_run)

                    if scripts_status == "generated" or scripts_status == "all":
                        f.write(call_exp)
                    count = count + 1
    f.close()
