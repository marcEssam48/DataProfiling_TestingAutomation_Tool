from D_A_T.UDI.read_smx_sheet.app_Lib import functions as funcs
from D_A_T.UDI.read_smx_sheet.app_Lib import TransformDDL
from D_A_T.UDI.read_smx_sheet.Logging_Decorator import Logging_decorator
from D_A_T.UDI.read_smx_sheet.Logging_Decorator import Logging_decorator
import  D_A_T.UDI.read_smx_sheet.testing_logging as test_logger


@Logging_decorator
def bmap_dup_desc_check(cf, source_output_path, table_mapping, core_tables, BMAP_VALUES,connect, script_connect_run):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    core_tables_look_ups = core_tables[core_tables['Is lookup'] == 'Y']
    core_tables_look_ups = core_tables_look_ups[core_tables_look_ups['Column name'].str.endswith(str('_DESC'))]
    count = 1
    lookup_tables_list = TransformDDL.get_src_lkp_tbls(table_mapping, core_tables)
    code_set_names = TransformDDL.get_code_set_names(BMAP_VALUES)
    scripts_status = str(cf.scripts_status).lower()
    for src in cf.source_names:
        for code_set_name in code_set_names:
            for table_name in lookup_tables_list:
                if table_name == code_set_name:
                    for core_table_index, core_table_row in core_tables_look_ups.iterrows():
                        if core_table_row['Table name'] == table_name:
                            call_line1 = "SEL " + core_table_row['Column name'] + " FROM " + cf.base_DB + "." + table_name
                            call_line2 = " GROUP BY " + core_table_row['Column name'] + " HAVING COUNT(*)>1;"
                            call_line3 = ""
                            # if src == "Null":
                            #     call_line3 = ""
                            #     # print("hello")
                            # else:
                                # print("bmap_dup_check_desc_Test_Case_")
                                # call_line3 = " WHERE "+cf.base_DB + "." + table_name+".Process_Name LIKE'%" + src + "%'" + "\n\n\n"
                                # call_line3 = " WHERE " +cf.base_DB + "." + table_name+ ".Process_Name in (select Process_Name from gdevp1t_gcfr.etl_process where source_name = '" +src+ "') "
                            bmap_check_name_line = "---bmap_dup_check_desc_Test_Case_" + str(count) + "---"

                            call_exp = bmap_check_name_line + "\n" + call_line1 + call_line3 + call_line2
                            # print(call_exp)

                            script_to_be_sent = call_line1 + call_line3 +call_line2.replace(";","").replace("\n","")
                            if scripts_status == "automated" or scripts_status == "all":
                                test_logger.insert_testing_logs(script_to_be_sent, table_name,
                                                                "", "BMAP TESTS ", bmap_check_name_line, src,
                                                                "BMAP_DUP_DESC_TEST",
                                                                cf.base_DB,cf,connect, script_connect_run)


                            if scripts_status == "generated" or scripts_status == "all":
                                f.write(call_exp)
                            count = count + 1
    f.close()
