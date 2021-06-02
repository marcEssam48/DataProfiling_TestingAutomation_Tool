from D_A_T.app_Lib import functions as funcs
from D_A_T.app_Lib import TransformDDL
from D_A_T.Logging_Decorator import Logging_decorator
import  D_A_T.testing_logging as test_logger

@Logging_decorator
def bmap_unmatched_values_check(cf, source_output_path, table_mapping, core_tables,BMAP,BMAP_VALUES,connect, script_connect_run):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    core_tables_look_ups = core_tables[core_tables['Is lookup'] == 'Y']
    count = 1
    lookup_tables_list = TransformDDL.get_src_lkp_tbls(table_mapping, core_tables)
    CD_column = ''
    CD_SET_ID_val = ''
    count = 0
    code_set_names = TransformDDL.get_code_set_names(BMAP_VALUES)
    scripts_status = str(cf.scripts_status).lower()
    for src in cf.source_names:
        for code_set_name in code_set_names:
            for table_name in lookup_tables_list:
                if table_name == code_set_name:
                    for core_table_index, core_table_row in core_tables_look_ups.iterrows():
                        if core_table_row['Table name'] == table_name:
                            if str(core_table_row['Column name']).endswith(str('_CD')):
                                CD_column = core_table_row['Column name']
                        for bmap_table_index,bmap_table_row in BMAP.iterrows():
                            if bmap_table_row['Code set name'] == table_name:
                                CD_SET_ID_val = str(bmap_table_row['Code set ID'])
                    if CD_column != '' and CD_SET_ID_val != '':
                        bmap_check_name_line = "---bmap_unmatched_Test_Case_" + str(count) + "---"
                        call_line1 = "SEL COALESCE(EDW_CODE,'NOT IN BMAP TABLE BUT IN BASE TABLE')AS EDW_CODE,\n"
                        call_line2 = "COALESCE(" + CD_column +",'NOT IN BASE TABLE BUT IN BMAP TABLE')AS BASE_CODE\n"
                        call_line3 = " FROM "+cf.UTLFW_v+".BMAP_STANDARD_MAP FULL OUTER JOIN "+cf.base_DB+'.'+table_name+'\n'
                        call_line4 = "ON "+cf.UTLFW_v+".BMAP_STANDARD_MAP.EDW_CODE = "+cf.base_DB+'.'+table_name+'.'+CD_column+'\n'
                        call_line5 = "WHERE EDW_CODE IS NULL OR "+CD_column+" IS NULL AND CODE_SET_ID = "+CD_SET_ID_val+'\n'
                        call_line6 = ""
                        # if src == "Null":
                        #     call_line6 = ""
                        # else:
                            # print("bmap_unmatched_Test_Case")
                            # call_line6 = " AND "+cf.base_DB+'.'+table_name+".Process_Name LIKE'%" + src + "%'" + ";\n\n\n"
                            # call_line6 = " AND "+cf.base_DB+'.'+table_name+".Process_Name in (select Process_Name from gdevp1t_gcfr.etl_process where source_name = '" +src+ "');"

                        call_exp = bmap_check_name_line + "\n" + call_line1 + call_line2+call_line3+call_line4+call_line5+call_line6

                        DB_PARAM =  cf.UTLFW_v + " , " + cf.base_DB

                        script_to_be_sent = call_line1 + call_line2 + call_line3 + call_line4 + call_line5 + call_line6.replace('\n',"")
                        if scripts_status == "automated" or scripts_status == "all":
                            test_logger.insert_testing_logs(script_to_be_sent, "BMAP_STANDARD_MAP",
                                                            table_name, "BMAP TEST", bmap_check_name_line, src, "BMAP_UNMATCHED_TEST",
                                                            DB_PARAM,cf,connect, script_connect_run)

                        if scripts_status == "generated" or scripts_status == "all":
                            f.write(call_exp)
                        count = count + 1
    f.close()
