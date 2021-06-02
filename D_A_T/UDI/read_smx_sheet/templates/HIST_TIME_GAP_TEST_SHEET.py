from D_A_T.UDI.read_smx_sheet.app_Lib import functions as funcs
from D_A_T.UDI.read_smx_sheet.app_Lib import TransformDDL
from D_A_T.UDI.read_smx_sheet.Logging_Decorator import Logging_decorator
import  D_A_T.UDI.read_smx_sheet.testing_logging as test_logger

@Logging_decorator
def hist_timegap_check(cf, source_output_path, table_mapping, core_tables,connect, script_connect_run):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    count = 1
    scripts_status = str(cf.scripts_status).lower()
    for src in cf.source_names:
        for table_mapping_index, table_mapping_row in table_mapping.iterrows():
            hist_check_name_line = "---hist_timegap_Test_Case_" + str(count) + "---"
            if table_mapping_row['Historization algorithm'] == 'HISTORY':
                target_table = table_mapping_row['Target table name']
                process_name = table_mapping_row['Mapping name']
                hist_cols = table_mapping_row['Historization columns'].split(',')
                hist_cols = [x.strip() for x in hist_cols]
                start_date = TransformDDL.get_core_tbl_sart_date_column(core_tables, target_table)
                end_date = TransformDDL.get_core_tbl_end_date_column(core_tables, target_table)
                hist_keys = TransformDDL.get_trgt_hist_keys(core_tables, target_table, hist_cols)
                start_data_type = TransformDDL.get_data_type_for_history(core_tables,start_date)
                end_data_type =  TransformDDL.get_data_type_for_history(core_tables,end_date)


                call_line1 = "SELECT "+hist_keys+','+start_date+',end_'
                call_line2 = " FROM ( sel "+hist_keys+','+start_date+',MAX('+end_date+')over(partition by '
                call_line3 = hist_keys+' order by '+start_date+' rows between 1 preceding and 1 preceding)as end_ '
                call_line4 = ' FROM '+cf.base_DB+'.'+target_table
                call_line5 = " WHERE PROCESS_NAME = 'TXF_CORE_"+process_name+"')tst"
                call_line6 = ""
                if start_data_type == "TIMESTAMP(0)" or start_data_type == "TIMESTAMP(6)" or start_data_type == "TIMESTAMP (0)" or start_data_type == "TIMESTAMP":
                    call_line6 = " where tst.end_ + INTERVAL'1' SECOND <>tst." +start_date+";\n\n\n"
                elif start_data_type == "DATE":
                    call_line6 = " where tst.end_ + INTERVAL'1' DAY <>tst." + start_date + ";\n\n\n"
                else:
                    continue


                # call_line6 = " WHERE CASE WHEN TYPE(tst.end_) = 'DATE' THEN end_  + INTERVAL'1' DAY ELSE end_  + INTERVAL'1' SECOND END <>tst."+start_date+';'+'\n\n\n'

                # call_line6 = " WHERE CAST (tst.end_ AS TIMESTAMP(0)) + INTERVAL'1' SECOND<>tst." + start_date + ';' + '\n\n\n'
                # call_line6 = " AND CASE WHEN TYPE(end_)='DATE' THEN  end_  + INTERVAL'1' DAY <>tst." + start_date + ';' + '\n\n\n'
                # call_line6 = " where CASE WHEN TYPE(tst.end_) = 'DATE' THEN cast(end_  as date ) + INTERVAL'1' DAY \n WHEN TYPE(tst.end_) = 'TIMESTAMP(0)' THEN CAST(end_ AS TIMESTAMP(0)) + INTERVAL'1' SECOND \n WHEN TYPE(tst.end_) = 'TIMESTAMP(6)' THEN CAST(end_ AS TIMESTAMP(6)) + INTERVAL'1' SECOND \n ELSE END_ END <> tst."+start_date+";\n\n\n"
                hist_test_case_exp = hist_check_name_line + '\n' + call_line1 + '\n' + call_line2 + '\n' + call_line3 + '\n' \
                                     + call_line4 + '\n' + call_line5 + '\n' + call_line6

                script_to_be_sent = call_line1 + call_line2 + call_line3 + call_line4 + call_line5 + call_line6.replace("\n", "")
                # print(script_to_be_sent)

                if scripts_status == "automated" or scripts_status == "all":
                    test_logger.insert_testing_logs(script_to_be_sent, target_table,
                                                    "", "HISTORY TEST", hist_check_name_line, src,
                                                    "HIST_TIME_GAP_TEST",
                                                    cf.base_DB,cf,connect, script_connect_run)

                if scripts_status == "generated" or scripts_status == "all":
                    f.write(hist_test_case_exp)
                count = count + 1
    f.close()
