from D_A_T.UDI.read_smx_sheet.app_Lib import functions as funcs
from D_A_T.UDI.read_smx_sheet.Logging_Decorator import Logging_decorator
import  D_A_T.UDI.read_smx_sheet.testing_logging as test_logger

@Logging_decorator
def nulls_check(cf, source_output_path, table_mapping, core_tables , Column_mapping,connect, script_connect_run):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    nulls_test_case_exp=''
    count=1
    scripts_status = str(cf.scripts_status).lower()
    for src in cf.source_names:
        for table_mapping_index, table_mapping_row in table_mapping.iterrows():
            for core_table_index, core_table_row in core_tables.iterrows():
                # column_mapping_row = Column_mapping[(Column_mapping['Mapping name'] == table_mapping_row['Mapping name']) & (Column_mapping['Column name'] == core_table_row['Column name'])]
                # is_sk = False
                # try:
                #     # print("before looop : " + str(column_mapping_row['Mapped to column']).lower() + " , " +str(type(column_mapping_row['Mapped to column'])))
                #
                #     if "sk_" in str(column_mapping_row['Mapped to column']).lower():
                #         is_sk = True
                #         # print("after looop : " + str(column_mapping_row['Mapped to column'].values[0]).lower())
                # except Exception as e:
                #     print(e)
                if core_table_row['Table name'] == table_mapping_row['Target table name'] and core_table_row['Mandatory'] == 'Y' :
                    # print(str(column_mapping_row['Mapped to column'].values[0]).lower() + "," + str(core_table_row['Table name']))
                    nulls_test_case_exp += "---Null_Test_Case_" + str(count) + "---"+'\n'+"SEL * FROM " +cf.base_DB +"."+core_table_row['Table name']+" WHERE " + core_table_row['Column name'] + " IS NULL AND PROCESS_NAME='TXF_CORE_"+table_mapping_row['Mapping name']+"';"+'\n'+'\n'
                    NULLS_NAME_LINE = "---Null_Test_Case_" + str(count) + "---"

                    script_to_be_sent = "SEL "+ core_table_row['Column name'] + " FROM " +cf.base_DB +"."+core_table_row['Table name']+" WHERE " + core_table_row['Column name'] + " IS NULL AND PROCESS_NAME='TXF_CORE_"+table_mapping_row['Mapping name']+"';"
                    if scripts_status == "automated" or scripts_status == "all":
                    # print(script_to_be_sent)
                        test_logger.insert_testing_logs(script_to_be_sent, core_table_row['Table name'],
                                                        "", "NULLS TEST", NULLS_NAME_LINE, src,
                                                        "NULLS_TEST",
                                                        cf.base_DB,cf,connect, script_connect_run)

                    count = count+1
                    # print(nulls_test_case_exp)
    if scripts_status == "generated" or scripts_status == "all":
        f.write(nulls_test_case_exp)
    f.close()
