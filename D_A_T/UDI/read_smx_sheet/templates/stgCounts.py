from D_A_T.UDI.read_smx_sheet.app_Lib import functions as funcs
from D_A_T.UDI.read_smx_sheet.Logging_Decorator import Logging_decorator
from D_A_T.UDI.read_smx_sheet.parameters import parameters as pm
from D_A_T.UDI.read_smx_sheet.app_Lib import TransformDDL as TDDL
import  D_A_T.UDI.read_smx_sheet.testing_logging as test_logger

@Logging_decorator
def stgCounts(cf, source_output_path, System, STG_tables,LOADING_TYPE,flag,connect, script_connect_run):
    file_name = funcs.get_file_name(__file__) + '_' + flag
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    scripts_status = str(cf.scripts_status).lower()
    if flag == 'Accepted':
        template_path = cf.templates_path + "/" + pm.compareSTGacccounts_template_filename
        file_name += '_'+flag
    else:
        template_path = cf.templates_path + "/" + pm.compareSTGcounts_template_filename
    smx_path = cf.smx_path
    template_string = ""
    try:
        REJ_TABLE_NAME = System['Rejection Table Name']
    except:
        REJ_TABLE_NAME = ''
    try:
        REJ_TABLE_RULE = System['Rejection Table Rules']
    except:
        REJ_TABLE_RULE = ''
    try:
        source_DB = System['Source DB']
    except:
        source_DB = ''
    try:
        Model_ID = System['Model ID']
    except:
        Model_ID = ''
    try:
        template_file = open(template_path, "r")
    except:
        template_file = open(smx_path, "r")
    if LOADING_TYPE.upper() == 'ONLINE':
        LOADING_TYPE = 'STG_ONLINE'
        IBM_AUDIT_TABLE = 'IBM_AUDIT_TABLE'
        ONLINE_LOAD_ID_CONDITION = 'AND IBM_STG_TABLE.LOAD_ID = IBM_AUDIT_TABLE.MIC_EXECUTIONGUID'
        QUALIFY_ORDERING = 'DATA_EXTRACTION_DATE DESC,MODIFICATION_TYPE DESC'
        MODEL_ID_CONDITION = ''
    else:
        LOADING_TYPE = 'STG_LAYER'
        IBM_AUDIT_TABLE = 'OFFLINE_AUDIT_TABLE'
        ONLINE_LOAD_ID_CONDITION = ''
        QUALIFY_ORDERING = 'CREATION_TIMESTAMP DESC'
        MODEL_ID_CONDITION = "WHERE MODEL_ID="+str(Model_ID)

    counter = 0
    for i in template_file.readlines():
        if i != "":
            template_string = template_string + i
    stg_table_names = funcs.get_stg_tables(STG_tables)
    for stg_tables_df_index, stg_tables_df_row in stg_table_names[(stg_table_names['Table name'] != REJ_TABLE_NAME) & (stg_table_names['Table name'] != REJ_TABLE_RULE)].iterrows():
        TABLE_NAME = stg_tables_df_row['Table name']
        TBL_PKs = TDDL.get_trgt_pk(STG_tables, TABLE_NAME)

        if flag == 'Accepted':
            output_script = template_string.format(TABLE_NAME=TABLE_NAME,
                                                   STG_DATABASE=cf.T_STG,
                                                   source_DB=source_DB,
                                                   LOADING_TYPE=LOADING_TYPE,
                                                   REJ_TABLE_NAME=REJ_TABLE_NAME,
                                                   REJ_TABLE_RULE=REJ_TABLE_RULE,
                                                   TBL_PKs=TBL_PKs,
                                                   ONLINE_LOAD_ID_CONDITION=ONLINE_LOAD_ID_CONDITION,
                                                   QUALIFY_ORDERING=QUALIFY_ORDERING,
                                                   IBM_AUDIT_TABLE=IBM_AUDIT_TABLE,
                                                   MODEL_ID_CONDITION=MODEL_ID_CONDITION
                                                   )
            process_name_line = "STG_ACCEPTED_COUNT_" + str(counter)
            if scripts_status == "automated" or scripts_status == "all":
                test_logger.insert_testing_logs(output_script, TABLE_NAME,
                                                REJ_TABLE_NAME, "STG CHECK", process_name_line, str(cf.source_names).replace("['","").replace("']","").replace("'",""),
                                                "STG_ACCEPTED_COUNT",
                                                source_DB,cf)
            counter += 1
        else:

            output_script = template_string.format(TABLE_NAME=TABLE_NAME,
                                                   STG_DATABASE=cf.T_STG,
                                                   WRK_DATABASE=cf.t_WRK,
                                                   TBL_PKs=TBL_PKs,
                                                   source_DB=source_DB,
                                                   ONLINE_LOAD_ID_CONDITION=ONLINE_LOAD_ID_CONDITION,
                                                   QUALIFY_ORDERING=QUALIFY_ORDERING,
                                                   LOADING_TYPE=LOADING_TYPE,
                                                   IBM_AUDIT_TABLE=IBM_AUDIT_TABLE,
                                                   MODEL_ID_CONDITION=MODEL_ID_CONDITION
                                               )
            process_name_line = "STG_ALL_COUNT_" + str(counter)
            if scripts_status == "automated" or scripts_status == "all":
                print("automated")
                test_logger.insert_testing_logs(output_script, TABLE_NAME,
                                                " ", "STG CHECK", process_name_line, str(cf.source_names).replace("['","").replace("']","").replace("'",""),
                                                "STG_ALL_COUNT",
                                                source_DB,cf,connect, script_connect_run)
            counter += 1

        seperation_line = '--------------------------------------------------------------------------------------------------------------------------------------------------------------------'
        output_script = output_script.upper() + '\n' + seperation_line + '\n'  + seperation_line + '\n'
        if scripts_status == "generated" or scripts_status == "all":
            f.write(output_script.replace('Â', ' '))
    f.close()
