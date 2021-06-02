from D_A_T.app_Lib import functions as funcs
from D_A_T.Logging_Decorator import Logging_decorator
from D_A_T.parameters import parameters as pm
from D_A_T.app_Lib import TransformDDL as TDDL
import  D_A_T.testing_logging as test_logger


@Logging_decorator
def stg_accepted_count(cf, source_output_path, System, STG_tables, LOADING_TYPE,connect, script_connect_run):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    smx_path = cf.smx_path
    template_path = cf.templates_path + "/" + pm.compareSTGacccounts_template_filename
    template_string = ""
    try:
        REJ_TABLE_NAME = System['Rejection Table Name']
    except:
        REJ_TABLE_NAME = 'REJECTIONS_TERADATA'
    try:
        REJ_TABLE_RULE = System['Rejection Table Rules']
    except:
        REJ_TABLE_RULE = 'BUSINESS_RULES_TERADATA'
    try:
        source_DB = System['Source DB']
    except:
        source_DB = 'STG_ONLINE'

    try:
        template_file = open(template_path, "r")
    except:
        template_file = open(smx_path, "r")

    try:
        Model_ID = System['Model ID']
    except:
        Model_ID = ''

    if LOADING_TYPE.upper() == 'ONLINE':
        LOADING_TYPE = 'STG_ONLINE'
        IBM_AUDIT_TABLE='IBM_AUDIT_TABLE'
        ONLINE_LOAD_ID_CONDITION = 'AND IBM_STG_TABLE.LOAD_ID = IBM_AUDIT_TABLE.MIC_EXECUTIONGUID'
        QUALIFY_ORDERING = 'DATA_EXTRACTION_DATE DESC,MODIFICATION_TYPE DESC'
        MODEL_ID_CONDITION = ""
    else:
        LOADING_TYPE = 'STG_LAYER'
        IBM_AUDIT_TABLE='OFFLINE_AUDIT_TABLE'
        ONLINE_LOAD_ID_CONDITION = ''
        QUALIFY_ORDERING = 'CREATION_TIMESTAMP DESC'
        MODEL_ID_CONDITION = "WHERE MODEL_ID=" + str(Model_ID)

    for i in template_file.readlines():
        if i != "":
            template_string = template_string + i
    stg_table_names = funcs.get_stg_tables(STG_tables)
    if LOADING_TYPE == 'ONLINE':
        LOADING_TYPE = 'STG_ONLINE'
    else:
        LOADING_TYPE = 'STG_LAYER'
    COUNTER = 1
    for src in cf.source_names:
        for stg_tables_df_index, stg_tables_df_row in stg_table_names[(stg_table_names['Table name'] != REJ_TABLE_NAME) & (
                stg_table_names['Table name'] != REJ_TABLE_RULE)].iterrows():
            TABLE_NAME = stg_tables_df_row['Table name']
            TBL_PKs = TDDL.get_trgt_pk(STG_tables, TABLE_NAME)
            output_script = template_string.format(TABLE_NAME=TABLE_NAME,
                                                   STG_DATABASE=cf.T_STG,
                                                   source_DB=source_DB,
                                                   LOADING_TYPE=LOADING_TYPE,
                                                   REJ_TABLE_NAME=REJ_TABLE_NAME,
                                                   REJ_TABLE_RULE=REJ_TABLE_RULE,
                                                   TBL_PKs=TBL_PKs,
                                                   MODEL_ID_CONDITION=MODEL_ID_CONDITION
                                                   )
            STG_LINE_NAME = "STG_ACCEPTED_COUNT " + str(COUNTER)
            test_logger.insert_testing_logs(output_script, TABLE_NAME,
                                            " ", "DATA VALIDATION", STG_LINE_NAME, src,
                                            "STG_ACCEPTED_COUNT",
                                            source_DB,connect, script_connect_run)
            COUNTER+=1
            seperation_line = '--------------------------------------------------------------------------------------------------------------------------------------------------------------------'
            output_script = output_script.upper() + '\n' + seperation_line + '\n'  + seperation_line + '\n'
            f.write(output_script.replace('Â', ' '))
    f.close()
