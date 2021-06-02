import D_A_T.sources_session as ss

def retrieve_config_file():
    with open("D_A_T/Udi_config_file/config.txt","r") as file:
        config = file.read()
    return config

def write_in_session(request,file_data):
    user_id = ss.user_session(request)
    file_path = 'D_A_T/Sessions/UDI_Sessions/'+str(user_id)+'_UDI'
    with open(file_path,'w') as file:
        file.write(file_data)
        file.close()
    return file_path

def write_the_config_file(output_path,smx_path,template_path,source_name,core_table_name,online_source_t, offline_source_t,db_prefix,scripts_flag , gcfr_ctl_Id , gcfr_stream_key ,gcfr_system_name, gcfr_stream_name , etl_process_table ,SOURCE_TABLES_LKP_table,SOURCE_NAME_LKP_table,history_tbl,gcfr_bkey_process_type,gcfr_snapshot_txf_process_type,gcfr_insert_txf_process_type,gcfr_others_txf_process_type,read_sheets_parallel,Data_mover_flag , testing_categories,ip,username,password,scripts_status):
    config_file = 'smx_path="'+smx_path+ '" \n' + \
                  'home_output_folder="' +output_path+ '"\n \n' + \
        '# Source Filter:' + "\n" + \
        '## for all sources: source_names=""' + "\n" + \
        '## for for certain sources: sources: source_names="TAMWEEN,TAX,EDU,CUSTOMS"'  + "\n \n" + \
        'source_names="' +source_name+ '"\n' + \
        'core_table_name="' + core_table_name + '"\n' + \
        'online_source_t = "'+online_source_t+'"\n' +\
        'offline_source_t = "'+offline_source_t+ '"\n \n' +\
    'etl_process_table = "' +etl_process_table+'"\n' + \
    'SOURCE_TABLES_LKP_table = "'+SOURCE_TABLES_LKP_table+'"\n' +\
    'SOURCE_NAME_LKP_table = "' +SOURCE_NAME_LKP_table+ '"\n' +\
    'history_tbl = "'+history_tbl+'"\n \n' + \
    'db_prefix = "'+db_prefix+'"\n'+\
    'scripts_flag = "'+scripts_flag+'"\n'+\
    'gcfr_ctl_Id =' +str(gcfr_ctl_Id) + '\n'+\
    'gcfr_stream_key = '+str(gcfr_stream_key) + '\n' +\
    'gcfr_system_name = "'+gcfr_system_name+'"\n'+\
    'gcfr_stream_name = "'+gcfr_stream_name+'"\n \n ' +\
    'gcfr_bkey_process_type = '+str(gcfr_bkey_process_type)+ '\n' +\
    'gcfr_snapshot_txf_process_type = '+str(gcfr_snapshot_txf_process_type)+ '\n' +\
    'gcfr_insert_txf_process_type = '+str(gcfr_insert_txf_process_type)+ '\n' +\
    'gcfr_others_txf_process_type = '+str(gcfr_others_txf_process_type)+ '\n \n' +\
    '# values: 1 or 0' + '\n' +\
    'read_sheets_parallel = 1' + '\n' +\
    'Data_mover_flag = 1' + '\n'+\
        'testing_catgs = ' + str(testing_categories)+ '" \n' + \
    'templates_folder_path="' + template_path + '" \n' +\
        'D215_template_filename = "D215.txt" ' + "\n" +\
        'compareSTGcounts_template_filename = compareSTGcounts.txt' + "\n" +\
        'compareSTGacccounts_template_filename = compareSTGacceptedCounts.txt' + "\n" +\
        'dataValidation_template_filename = dataValidation.txt' + "\n" +\
        'ip =' +ip+ "\n" +\
        'username =' + username + "\n" +\
        'password =' + password + "\n" +\
        'scripts_status =' +scripts_status + "\n"


    return config_file


