from django.contrib.auth.models import User , auth

def user_session(request):
    user_id = request.user.id
    return user_id

def source_log(user_id,source_name):
    with open("D_A_T/Sessions/sources_sessions/"+str(user_id)+"_source_name.txt", "w") as file:
        file.write(source_name)
        file.close()

def connection_log(user_id,connection_name):
    with open("D_A_T/Sessions/connection_sessions/"+str(user_id)+"_connection_name.txt", "w") as file:
        file.write(connection_name)
        file.close()

def get_file_path(request,flag):
    user_id = user_session(request)
    file_path =''
    if flag == 'S':
        file_path = "D_A_T/Sessions/sources_sessions/"+str(user_id)+"_source_name.txt"
    elif flag == 'C':
        file_path = "D_A_T/Sessions/connection_sessions/"+str(user_id)+"_connection_name.txt"
    return file_path

def templates_path(request):
    user_id = user_session(request)
    file_path = "D_A_T/Sessions/templates_sessions/" + str(user_id) + "_templates.csv"
    return file_path

def temp_path(request):
    user_id = user_session(request)
    file_path = "D_A_T/Sessions/templates_sessions/" + str(user_id) + "_temp.csv"
    return file_path

def dq_template_path(request):
    user_id = user_session(request)
    file_path = "D_A_T/Sessions/dq_templates/" + str(user_id) + "_dq_template.csv"
    return file_path

def user_input_path(request):
    user_id = user_session(request)
    file_path = "D_A_T/Sessions/user_input_sessions/" + str(user_id) + "_user_input.txt"
    return file_path

def db_path(request):
    user_id = user_session(request)
    file_path = "D_A_T/Sessions/db_sessions/" + str(user_id) + "_db.txt"
    return file_path

def batch_file_path(request):
    user_id = user_session(request)
    file_path = "D_A_T/Sessions/batch_sessions/" + str(user_id) + "_batch_file.txt"
    return file_path

def dq_charts_last_file_path(request):
    user_id = user_session(request)
    file_path = "D_A_T/Sessions/last_dq_run_sessions/" + str(user_id) + "_last_dq_result.txt"
    return file_path
def chart_data_path(request):
    user_id = user_session(request)
    file_path = "D_A_T/Sessions/stats_sessions_charts/" + str(user_id) + "_chart_data.txt"
    return file_path
def chart_dq_data_file_path(request):
    user_id = user_session(request)
    file_path = "D_A_T/Sessions/dq_sessions_charts/" + str(user_id) + "_chart_dq_data.txt"
    return file_path

def read_template_path_file(request):
    user_id = user_session(request)
    file_path = "D_A_T/Sessions/read_temp_sessions/" + str(user_id) + "_read_template.txt"
    return file_path

def total_sessions(request):
    user_id = user_session(request)
    file_path = "D_A_T/Sessions/total_batches_sessions/" + str(user_id) + "_total_temp.txt"
    return file_path



