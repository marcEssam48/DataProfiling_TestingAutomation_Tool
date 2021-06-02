from D_A_T.app_Lib import manage_directories as md, functions as funcs
import threading
import random
import datetime as dt
import D_A_T.generate_scripts as gs

import D_A_T.UDi_funcs as udi_fun
def get_config_file_values(config_file_path):
    try:
        config_file_values = funcs.get_config_file_values(config_file_path)
        smx_path = config_file_values["smx_path"]
        output_path = config_file_values["output_path"]
        source_names = config_file_values["source_names"]
        source_names = "All" if source_names is None else source_names
        db_prefix = config_file_values["db_prefix"]


    except:
        smx_path = ""
        output_path = ""
        source_names = ""
        db_prefix = ""
    return config_file_values


def generate_scripts_thread(request,file_data,dics):
    try:
        g = gs.GenerateScripts(None, dics)
        config_file_path = udi_fun.write_in_session(request,file_data)
        x = open(config_file_path)

        try:
            # self.enable_disable_fields(DISABLED)
            g.generate_scripts()
            # self.enable_disable_fields(NORMAL)
            # self.UDI_scripts_generation.config(state=NORMAL)
            # self.Testing_scripts_generation.config(state=NORMAL)

        #     print("Total Elapsed time: ", g.elapsed_time, "\n")
        except:
            print("Error generating scripts")

    except Exception as e:
        print(e)

# class GenerateScriptsThread(threading.Thread):
#     def __init__(self, threadID, name, front_end_c, thread=None):
#         threading.Thread.__init__(self)
#         self.threadID = threadID
#         self.name = name
#         self.FrontEndC = front_end_c
#         self.thread = thread
#         self.daemon = True
#
#     def run(self):
#         if self.threadID == 1:
#             self.FrontEndC.generate_scripts_thread(request,file_data)

