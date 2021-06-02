import pandas as pd
import calendar
import time
import os
import datetime
from django.http import HttpResponse

def script_replace(file, scriptNumber,name,isRunAll):
    df = pd.read_csv(file)

    scriptFile = open(r'D_A_T/CHECKS/TD_Scripts/' + str(scriptNumber) + '.txt', mode='r')
    script = scriptFile.read()
    lines = ""
    for index, row in df.iterrows():
        dwh = ""
        # if pd.isnull(row['DATAWAREHOUSE_TYPE']):
        #     dwh = " "
        # elif str(row['DATAWAREHOUSE_TYPE']).strip() == "":
        #     dwh = " "
        # else :
        #     dwh = "@" + str(row['DATAWAREHOUSE_TYPE'])

        lines += (script.replace('column', str(row['COLUMN'])).replace("db", str(row['DATABASE'])).replace("table", str(row['TABLE'])).replace("DWH",str(dwh)) + ';')
    return lines

class columns:

    def distinct_values(file,isRunAll):
        return script_replace(file, 320, "Distinct Values",isRunAll)


    def active_columns(file,isRunAll): #rename to null percentage
        return script_replace(file, 310, "Null Percentage",isRunAll)

    def null_columns(file,isRunAll):
        return script_replace(file, 330, "Null Number",isRunAll)

    def date_validate(file,isRunAll):
        df = pd.read_csv(file)

        scriptFile = open(r'D_A_T/CHECKS/TD_Scripts/340.txt', mode='r')
        script = scriptFile.read()
        lines = ""
        for index, row in df.iterrows():
            if(str(row['DATA_TYPE']).upper().strip() in ('TIMESTAMP', 'DATE')):
                dwh = ""
                # if pd.isnull(row['DATAWAREHOUSE_TYPE']):
                #     dwh = " "
                # elif str(row['DATAWAREHOUSE_TYPE']).strip() == "":
                #     dwh = " "
                # else:
                #     dwh = "@" + str(row['DATAWAREHOUSE_TYPE'])

                lines += (script.replace('column', row['COLUMN']).replace("db", row['DATABASE']).replace("table", row['TABLE']).replace("DWH", str(dwh)) + ';')
        return lines

    def pk_stats(file,db_type):
        df = pd.read_csv(file)
        lines = ""
        script =""
        if db_type == "2":
            scriptFile = open(r'D_A_T/CHECKS/TD_Scripts/350.txt', mode='r')
            script = scriptFile.read()
        else:
            scriptFile = open(r'D_A_T/CHECKS/ANSI_Scripts/350.txt', mode='r')
            script = scriptFile.read()


        for index, row in df.iterrows():
            dwh = ""
            # if pd.isnull(row['DATAWAREHOUSE_TYPE']):
            #     dwh = " "
            # elif str(row['DATAWAREHOUSE_TYPE']).strip() == "":
            #     dwh = " "
            # else:
            #     dwh = "@" + str(row['DATAWAREHOUSE_TYPE'])
            if str(row["CONSTRAINT_TYPE"]).strip().upper() == "PK":
                lines += (script.replace('column', row['COLUMN']).replace("db", row['DATABASE']).replace("table", row['TABLE']).replace("DWH", str(dwh)) + ';')
        return  lines

    def all_columns(file):
        columns.distinct_values(file,True)
        columns.active_columns(file,True)
        columns.null_columns(file,True)
        columns.date_validate(file,True)
