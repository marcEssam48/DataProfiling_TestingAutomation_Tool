import pandas as pd
import calendar
import time
import datetime;
import os;

class table:

    def total_columns(file, isRunAll):
        tables = []
        counters = []
        counter = 0
        data = pd.read_csv(file)
        dbs = []

        lines = ""
        context = {}

        for x in data['TABLE']:
            if x not in tables:
                tables.append(x)
        # lines += "This Source has " + str(len(table.tables)) + " tables :"
        context['tables'] = tables

        for y in data['DATABASE']:
            if y not in dbs:
                dbs.append(y)
        context['dbs'] = dbs

        for i in range(0, len(tables)):
            for j in data['TABLE']:
                if tables[i] == j:
                    counter += 1

            # print("Table " + tables[i] + " has " + str(counter) + " Columns ")

            # lines += "Table " + table.tables[i] + " has " + str(counter) + " Columns "

            counters.append(counter)
            counter = 0
        context['columns'] = counters
        # print(context)
        return context

    def total_rows(file,isRunAll,db_type):
        df = pd.read_csv(file)
        script = ""
        if db_type == "2":
            scriptFile = open(r'D_A_T/CHECKS/TD_Scripts/220.txt', mode='r')
            script = scriptFile.read()
        else:
            scriptFile = open(r'D_A_T/CHECKS/ANSI_Scripts/220.txt', mode='r')
            script = scriptFile.read()

        lines = ""

        visited = []

        for index, row in df.iterrows():
            dwh = ""
            # if pd.isnull(row['DATAWAREHOUSE_TYPE']):
            #     dwh = " "
            # elif str(row['DATAWAREHOUSE_TYPE']).strip() == "":
            #     dwh = " "
            # else:
            #     dwh = "@" + str(row['DATAWAREHOUSE_TYPE'])
            if(row['TABLE'] not in visited):
                visited.append(row['TABLE'])
                lines += (script.replace('column', str(row['COLUMN'])).replace("db", str(row['DATABASE'])).replace("table", str(row['TABLE'])).replace("DWH",str(dwh)) + ';')

        return lines

    def unique_col(file):
        # write your code here
        print(file)

    def pk_col(file, isRunAll,db_type):
        table_dictionary = {}
        df = pd.read_csv(file)
        lines = ""

        # for x in range(0, len(df["DATAWAREHOUSE_TYPE"])):
            # print(str(df["TABLE"][x]) + " : " + str(df["DATAWAREHOUSE_TYPE"][x]))
            # table.table_dictionary[str(df["TABLE"][x])] = str("@" + str(df["DATAWAREHOUSE_TYPE"][x]))
        script = ""
        if db_type == "2":
            scriptFile = open(r'D_A_T/CHECKS/TD_Scripts/230.txt', mode='r')
            script = scriptFile.read()
        else :
            scriptFile = open(r'D_A_T/CHECKS/ANSI_Scripts/230.txt', mode='r')
            script = scriptFile.read()


        for index, row in df.iterrows():
            dwh_a = ""
            swh_b = ""
            if table_dictionary.get(row["TABLE"]) == "@nan":
                dwh_a = " "
            elif table_dictionary.get(row["TABLE"]) == "@ ":
                dwh_a = " "
            else:
                dwh_a = table_dictionary.get(row["TABLE"])
            if table_dictionary.get(row["REFERENCE_TABLE_NAME"]) == "@nan":
                dwh_b = " "
            elif table_dictionary.get(row["TABLE"]) == "@ ":
                dwh_b = " "
            else:
                dwh_b = table_dictionary.get(row["REFERENCE_TABLE_NAME"])

            if str(row["CONSTRAINT_TYPE"]).strip().upper() == "FK":
                    lines += (script.replace('column', str(row['COLUMN'])).replace("db", str(row['DATABASE'])).replace("tableA", str(row['TABLE'])).replace("REF_TABLE", str(row["REFERENCE_TABLE_NAME"])).replace("FK", str(row["COLUMN"])).replace("PK", str(row["REFERENCE_COLUMN_NAME"])).replace("DWHA",str(dwh_a)).replace("DWHB", str(dwh_b)) + ';')
        return lines

    def fk_col(file):
        # write your code here
        print(file)

    def run_all_table_checks(file):
        # table.total_columns(file,True)
        table.total_rows(file,True)
        # table.unique_col(file,True)
        table.pk_col(file,True)
        # table.fk_col(file)

        # C:\Data Modeling\Data Analysis Tool\template.csv


