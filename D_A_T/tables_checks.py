import pandas as pd
import calendar
import time
import os
import datetime
import D_A_T.batching as batcher

class tables:
    table_dictionary = {}

    def ref_tables_checks(file,isRunAll,db_type):
        df = pd.read_csv(file)
        lines = ""

        # for x in range(0, len(df["DATAWAREHOUSE_TYPE"])):
            # print(str(df["TABLE"][x]) + " : " + str(df["DATAWAREHOUSE_TYPE"][x]))
            # tables.table_dictionary[str(df["TABLE"][x])] = str("@" + str(df["DATAWAREHOUSE_TYPE"][x]))
        script = ""
        if db_type == "1":
            scriptFile = open(r'D_A_T/CHECKS/TD_Scripts/110.txt', mode='r')
            script = scriptFile.read()
        else :
            scriptFile = open(r'D_A_T/CHECKS/ANSI_Scripts/110.txt', mode='r')
            script = scriptFile.read()

        for index, row in df.iterrows():
            dwh_a = ""
            swh_b = ""
            if tables.table_dictionary.get(row["TABLE"]) == "@nan":
                dwh_a = " "
            elif tables.table_dictionary.get(row["TABLE"]) == "@ ":
                dwh_a = " "
            elif tables.table_dictionary.get(row["TABLE"]) == "None":
                dwh_a = " "
            else:
                dwh_a = tables.table_dictionary.get(row["TABLE"])
            if tables.table_dictionary.get(row["REFERENCE_TABLE_NAME"]) == "@nan":
                dwh_b = " "
            elif tables.table_dictionary.get(row["TABLE"]) == "@ ":
                dwh_b = " "
            elif tables.table_dictionary.get(row["TABLE"]) == "None":
                dwh_b = " "
            else:
                dwh_b = tables.table_dictionary.get(row["REFERENCE_TABLE_NAME"])
            if (str(row['CONSTRAINT_TYPE']).strip().upper() == 'FK'):
                lines += (script.replace('tableA', str(row['TABLE'])).replace('tableB', str(
                    row['REFERENCE_TABLE_NAME'])).replace('PK', str(row['REFERENCE_COLUMN_NAME'])).replace('FK', str(
                    row['COLUMN'])).replace("db", str(row['DATABASE'])).replace("DWHA", str(dwh_a)).replace("DWHB", str(
                    dwh_b)) + ';')

        return lines
    def run_all_tables_checks(file):

        tables.ref_tables_checks(file,True)

