from datetime import datetime

import  pandas as pd
import pytz
import teradatasql

import D_A_T.sources_session as ss
import D_A_T.table_checks as table
def populate(request,connect,batched_scripts,length,db_hide):

    output = []
    big_list = []
    chart_data_tables = []
    chart_data_columns = []
    chart_data_total_rows = []
    chart_data_distinct = []
    chart_data_nulls = []
    output = []
    ui_output = []
    big_ui = []
    n = 0
    file_template_path = ss.templates_path(request)
    # print(file_template_path)
    data_types_list = list(pd.read_csv(file_template_path)['DATA_TYPE'])
    total_rows_list = []
    critcal_counter = 0
    total_batches_count_from_file = ss.total_sessions(request)
    with open(total_batches_count_from_file,"r")  as total_number_of_batches:
        batches_count = total_number_of_batches.read()
    d = datetime.now()
    pacific = pytz.timezone('Etc/GMT+2')
    execution_date = pacific.localize(d)
    # execution_date = datetime.now('EET')


    for script in batched_scripts:
        script = script.replace('@Empty', '')
        txt = str(script).split(".", 1)[-1]
        table_name = txt.split(" ", 1)[0].replace(';', '')
        table_name = table_name.replace('WHERE', '').strip()  # for unknown behaviour

        if table_name not in str(output):
            output.append(table_name)
            ui_output.append(execution_date)
            ui_output.append(length)
            ui_output.append(batches_count)
            ui_output.append(db_hide)
            ui_output.append(table_name)
            sql_rows = "select count(*) as count_all from " + db_hide + "." + table_name + ";"
            res_rows = pd.read_sql(sql_rows,connect)
            for ix in range(0,len(res_rows)):
                total_rows_list.append(res_rows.loc[ix,"count_all"])

        result = pd.read_sql(str(script) + ';', connect)
        column_names = result.head(0)

        for i in range(0, len(result)):
            for y in column_names:
                if y not in output:
                    output.append(y)
                    ui_output.append(y)
                    # print(critcal_counter)
                    ui_output.append(data_types_list[critcal_counter])
                    ui_output.append(total_rows_list[critcal_counter])
                    critcal_counter+=1
                output.append(str(result.loc[i, y]))
                ui_output.append(str(result.loc[i, y]).split(" out of ")[0])




                n = n + 1

        if n == 5:
            big_list.append(output)
            big_ui.append(ui_output)
            print(ui_output)
            chart_data_tables.append(output[0])
            chart_data_columns.append(output[1])

            try:
                chart_data_total_rows.append(output[2].split('out of')[1])
                chart_data_distinct.append(output[2].split('out of')[0])
            except:
                continue
            chart_data_nulls.append(output[4])
            output = []
            ui_output = []
            n = 0
    return big_list , big_ui, chart_data_tables , chart_data_columns , chart_data_total_rows , chart_data_distinct , chart_data_nulls

def code_replace(file_path,temp_path, selected_tables,db_type):
    scripts = []
    df = pd.read_csv(file_path)
    for table_ in selected_tables:

        df_slice = df[df.TABLE == str(table_).strip()]
        df_slice.to_csv(temp_path)
        columns = df_slice.COLUMN
        for col in columns:

            row = df_slice[df_slice.COLUMN == col].iloc[-1]
            dwh = ""
            if db_type == '2':
                scriptFile = open(r'D_A_T/CHECKS/TD_Scripts/320.txt', mode='r')
                scripts.append(scriptFile.read().replace('column', str(row['COLUMN'])).replace("db", str(row['DATABASE'])).replace("table",str(row['TABLE'])).replace("DWH", str(dwh)) + ';')
                scriptFile = open(r'D_A_T/CHECKS/TD_Scripts/330.txt', mode='r')
                scripts.append(scriptFile.read().replace('column', str(row['COLUMN'])).replace("db", str(row['DATABASE'])).replace("table",str(row['TABLE'])).replace("DWH", str(dwh)) + ';')
                scriptFile = open(r'D_A_T/CHECKS/TD_Scripts/310.txt', mode='r')
                scripts.append(scriptFile.read().replace('column', str(row['COLUMN'])).replace("db", str(row['DATABASE'])).replace("table",str(row['TABLE'])).replace("DWH", str(dwh)) + ';')
                scriptFile = open(r'D_A_T/CHECKS/TD_Scripts/360.txt', mode='r')
                scripts.append(scriptFile.read().replace('column', str(row['COLUMN'])).replace("db", str(row['DATABASE'])).replace("table",str(row['TABLE'])).replace("DWH", str(dwh)) + ';')
                scriptFile = open(r'D_A_T/CHECKS/TD_Scripts/370.txt', mode='r')
                scripts.append(scriptFile.read().replace('column', str(row['COLUMN'])).replace("db", str(row['DATABASE'])).replace("table",str(row['TABLE'])).replace("DWH", str(dwh)) + ';')
            else:
                scriptFile = open(r'D_A_T/CHECKS/ANSI_Scripts/320.txt', mode='r')
                scripts.append(scriptFile.read().replace('column', str(row['COLUMN'])).replace("db", str(row['DATABASE'])).replace("table",str(row['TABLE'])).replace("DWH", str(dwh)) + ';')
                scriptFile = open(r'D_A_T/CHECKS/ANSI_Scripts/330.txt', mode='r')
                scripts.append(scriptFile.read().replace('column', str(row['COLUMN'])).replace("db", str(row['DATABASE'])).replace("table", str(row['TABLE'])).replace("DWH", str(dwh)) + ';')
                scriptFile = open(r'D_A_T/CHECKS/ANSI_Scripts/310.txt', mode='r')
                scripts.append(scriptFile.read().replace('column', str(row['COLUMN'])).replace("db", str(row['DATABASE'])).replace("table",str(row['TABLE'])).replace("DWH", str(dwh)).replace("\n","") + ';')
                scriptFile = open(r'D_A_T/CHECKS/ANSI_Scripts/360.txt', mode='r')
                scripts.append( scriptFile.read().replace('column', str(row['COLUMN'])).replace("db", str(row['DATABASE'])).replace("table", str(row['TABLE'])).replace("DWH", str(dwh)).replace("\n", "") + ';')
                scriptFile = open(r'D_A_T/CHECKS/ANSI_Scripts/370.txt', mode='r')
                scripts.append(scriptFile.read().replace('column', str(row['COLUMN'])).replace("db", str(row['DATABASE'])).replace("table", str(row['TABLE'])).replace("DWH", str(dwh)).replace("\n", "") + ';')

    return  scripts


