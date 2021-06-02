import pandas as pd
import teradatasql
import requests
import  datetime as dt


def teradata_connection():
    connect = teradatasql.connect(host='172.19.3.10', user='TST_PETER', password='TST_PETER', encryptdata=True)
    return connect


def read_excel_data(excel_file, sheet_name):
    excel_data = pd.read_excel(excel_file, sheet_name=sheet_name)
    return excel_data


def apis_unique_list (automation_template):
    unique_list=[]
    for index, row in automation_template.iterrows():
        if row['Client'] not in unique_list:
            unique_list.append(row['Client'])
    return unique_list


def permutations_query (client, automation_template):
    distinct_query = ''
    for index, row in automation_template.iterrows():
        if client ==row['Client'] :
            distinct_query = row['Query']
            break

    return distinct_query


def permutations_flag (client, automation_template):
    flag = ''
    for index, row in automation_template.iterrows():
        if client.strip() ==str(row['Client']).strip() :
            flag = row['PermutationsFlag']
            break
    return flag

def Running_flag(client,automation_template):
    flag = ''
    for index, row in automation_template.iterrows():
        if client.strip() == str(row['Client']).strip():
            flag = row['RunningFlag']
            break
    return flag


def distinct_query_columns (query):
    cols=query.split("DISTINCT")[1]
    comma_delimited_cols = cols.split("FROM")[0]
    distinct_query_columns_list = comma_delimited_cols.split(",")
    return distinct_query_columns_list




def cleaning_columns (distinct_query_columns_list):
    clean_col_list=[]
    for col in distinct_query_columns_list:
        clean_col_list.append(str(col).strip().replace(" ",""))
    return clean_col_list


def data_type_check(column_name,data_types_template):
    data_type=''
    for index,row in data_types_template.iterrows():
        if column_name == str(row['ColumnName']):
            data_type = row['DataType']
            return data_type


def null_replacement(where_list):
    clean_where_list = []
    for stmt in where_list:
       new_stmt= stmt.replace("= nan"," IS NULL ")
       clean_where_list.append(new_stmt)
       break

    return clean_where_list


def permutations_where_condition (distinct_permutations_result,distinct_cols,data_types_template):
    where_stmt = ''
    where_list = []
    for j in range(0,len(distinct_permutations_result)):
        for i in range(0,len(distinct_cols)):
            data_type = data_type_check(distinct_cols[i], data_types_template)
            if i != len(distinct_cols) -1:
                if data_type == 'VARCHAR' or data_type == 'CHAR':
                    where_stmt += ' ' + str(distinct_cols[i]) + " = '" + str(distinct_permutations_result.loc[j,distinct_cols[i]]) + "' AND "
                elif data_type == 'DATE':
                    where_stmt += ' ' + str(distinct_cols[i]) + " = '" + str(dt.datetime.strptime(str(distinct_permutations_result.loc[j, distinct_cols[i]]),'%m/%d/%Y')).replace(" 00:00:00","").replace("/","-") + "' AND "
                else:
                    where_stmt += ' ' + str(distinct_cols[i]) + " = " + str(distinct_permutations_result.loc[j, distinct_cols[i]]) + " AND "

            else:
                if data_type == 'VARCHAR' or data_type == 'CHAR':
                    where_stmt += ' ' + str(distinct_cols[i]) + " = '" + str(distinct_permutations_result.loc[j, distinct_cols[i]]) + "' "
                elif data_type == 'DATE':
                    where_stmt += ' ' + str(distinct_cols[i]) + " = '" + str(dt.datetime.strptime(str(distinct_permutations_result.loc[j, distinct_cols[i]]),'%m/%d/%Y')).replace(" 00:00:00", "").replace("/","-") + "' "

                else:
                    where_stmt += ' ' + str(distinct_cols[i]) + " = " + str(distinct_permutations_result.loc[j, distinct_cols[i]])

        # print(where_stmt)
        where_list.append(where_stmt)
        where_stmt = ''
        break

    return where_list


def query_column_input (client,automation_template):
    input_col=''
    for index, row in automation_template.iterrows():
        if client==row['Client']:
            input_col = row['InputColumn']
            return input_col


def database_table_input (client,automation_template):
    database_table =''
    for index, row in automation_template.iterrows():
        if client == row['Client']:
            database_table = str(row['DatabaseName']) + '.' + str(row['TableName'])
            return database_table


def soap_request(service):
    file = open("C:\\Users\\Nooran.helmy\\Desktop\\tool\\HALAZOUNA TOOL\\Halazouna 1.2.1-BETA\\New_UDI_DataProfiling_Tool-master\\D_A_T\\API_FUNCS\\SOAPTemps\\" + service + ".txt", "r+", encoding="utf8")
    content = ""
    for string in file.readlines():
        content += string
    return content


def soap_response(xml):
    url = "http://172.19.3.8:8096/rta-digital-egypt/rta"
    headers = {'content-type': 'application/soap+xml'}
    headers = {'content-type': 'text/xml'}
    body = xml
    response = requests.post(url, data=body, headers="")
    return response.content

def soap_request_multiple_input(client,Service_Slug,automation_template,xml_request,Input_Param,key,connect):
    input_columns =  Input_Param.split(",")
    InpDBColumns = []
    InpXMLParam = []
    xml_request_temp = xml_request
    for element in input_columns:
        InpDBColumns.append(element.split("=")[0])
        InpXMLParam.append(element.split("=")[1])
    FCNSelect = "SELECT " + str(query_column_input(client,automation_template)) + " FROM " + str(database_table_input(client, automation_template)) + " WHERE " + str(InpDBColumns[0]) + " = '" + str(key) + "'"
    FCNResult = pd.read_sql(FCNSelect,connect)
    for index in range(0,len(InpDBColumns)):
        xml_request_temp = str(xml_request_temp).replace(str(InpXMLParam[index]).strip(),str(FCNResult.loc[0,str(InpDBColumns[index]).strip()]))

    return xml_request_temp

def splitting_where_params_value(stmt):
    dictionary_of_clean_values = {}
    unclean_values_list = stmt.replace(",","").split("AND")
    # print(unclean_values_list)
    for element in unclean_values_list:
        try:
            dictionary_of_clean_values[element.split("=")[0]]= element.split("=")[1]
        except :
            continue
    return dictionary_of_clean_values


# returns all output columns of a certain Service regardless which service slug is assigned
def unique_outputs_accross_service(client,automation_template):
    unique_service_output = []
    outputs_list_of_strings = []
    for index,row in automation_template.iterrows():
        if row["Client"] == client:
            outputs = str(row["OutputParameter"]).split(",")
            for output_column in outputs:
                if output_column not in unique_service_output and output_column != "nan":
                    unique_service_output.append(output_column)
    return  unique_service_output

#  returns all column names in the physical database
def get_real_column_names(parameter_list,parameters_template,client):
    RealColNames = []
    for param_name in parameter_list:

        for index,row in parameters_template.iterrows():
            if param_name == str(row["ParameterName"]).strip() and row["DBColumnName"] not in RealColNames and client == str(row["ServiceName"]):
                RealColNames.append(row["DBColumnName"])
    return RealColNames



#  generates script that selects all outputs needed for a certain Service regardless which service slug is assigned
def select_all_service_output_script(database_table,output_col_list,input_col_value,input_col,data_types_template):
    query = "Select "
    for index in range(0,len(output_col_list)):
        if index != len(output_col_list)-1:
            query+= output_col_list[index] + " , "
        else:
            query += output_col_list[index]
    query+= " FROM " + str(database_table) + " WHERE " + str(input_col.split(",")[0]) + " = "
    data_type = data_type_check(input_col, data_types_template)
    if data_type == "VARCHAR":
        query+= " '" + input_col_value + "' "
    else:
        query += input_col_value

    return query


def ConvertDataFrameToDictionary(outputDataFrame,parameters_template):
    OutputResultDict = {}
    for col_name in outputDataFrame.head(0):
        for index, row in parameters_template.iterrows():
            if str(row["DBColumnName"]).strip() == col_name:
                # print(col_name)
                OutputResultDict[row["ParameterName"]] = str(outputDataFrame.loc[0,col_name]).replace("-","/")
                # print(OutputResultDict[row["ParameterName"]])

    return OutputResultDict

def ConvertDataFrameToDictionaryMultipleOutputs(outputDataFrame,parameters_template):
    dictionaries=[]
    OutputResultDict = {}

    for i in range(0,len(outputDataFrame)):
        for col_name in outputDataFrame.head(0):
            for index, row in parameters_template.iterrows():
                if str(row["DBColumnName"]).strip() == col_name:
                    # print(col_name)
                    OutputResultDict[row["ParameterName"]] = str(outputDataFrame.loc[i,col_name]).replace("-","/")

                # print(OutputResultDict[row["ParameterName"]])
        dictionaries.append(OutputResultDict)
        OutputResultDict = {}

    return dictionaries

def ConvertDataFrameToDictionaryCounterCase(outputDataFrame,parameters_template,counter):
    OutputResultDict = {}
    for col_name in outputDataFrame.head(0):
        for index, row in parameters_template.iterrows():
            if str(row["DBColumnName"]).strip() == col_name:
                OutputResultDict[row["ParameterName"]] = str(outputDataFrame.loc[counter,col_name]).replace("-","/").replace(".0","")

    return OutputResultDict



def XML_parser(xml_response):
    number_of_keys = len(xml_response.split("<ns2:key>"))
    xmlDictionary = {}
    for i in range(1,number_of_keys):
        try:
            keys_splitter = xml_response.split("<ns2:key>")[i].split("</ns2:key>")[0]
            values_splitter = xml_response.split("<ns2:value>")[i].split("</ns2:value>")[0]
            xmlDictionary[keys_splitter] = values_splitter
        except :
            print("Invalid user")
    return xmlDictionary

def XML_parser_MultipleOutput(xml_response,keys_length):
    number_of_keys = len(xml_response.split("<ns2:key>"))
    # print(number_of_keys)
    # print(keys_length)
    xmlDictionary = {}
    XmlMultList = []
    xmlUniqueKeys = []
    # for j in range(0,keys_length)
    for i in range(1,number_of_keys):
        try:
            keys_splitter = xml_response.split("<ns2:key>")[i].split("</ns2:key>")[0]
            # values_splitter = xml_response.split("<ns2:value>")[i].split("</ns2:value>")[0]
            if keys_splitter not in xmlUniqueKeys:
                xmlUniqueKeys.append(keys_splitter)
            # print(keys_splitter + " : " + values_splitter)
            # xmlDictionary[keys_splitter] = values_splitter
        except :
            print("Invalid user")
    for i in range(1,number_of_keys):
        try:
            keys_splitter = xml_response.split("<ns2:key>")[i].split("</ns2:key>")[0]
            values_splitter = xml_response.split("<ns2:value>")[i].split("</ns2:value>")[0]
            # print(keys_splitter + " : " + values_splitter)
            if i % len(xmlUniqueKeys) == 0:
                xmlDictionary[keys_splitter] = values_splitter
                XmlMultList.append(xmlDictionary)
                xmlDictionary = {}
            else:
                xmlDictionary[keys_splitter] = values_splitter
        except :
            print("Invalid user")
    return XmlMultList

def SortDictionary(list):
    for i in range(0,len(list)):
        for j in range(i+1,len(list)):
            if str(list[i]) > str(list[j]) :
                temp = str(list[j])
                list[j] = str(list[i])
                list[i] = str(temp)
    return list



def XML_DATABASE_COMPARISON(XMLDictionary,DatabaseDictionary,key):
    flag = False
    for value in XMLDictionary[key]:
        if XMLDictionary[key][value] == DatabaseDictionary[key][value]:
            flag = True
            print("IN XML : " + str(XMLDictionary[key]))
            print("IN  DB : " + str(DatabaseDictionary[key]))
            print("key : " + key + " Passed the test.")
            print("============================================================")
        else:
            flag = False
            break
    return flag


def CompareMultiple(XMLDictionary, DBDictionary, key):
    XMLList = XMLDictionary[key]
    DBList = DBDictionary[key]
    flag = False
        # print(len(XMLList))
    if len(XMLList) == len(DBList):
        for i in range (0,len(XMLList)):

            print("Running " + str(i+1) + " out of " + str(len(XMLList)))
            XMLInternalDict = {}
            DBInternalDict = {}
            XMLSortedDict={}
            DBSortedDict={}
            XMLInternalDict = XMLDictionary[key][i]
            DBInternalDict =  DBDictionary[key][i]
            for xmlkey in sorted(XMLInternalDict):
                XMLSortedDict[xmlkey]= XMLInternalDict[xmlkey]

            for dbkey in sorted(DBInternalDict):
                DBSortedDict[dbkey]= DBInternalDict[dbkey]
            print("XML : " + str(XMLSortedDict).replace("nan", "None"))
            print("DB  : " + str(DBSortedDict))
            if str(XMLSortedDict).replace("nan","None") == str(DBSortedDict).replace("nan","None"):
                flag = True

            else:
                flag = False
                break
    else:
        flag = False
    return flag

    # print(len(DBDictionary))
    # print(len(XMLList))
def replace_top_x(query,input_col):
    query = query.replace("TOP X *","TOP 10 *")
    query += " ORDER BY " + str(input_col)
    return query


def execute_script(script,connect):
    result = pd.read_sql(script,connect)
    return result

def XMLToDictionary(DatabaseDictionary,client,ServiceSlug,InputParam,InputColumn,OutputParam):
    XMLDictionary = {}
    for key in DatabaseDictionary.keys():
        xml_request_template = soap_request(client)
        xml_request = xml_request_template.replace("ServiceSlug_value", ServiceSlug).replace(InputParam, key)
        xml_response = soap_response(xml_request).decode()
        # print(xml_request)
        XMLDictionary[key] = XML_parser(xml_response)
        Comaprison_status = XML_DATABASE_COMPARISON(XMLDictionary, DatabaseDictionary,key)
        if Comaprison_status == True:
            print(ServiceSlug + " Case Passed")
        else:
            print(ServiceSlug + " Case Failed")
            if InputColumn == "nan" or OutputParam == "nan":
                print("Something missing in the excel please check it out  ")
            else:
                print("Something went wrong")

        print("==============================================================================================")



def permutations_case (query, connect, client, automation_template, data_types_template,parameters_template,flag):
    distinct_cases_dictionary = {}
    # print("getting distinct cols")
    distinct_cols = distinct_query_columns(query)
    # print("getting clean cols")
    clean_cols = cleaning_columns(distinct_cols)
    # print(query)
    # print("getting permutations dataframe")
    permutations_dataframe = pd.read_sql(query, connect)
    # print("getting where unclean list")
    # if flag == 1:
    where_unclean_list = permutations_where_condition(permutations_dataframe,clean_cols,data_types_template)
    # print("getting where clean list")
    where_clean_list = null_replacement(where_unclean_list)
    # print(where_clean_list)
    # print("getting input cols")
    input_col = query_column_input(client, automation_template)
    # print("getting DB.TABLE")
    database_table = database_table_input(client, automation_template)
    # print("getting OUTPUT PARAMETERS")
    output_col_list = unique_outputs_accross_service(client, automation_template)
    # print("getting OUTPUT DB COL NAMES")
    ColRealNames = get_real_column_names(output_col_list,parameters_template,client)


    for stmt in where_clean_list:
        if str(input_col) != "nan":
            permutations_final_query = 'SELECT TOP 1 ' + str(input_col) + ' FROM ' + str(database_table) +' WHERE ' + str(stmt)
            # print(permutations_final_query)
            permutations_result = pd.read_sql(permutations_final_query, connect)
            # print(permutations_result.loc[0,str(input_col)])
            input_col_value = permutations_result.loc[0,str(input_col).split(",")[0]]
            # distinct_cases_dictionary[input_col_value] = splitting_where_params_value(stmt)
            output_query = select_all_service_output_script(database_table, ColRealNames, input_col_value, input_col,data_types_template)
            output_query_result = pd.read_sql(output_query,connect)

            if flag ==1:
                distinct_cases_dictionary[input_col_value] = ConvertDataFrameToDictionary(output_query_result,parameters_template)
            elif flag ==0:
                distinct_cases_dictionary[input_col_value] =ConvertDataFrameToDictionaryMultipleOutputs(output_query_result,parameters_template)
        else:
            print("Something missing in the excel please check it out  ")
    return distinct_cases_dictionary


def full_table_case (query, connect, client, automation_template, data_types_template,parameters_template):
    db_dict={}
    input_col=query_column_input(client, automation_template)
    query_ready=replace_top_x(query,input_col)
    output_col_list = unique_outputs_accross_service(client, automation_template)

    ColRealNames = get_real_column_names(output_col_list, parameters_template, client)
    full_table_dataframe = pd.read_sql(query_ready, connect)
    # print(db_output_dict)
    counter =0
    for index,row in full_table_dataframe.iterrows():
        input_parameter_value=str(row[str(input_col).split(",")[0]])
        single_input_row=full_table_dataframe[full_table_dataframe[str(input_col).split(",")[0]] == input_parameter_value]
        db_output_dict=ConvertDataFrameToDictionaryCounterCase(single_input_row,parameters_template,counter)
        db_dict[input_parameter_value]=db_output_dict
        counter+=1
    return db_dict


def GetInputParameters(client, automation_template):
    InputParameters=''
    for index,row in automation_template.iterrows():
        if row["Client"]==client:
            InputParameters=row["InputParam"]
            break
    return InputParameters


def FullTableMacro(query, connect, client, automation_template, data_types_template,parameters_template,procedure_flag):
    db_dict = {}
    input_col = query_column_input(client, automation_template)
    query_ready = replace_top_x(query, input_col)
    output_col_list = unique_outputs_accross_service(client, automation_template)
    ColRealNames = get_real_column_names(output_col_list, parameters_template, client)
    full_table_dataframe = pd.read_sql(query_ready, connect)
    UncleanInputColumnParameter=GetInputParameters(client, automation_template)
    SlicedInputParameters=UncleanInputColumnParameter.split(",")
    input_coloumn=[]
    for element in SlicedInputParameters:
        input_coloumn.append(element.split("=")[0])
    counter = 0
    macro_query =""
    procedure_query=""
    for index, row in full_table_dataframe.iterrows():
    #     InputColumnName = str(input_col).split(",")
        macro_body = "("
        for i in range(0,len(input_coloumn)):
            input_parameter_value = str(row[input_coloumn[i]])
            if i == len(input_coloumn) -1 :
                macro_body += "'" + input_parameter_value + "')"
            else:
                macro_body += "'" + input_parameter_value + "' , "
        if procedure_flag==0:
            macro_query = "Execute " + "GTST1V_API_DATA.MAC_ASSET02_COMPANY_VERFICATION_2" + str(macro_body)
            macro_result = pd.read_sql(macro_query,connect)
            db_dict[str(row[input_coloumn[0]])] = ConvertDataFrameToDictionary(macro_result,parameters_template)
        elif procedure_flag==1:
            procedure_query = "CALL " + "GTST1V_API_DATA.SP_ASSET01_CITIZEN_NAME_VERFICATION" + str(macro_body)
            # procedure_result = pd.read_sql(procedure_query,connect)
            procedureResult = pd.read_sql(procedure_query,connect)
            print(procedure_query)
            db_dict[str(row[input_coloumn[1]])] = ConvertDataFrameToDictionary(procedureResult, parameters_template)
    return db_dict

def GetProcedureQuery(client,procedure_template):
    procedure_query=""
    for index, row in procedure_template.iterrows():
        if row["Client"]==client:
            procedure_query=row["Query"]
    return procedure_query

def GetProcedureParams(client,procedure_template):
    procedure_parameters=""
    for index, row in procedure_template.iterrows():
        if row["Client"]==client:
            procedure_parameters=row["Parameters"]
    return procedure_parameters


def ProcedureQuery(procedure_template_query,parameters,permutations_result,data_types_template):
    select_stmt=procedure_template_query.split("WHERE")[0]
    where_condition =" WHERE "
    parameter_list=parameters.split(",")
    DBOutputColumns= permutations_result.head(0)
    ProcedureColumns=[]
    for element in parameter_list:
        ProcedureColumns.append(element.split("=")[0])

    for i in range(0,len(ProcedureColumns)):
        datatype = data_type_check(str(ProcedureColumns[i]), data_types_template)
        if i ==len(ProcedureColumns)-1:
            if datatype =="VARCHAR" or datatype =="CHAR":

                where_condition += str(ProcedureColumns[i])+ " = '" +str(permutations_result.loc[0,str(ProcedureColumns[i])])+"'"
            else:
                where_condition += str(ProcedureColumns[i]) + " = " + str(permutations_result.loc[0, str(ProcedureColumns[i])]) + ""

        else:
            if datatype == "VARCHAR" or datatype == "CHAR":
                where_condition += str(ProcedureColumns[i]) + " = '" + str(permutations_result.loc[0, str(ProcedureColumns[i])]) + "' AND "
            else:
                where_condition += str(ProcedureColumns[i]) + " = " + str(permutations_result.loc[0, str(ProcedureColumns[i])]) + " AND "
    executablequery=select_stmt + where_condition
    executablequery = executablequery.replace("= None"," IS NULL")
    return executablequery






def permutationsprodcedure(query, connect, client, automation_template, parameters_template,data_types_template,procedure_template):
    distinct_cases_dictionary = {}
    # print("getting distinct cols")
    distinct_cols = distinct_query_columns(query)
    # print("getting clean cols")
    clean_cols = cleaning_columns(distinct_cols)
    # print(query)
    # print("getting permutations dataframe")
    permutations_dataframe = pd.read_sql(query, connect)
    # print("getting where unclean list")
    # if flag == 1:
    where_unclean_list = permutations_where_condition(permutations_dataframe, clean_cols, data_types_template)
    # print("getting where clean list")
    where_clean_list = null_replacement(where_unclean_list)
    # print(where_clean_list)
    # print("getting input cols")
    input_col = query_column_input(client, automation_template)
    # print("getting DB.TABLE")
    database_table = database_table_input(client, automation_template)
    # print("getting OUTPUT PARAMETERS")
    output_col_list = unique_outputs_accross_service(client, automation_template)
    # print("getting OUTPUT DB COL NAMES")
    ColRealNames = get_real_column_names(output_col_list, parameters_template, client)

    for stmt in where_clean_list:
        if str(input_col) != "nan":
            permutations_final_query = 'SELECT TOP 1 ' + str(input_col) + ' FROM ' + str(
                database_table) + ' WHERE ' + str(stmt)
            # print(permutations_final_query)
            permutations_result = pd.read_sql(permutations_final_query, connect)
            # print(permutations_result.loc[0,str(input_col)])
            input_col_value = permutations_result.loc[0, str(input_col).split(",")[1]]

            procedure_query = "Call GTST1V_API_DATA.SP_TAMWEEN_ENTITLEMENT_AUTOMATION "
            procedure_body = "("
            inpColList = input_col.split(",")
            for InpColIndex in range(0,len(inpColList)):
                if InpColIndex == len(input_col.split(",")) -1:
                    procedure_body += "'" + str(permutations_result.loc[0,inpColList[InpColIndex]]) + "')"
                else :
                    procedure_body += "'" + str(permutations_result.loc[0, inpColList[InpColIndex]]) + "',"

            procedure_query+=procedure_body
            procedure_result = pd.read_sql(procedure_query,connect)
            print(procedure_result)

            procedure_query=GetProcedureQuery(client,procedure_template)
            procedure_parameters=GetProcedureParams(client,procedure_template)
            executablequery=ProcedureQuery(procedure_query, procedure_parameters, permutations_result,data_types_template)
            procedure_result = pd.read_sql(executablequery,connect)
            distinct_cases_dictionary[input_col_value] =ConvertDataFrameToDictionary(procedure_result,parameters_template)
            # print(distinct_cases_dictionary)




            # procedure_query=GetProcedureQuery(client,procedure_template)
            # procedure_parameters=GetProcedureParams(client,procedure_template)
            # executablequery=ProcedureQuery(procedure_query, procedure_parameters, permutations_result,data_types_template)
            # procedure_result = pd.read_sql(executablequery,connect)
            # distinct_cases_dictionary[input_col_value] =ConvertDataFrameToDictionary(procedure_result,parameters_template)
    return  distinct_cases_dictionary

            # procedure_query = "Call GTST1V_API_DATA.SP_TAMWEEN_ENTITLEMENT "
            # procedure_body = "("
            # inpColList = input_col.split(",")
            # for InpColIndex in range(0,len(inpColList)):
            #     if InpColIndex == len(input_col.split(",")) -1:
            #         procedure_body += "'" + str(permutations_result.loc[0,inpColList[InpColIndex]]) + "')"
            #     else :
            #         procedure_body += "'" + str(permutations_result.loc[0, inpColList[InpColIndex]]) + "',"
            #
            # procedure_query+=procedure_body
            # print(procedure_query)
            # procedure_result = pd.read_sql(procedure_query,connect)
            # print(procedure_result)













        # input_parameter_value = str(row[])








