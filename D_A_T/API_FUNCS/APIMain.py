import D_A_T.API_FUNCS.APIFunctions as fn
def APIMain(connect,excel_path):
    sheet_name = 'AutomationSheet'
    data_types_sheet = 'DataTypes'
    param_sheet = 'ParameterColumnSheet'
    procedure_sheet = 'ProcedureSheet'
    automation_template = fn.read_excel_data(excel_path, sheet_name)
    data_types_template = fn.read_excel_data(excel_path, data_types_sheet)
    parameters_template = fn.read_excel_data(excel_path, param_sheet)
    procedure_template = fn.read_excel_data(excel_path, procedure_sheet)
    apis_unique_list = fn.apis_unique_list(automation_template)

    for client in apis_unique_list:
        permutations_flag = fn.permutations_flag(client, automation_template)
        Running_flag = fn.Running_flag(client, automation_template)
        if permutations_flag == 'Y' and Running_flag == 'Y':
            print("Running " + client + " ... ")
            permutations_query = fn.permutations_query(client, automation_template)
            DatabaseDictionary = fn.permutations_case(permutations_query, connect, client, automation_template,
                                                      data_types_template, parameters_template, 1)
            print("DataBase Dictionary Populated ... ")
            XMLDictionary = {}
            for index, row in automation_template.iterrows():
                if str(row["Client"]) == client and str(row["RunningFlag"]) == 'Y':
                    print("Running " + str(row["ServiceSlug"]) + " ... ")
                    for key in DatabaseDictionary.keys():
                        xml_request_template = fn.soap_request(client)
                        xml_request = xml_request_template.replace("ServiceSlug_value",
                                                                   str(row["ServiceSlug"])).replace(
                            str(row["InputParam"]), key)
                        xml_response = fn.soap_response(xml_request).decode()
                        # print(xml_request)
                        XMLDictionary[key] = fn.XML_parser(xml_response)
                        Comaprison_status = fn.XML_DATABASE_COMPARISON(XMLDictionary, DatabaseDictionary, key)
                        if Comaprison_status == True:
                            print(str(row["ServiceSlug"]) + " Case Passed")
                        else:
                            print(str(row["ServiceSlug"]) + " Case Failed")
                            if str(row["InputColumn"]) == "nan" or str(row["OutputParameter"]) == "nan":
                                print("Something missing in the excel please check it out  ")
                            else:
                                print("Something went wrong")

                            print(
                                "==============================================================================================")
        if permutations_flag == 'FT' and Running_flag == 'Y':
            XMLDictionary = {}
            print("Running " + client + " ... ")
            permutations_query = fn.permutations_query(client, automation_template)
            DBDictionary = fn.full_table_case(permutations_query, connect, client, automation_template,
                                              data_types_template, parameters_template)
            print("DataBase Dictionary Populated ... ")
            for index, row in automation_template.iterrows():
                if str(row["Client"]) == client and str(row["RunningFlag"]) == 'Y':
                    print("Running " + str(row["ServiceSlug"]) + " ... ")
                    for key in DBDictionary.keys():
                        xml_request_template = fn.soap_request(client)
                        xml_request = xml_request_template.replace("ServiceSlug_value",
                                                                   str(row["ServiceSlug"])).replace(
                            str(row["InputParam"]), key)
                        xml_response = fn.soap_response(xml_request).decode().replace("<ns2:value/>",
                                                                                      "<ns2:value>nan</ns2:value>")
                        XMLDictionary[key] = fn.XML_parser(xml_response)
                        Comaprison_status = fn.XML_DATABASE_COMPARISON(XMLDictionary, DBDictionary, key)
                        if Comaprison_status == True:
                            print(str(row["ServiceSlug"]) + " Case Passed")
                        else:
                            print(str(row["ServiceSlug"]) + " Case Failed")
                            if str(row["InputColumn"]) == "nan" or str(row["OutputParameter"]) == "nan":
                                print("Something missing in the excel please check it out  ")
                            else:
                                print("Something went wrong")

                        print(
                            "==============================================================================================")
        if permutations_flag == 'FCN' and Running_flag == 'Y':
            XMLDictionary = {}
            print("Running " + client + " ... ")
            permutations_query = fn.permutations_query(client, automation_template)
            DBDictionary = fn.permutations_case(permutations_query, connect, client, automation_template,
                                                data_types_template, parameters_template, 1)
            print("DataBase Dictionary Populated ... ")
            for index, row in automation_template.iterrows():
                if str(row["Client"]) == client and str(row["RunningFlag"]) == 'Y':
                    print("Running " + str(row["ServiceSlug"]) + " ... ")
                    for key in DBDictionary.keys():
                        xml_request_template = fn.soap_request(client)
                        xml_request = xml_request_template.replace("ServiceSlug_value", str(row["ServiceSlug"]))
                        Service_Slug = str(row["ServiceSlug"])
                        Input_Param = str(row["InputParam"])
                        xml_request_multiple_input = fn.soap_request_multiple_input(client, Service_Slug,
                                                                                    automation_template, xml_request,
                                                                                    Input_Param, key, connect)
                        xml_response = fn.soap_response(xml_request_multiple_input).decode().replace("<ns2:value/>",
                                                                                                     "<ns2:value>nan</ns2:value>")
                        XMLDictionary[key] = fn.XML_parser(xml_response)
                        Comaprison_status = fn.XML_DATABASE_COMPARISON(XMLDictionary, DBDictionary, key)
                        if Comaprison_status == True:
                            print(str(row["ServiceSlug"]) + " Case Passed")
                        else:
                            print(str(row["ServiceSlug"]) + " Case Failed")
                            if str(row["InputColumn"]) == "nan" or str(row["OutputParameter"]) == "nan":
                                print("Something missing in the excel please check it out  ")
                            else:
                                print("Something went wrong")

                        print(
                            "==============================================================================================")
        if permutations_flag == 'FTMI' and Running_flag == 'Y':
            XMLDictionary = {}
            print("Running " + client + " ... ")
            permutations_query = fn.permutations_query(client, automation_template)
            DBDictionary = fn.full_table_case(permutations_query, connect, client, automation_template,
                                              data_types_template, parameters_template)
            print("DataBase Dictionary Populated ... ")
            for index, row in automation_template.iterrows():
                if str(row["Client"]) == client and str(row["RunningFlag"]) == 'Y':
                    print("Running " + str(row["ServiceSlug"]) + " ... ")
                    for key in DBDictionary.keys():
                        xml_request_template = fn.soap_request(client)
                        xml_request = xml_request_template.replace("ServiceSlug_value", str(row["ServiceSlug"]))
                        Service_Slug = str(row["ServiceSlug"])
                        Input_Param = str(row["InputParam"])
                        xml_request_multiple_input = fn.soap_request_multiple_input(client, Service_Slug,
                                                                                    automation_template, xml_request,
                                                                                    Input_Param, key, connect)
                        xml_response = fn.soap_response(xml_request_multiple_input).decode().replace("<ns2:value/>",
                                                                                                     "<ns2:value>nan</ns2:value>")
                        XMLDictionary[key] = fn.XML_parser(xml_response)
                        if XMLDictionary[key]["VERIFICATION_RESULT"] == "T":
                            print(str(row["ServiceSlug"]) + " Case Passed")
                            print(key + " Case Passed")
                        else:
                            print(str(row["ServiceSlug"]) + " Case Failed")
                            if str(row["InputColumn"]) == "nan" or str(row["OutputParameter"]) == "nan":
                                print("Something missing in the excel please check it out  ")
                            else:
                                print("Something went wrong")
        if permutations_flag == 'FTM' and Running_flag == 'Y':
            XMLDictionary = {}
            print("Running " + client + " ... ")
            permutations_query = fn.permutations_query(client, automation_template)
            DBDictionary = fn.FullTableMacro(permutations_query, connect, client, automation_template,
                                             data_types_template, parameters_template, 0)
            print("DataBase Dictionary Populated ... ")
            for index, row in automation_template.iterrows():
                if str(row["Client"]) == client and str(row["RunningFlag"]) == 'Y':
                    print("Running " + str(row["ServiceSlug"]) + " ... ")
                    for key in DBDictionary.keys():
                        xml_request_template = fn.soap_request(client)
                        xml_request = xml_request_template.replace("ServiceSlug_value", str(row["ServiceSlug"]))
                        Service_Slug = str(row["ServiceSlug"])
                        Input_Param = str(row["InputParam"])
                        # print(Input_Param)
                        xml_request_multiple_input = fn.soap_request_multiple_input(client, Service_Slug,
                                                                                    automation_template, xml_request,
                                                                                    Input_Param, key, connect)
                        xml_response = fn.soap_response(xml_request_multiple_input.encode('utf-8')).decode().replace(
                            "<ns2:value/>", "<ns2:value>nan</ns2:value>")
                        XMLDictionary[key] = fn.XML_parser(xml_response)
                        Comaprison_status = fn.XML_DATABASE_COMPARISON(XMLDictionary, DBDictionary, key)
                        if Comaprison_status == True:
                            print(str(row["ServiceSlug"]) + " Case Passed")
                        else:
                            print(str(row["ServiceSlug"]) + " Case Failed")
                            if str(row["InputColumn"]) == "nan" or str(row["OutputParameter"]) == "nan":
                                print("Something missing in the excel please check it out  ")
                            else:
                                print("Something went wrong")
                        print(
                            "==============================================================================================")
        if permutations_flag == 'FTP' and Running_flag == 'Y':
            XMLDictionary = {}
            print("Running " + client + " ... ")
            permutations_query = fn.permutations_query(client, automation_template)
            DBDictionary = fn.FullTableMacro(permutations_query, connect, client, automation_template,
                                             data_types_template, parameters_template, 1)
            # print(DBDictionary)
            print("DataBase Dictionary Populated ... ")
            for index, row in automation_template.iterrows():
                if str(row["Client"]) == client and str(row["RunningFlag"]) == 'Y':
                    print("Running " + str(row["ServiceSlug"]) + " ... ")
                    for key in DBDictionary.keys():
                        xml_request_template = fn.soap_request(client)
                        xml_request = xml_request_template.replace("ServiceSlug_value", str(row["ServiceSlug"]))
                        Service_Slug = str(row["ServiceSlug"])
                        Input_Param = str(row["InputParam"])
                        # print(Input_Param)
                        xml_request_multiple_input = fn.soap_request_multiple_input(client, Service_Slug,
                                                                                    automation_template, xml_request,
                                                                                    Input_Param, key, connect)
                        xml_response = fn.soap_response(xml_request_multiple_input.encode('utf-8')).decode().replace(
                            "<ns2:value/>", "<ns2:value>nan</ns2:value>")
                        XMLDictionary[key] = fn.XML_parser(xml_response)
                        Comaprison_status = fn.XML_DATABASE_COMPARISON(XMLDictionary, DBDictionary, key)
                        if Comaprison_status == True:
                            print(str(row["ServiceSlug"]) + " Case Passed")
                        else:
                            print(str(row["ServiceSlug"]) + " Case Failed")
                            if str(row["InputColumn"]) == "nan" or str(row["OutputParameter"]) == "nan":
                                print("Something missing in the excel please check it out  ")
                            else:
                                print("Something went wrong")
                        print(
                            "==============================================================================================")
        if permutations_flag == 'MO' and Running_flag == 'Y':
            XMLDictionary = {}
            print("Running " + client + " ... ")
            permutations_query = fn.permutations_query(client, automation_template)
            DBDictionary = fn.permutations_case(permutations_query, connect, client, automation_template,
                                                data_types_template, parameters_template, 0)
            print("DataBase Dictionary Populated ... ")
            for index, row in automation_template.iterrows():
                if str(row["Client"]) == client and str(row["RunningFlag"]) == 'Y':
                    print("Running " + str(row["ServiceSlug"]) + " ... ")
                    for key in DBDictionary.keys():
                        xml_request_template = fn.soap_request(client)
                        xml_request = xml_request_template.replace("ServiceSlug_value",
                                                                   str(row["ServiceSlug"])).replace(
                            str(row["InputParam"]), key)
                        xml_response = fn.soap_response(xml_request).decode().replace("<ns2:value/>",
                                                                                      "<ns2:value>nan</ns2:value>")
                        # print(xml_request)
                        keys_length = len(DBDictionary[key])
                        XMLDictionary[key] = fn.XML_parser_MultipleOutput(xml_response, keys_length)
                        # comparisonFlag = fn.CompareMultiple(XMLDictionary, DBDictionary, key)
                        Comaprison_status = fn.CompareMultiple(XMLDictionary, DBDictionary, key)
                        if Comaprison_status == True:
                            print(str(row["ServiceSlug"]) + " Case Passed")
                        else:
                            print(str(row["ServiceSlug"]) + " Case Failed")
                            if str(row["InputColumn"]) == "nan" or str(row["OutputParameter"]) == "nan":
                                print("Something missing in the excel please check it out  ")
                            else:
                                print("Something went wrong")
                        print(
                            "==============================================================================================")
        if permutations_flag == 'BR' and Running_flag == 'Y':
            XMLDictionary = {}
            print("Running " + client + " ... ")
            permutations_query = fn.permutations_query(client, automation_template)
            DBDictionary = fn.permutationsprodcedure(permutations_query, connect, client, automation_template,
                                                     parameters_template, data_types_template, procedure_template)
            print("DataBase Dictionary Populated ... ")
            for index, row in automation_template.iterrows():
                if str(row["Client"]) == client and str(row["RunningFlag"]) == 'Y':
                    print("Running " + str(row["ServiceSlug"]) + " ... ")
                    for key in DBDictionary.keys():
                        xml_request_template = fn.soap_request(client)
                        xml_request = xml_request_template.replace("ServiceSlug_value", str(row["ServiceSlug"]))
                        Service_Slug = str(row["ServiceSlug"])
                        Input_Param = str(row["InputParam"])
                        xml_request_multiple_input = fn.soap_request_multiple_input(client, Service_Slug,
                                                                                    automation_template, xml_request,
                                                                                    Input_Param, key, connect)
                        # print(xml_request_multiple_input)
                        xml_response = fn.soap_response(xml_request).decode()
                        XMLDictionary[key] = fn.XML_parser(xml_response)
                        Comaprison_status = fn.XML_DATABASE_COMPARISON(XMLDictionary, DBDictionary, key)
                        if Comaprison_status == True:
                            print(str(row["ServiceSlug"]) + " Case Passed")
                        else:
                            print(str(row["ServiceSlug"]) + " Case Failed")
                            if str(row["InputColumn"]) == "nan" or str(row["OutputParameter"]) == "nan":
                                print("Something missing in the excel please check it out  ")
                            else:
                                print("Something went wrong")

                            print(
                                "==============================================================================================")
            # print(XMLDictionary)
