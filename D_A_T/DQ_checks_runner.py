import pandas as pd
import  D_A_T.db_connections as db
# from django.http import HttpResponse
import teradatasql
import D_A_T.batching as batcher
def replace_scripts(check_path,data,request):
    scripts = []
    batched_scripts = []
    print("REPLACING ...")

    with open(check_path, "r") as f:
        script_unreplaced = f.read()
        for i in range(0, len(data)):
            script = script_unreplaced.replace("db", str(data.loc[i, "Database"])).replace("table", str(data.loc[i, "Table"])).replace("column", str(data.loc[i, "Column"])).replace("\n"," ")
            scripts.append(script)
            print(scripts)
        batch_ids = batcher.string_to_list(request)
        print("BATCH IDS PASSED")
        print(batch_ids)
        if str(batch_ids).strip() == "['']" or str(batch_ids) == '' or str(batch_ids) == "[]":
            print("NO batch ids")
            batched_scripts = scripts
        else:
            print("THERE ARE BATCH_IDS")
            try:
                batched_scripts = batcher.batching(scripts,batch_ids,request)
            except:
                batched_scripts = scripts
        print(batched_scripts)
    return batched_scripts


def run_dq_checks_for_category(path , checks , request):

    data = pd.read_csv(path)
    check_path = ""
    scripts_list = []
    scripts_category_dictionary = {}
    prev_category = []
    for check in checks :
        check_path = "D_A_T/DQ_Checks/"+check
        category = str(check).split(".", 1)[0]

        scripts_list.append(replace_scripts(check_path,data,request))
        scripts_category_dictionary[category] = scripts_list

        if category not in prev_category:
            prev_category.append(category)
        else:
            scripts_list = []
    return  scripts_category_dictionary

def run_dq_checks(path , checks,db_type,request):

    data = pd.read_csv(path)
    check_path = ""
    scripts_list = []
    scripts_category_dictionary = {}
    prev_category = []
    check_path = ""
    for check in checks :
        print(check)
        if db_type != "2":
            check_path = "D_A_T/DQ_Checks/"+check
        else:
            check_path = "D_A_T/T_mode_dq_checks/" + check

        category = str(check).split(".", 1)[0]

        scripts_list.append(replace_scripts(check_path,data , request))
        scripts_category_dictionary[check] = scripts_list
        print(scripts_list)


        scripts_list = []
    return  scripts_category_dictionary



def accuracy_category(scripts_category_dictionary,category,ip,username,password):
    score = 0
    accuracy_scores = []
    for script in scripts_category_dictionary[category]:
        for sql in script:
            connect = db.myssql_connection(ip,username,password)
            query = pd.read_sql(sql,connect)
            for head in query.head():
                for x in range(0,len(query)):
                    try:
                        score = float(str(query.loc[x,head]).replace("%",""))
                    except:
                        continue
                    if score > 10 :
                        accuracy_scores.append(score)
    return  sum(accuracy_scores)/len(accuracy_scores)


def accuracy_percentage_check(scripts_category_dictionary,check,ip,username,password,check_name,category_name,db_type):
    score = 0
    final_score = []
    list_of_results = []

    accuracy_scores = []

    for script in scripts_category_dictionary[check]:
        for sql in script:
            if db_type != "2":
                connect = db.myssql_connection(ip,username,password)
                query = pd.read_sql(sql,connect)
                for head in query.head():
                    for x in range(0,len(query)):
                        try:
                            score = float(str(query.loc[x,head]).replace("%",""))
                        except:
                            continue
                        if score > 10 :
                            head_split = head.split(".")
                            final_score.append(head_split[0])
                            final_score.append(head_split[1])
                            final_score.append(head_split[2])
                            final_score.append(round(score,4))
                            final_score.append(check_name)
                            final_score.append(category_name)
                            list_of_results.append(final_score)
                            final_score = []
            else:
                with teradatasql.connect(host=ip, user=username, password=password, encryptdata=True) as connect:
                    query = ""
                    try:
                        query = pd.read_sql(sql, connect)
                    except:
                        continue
                    for head in query.head():
                        for x in range(0, len(query)):
                            try:
                                score = float(str(query.loc[x, head]).replace("%", ""))
                            except:
                                continue
                            if score > 10:
                                head_split = head.split(".")
                                final_score.append(head_split[0])
                                final_score.append(head_split[1])
                                final_score.append(head_split[2])
                                final_score.append(round(score, 4))
                                final_score.append(check_name)
                                final_score.append(category_name)
                                list_of_results.append(final_score)
                                final_score = []
                        # accuracy_scores.append(score)
    # try:
        # final_score = sum(accuracy_scores)/len(accuracy_scores)
    # except:
        # final_score =0
    return  list_of_results














