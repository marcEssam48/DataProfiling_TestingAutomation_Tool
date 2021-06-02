from django.http import HttpResponse
import D_A_T.sources_session as ss

def batching(scripts, batches,request):
    batched_scripts = []
    write_in_file(str(batches),request)

    for script in scripts:
        script = str(script).replace(";", "")
        index = 0
        for batch in batches:
            if 'where' not in script.lower() and 'group by' not in script.lower() and 'order by' not in script.lower() and 'select' in script.lower() and len(batches) != 0 and str(batches) != "['']":
                script += " where batch_id = " + batch + ""
            elif 'where' not in script.lower() and 'group by' not in script.lower() and 'order by' in script.lower() and len(batches) != 0 and str(batches) != "['']":
                script = inserting_where_and_order(script, batch)
            elif 'where' not in script.lower() and 'group by' in script.lower() and 'order by' not in script.lower() and len(batches) != 0 and str(batches) != "['']":
                script = inserting_where_and_group(script, batch)
            elif 'where' not in script.lower() and 'group by' in script.lower() and 'order by' in script.lower() and len(batches) != 0 and str(batches) != "['']":
                script = inserting_where_and_group(script, batch)
            elif 'where' in script.lower() and 'group by' in script.lower() and 'order by' in script.lower() and len(batches) != 0 and str(batches) != "['']":
                script = inserting_between_where_and_group(script, batch,index,len(batches))
            elif 'where' in script.lower() and 'group by' in script.lower() and 'order by' not in script.lower() and len(batches) != 0 and str(batches) != "['']":
                script = inserting_between_where_and_group(script, batch,index,len(batches))
            elif 'where' in script.lower() and 'group by' not in script.lower() and 'order by' in script.lower() and len(batches) != 0 and str(batches) != "['']":
                script = inserting_between_where_and_order(script, batch,index,len(batches))
            elif 'where' in script.lower() and 'group by' not in script.lower() and 'order by' not in script.lower() and len(batches) != 0 and str(batches) != "['']":
                list = []
                list.append(script)
                script = script_modification_of_anded_batches(list, batch, index, len(batches))
            elif len(batches) == 0 or str(batches) == "['']":
                batched_scripts = scripts
                break
            index+=1


        batched_scripts.append(script)

    return batched_scripts


def inserting_between_where_and_group(script, batch , index , length):
    script_modified = script.lower().split("group by")
    final_modified_script = script_modification_of_anded_batches(script_modified, batch , index , length)
    final_modified_script += " group by" + script_modified[1] + ""
    return final_modified_script

def script_modification_of_anded_batches(script, batch , index , length):
    script_modified = script
    final_modified_script = ""
    if index == 0 and index == length-1:
        final_modified_script = script_modified[0] + "and ( batch_id = " + batch + " )"
    elif index == 0 and index != length-1:
        final_modified_script = script_modified[0] + "and ( batch_id = " + batch + " "
    elif index != 0 and index == length-1 and "where batch_id" not in script_modified[0]:
        final_modified_script = script_modified[0] + " or batch_id = " + batch + " )"
    elif index != 0 and index == length-1 and "where batch_id"  in script_modified[0]:
        final_modified_script = script_modified[0] + " or batch_id = " + batch + ""
    elif index != 0 and index != length-1:
        final_modified_script = script_modified[0] + " or batch_id = " + batch + ""
    return final_modified_script

def inserting_between_where_and_order(script, batch , index, length):
    script_modified = script.lower().split("order by")
    final_modified_script = script_modification_of_anded_batches(script_modified, batch, index, length)
    final_modified_script += " order by" + script_modified[1] + ""
    return final_modified_script


def inserting_where_and_order(script, batch):
    script_modified = script.lower().split("order by")
    final_modified_script = ""
    final_modified_script = script_modified[0] + " where batch_id = " + batch + ""
    final_modified_script += " order by" + script_modified[1] + ""
    return final_modified_script


def inserting_where_and_group(script, batch):
    script_modified = script.lower().split("group by")
    final_modified_script = ""
    final_modified_script = script_modified[0] + " where batch_id = " + batch + ""
    final_modified_script += " group by" + script_modified[1] + ""
    return final_modified_script


def string_to_list(request):

    batch_file_path = ss.batch_file_path(request)
    file = open(batch_file_path, "r")
    batches_string = file.read()
    file.close()
    batch_ids = str(batches_string).split(",")
    new_batch = []
    for batch in batch_ids:
        new_batch.append(batch.replace("'", "").replace("[", "").replace("]", "").replace(" ", ""))
    return new_batch


def write_in_file(batches,request):
    batch_file_path = ss.batch_file_path(request)
    file = open(batch_file_path, "w")
    batches_string = file.write(batches)
    file.close()

