from django.db import models
from django.conf import settings


class user_connection(models.Model):
    id = models.AutoField(primary_key=True)
    user_id = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    database_type = models.IntegerField()
    username = models.CharField(max_length=100)
    password = models.CharField(max_length=100)
    ip_address = models.CharField(max_length=15)
    connection_name = models.CharField(max_length=100)
    def __str__(self):
        return self.ip_address

class user_note(models.Model):
    id = models.AutoField(primary_key=True)
    user_id = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    note =  models.CharField(max_length=1000)
    note_date = models.CharField(max_length=50)
    def __str__(self):
        return self.note

class user_query(models.Model):
    id = models.AutoField(primary_key=True)
    user_id = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    query =  models.CharField(max_length=1000)
    category = models.CharField(max_length=200)
    check_name = models.CharField(max_length=200)
    check_description = models.CharField(max_length=1000)
    query_date = models.CharField(max_length=50)
    connection_id = models.CharField(max_length=1000)
    db_name = models.CharField(max_length=500)
    def __str__(self):
        return self.query

class user_source(models.Model):
    id = models.AutoField(primary_key=True)
    connection_id = models.ForeignKey(user_connection, on_delete=models.CASCADE)
    database = models.CharField(max_length=100)
    source = models.CharField(max_length=100)
    tables = models.CharField(max_length=10000)
    def __str__(self):
        return self.source
class test_cases(models.Model):
    id = models.AutoField(primary_key=True)
    # database = models.CharField(max_length=100)
    user_id = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    source_id = models.ForeignKey(user_source,on_delete=models.CASCADE)
    date = models.CharField(max_length=50)

    def __str__(self):
        return self.source_id
    
class statistics(models.Model):
    id = models.AutoField(primary_key=True)
    # database = models.CharField(max_length=100)
    user_id = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    source_id = models.ForeignKey(user_source, on_delete=models.CASCADE)
    date = models.CharField(max_length=50)
    def __str__(self):
        return self.source_id
    
class data_quality(models.Model):
    id = models.AutoField(primary_key=True)
    # database = models.CharField(max_length=100)
    user_id = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    source_id = models.ForeignKey(user_source, on_delete=models.CASCADE)
    date = models.CharField(max_length=50)
    def __str__(self):
        return self.source_id

# class logs(models.Model):
#     id = models.AutoField(primary_key=True)
#     test_case_id = models.ForeignKey(test_cases, on_delete=models.CASCADE)
#     statistics_id = models.ForeignKey(statistics, on_delete=models.CASCADE)
#     data_quality_id = models.ForeignKey(data_quality, on_delete=models.CASCADE)
#     user_id = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
#     date = models.CharField(max_length=50)
#     def __str__(self):
#         return self.date

class test_case_result(models.Model):
    id = models.AutoField(primary_key=True)
    test_case_id = models.ForeignKey(test_cases,on_delete=models.CASCADE)
    check_name = models.CharField(max_length=100)
    table_name = models.CharField(max_length=100)
    result = models.CharField(max_length=10000)
    status = models.CharField(max_length=15)
    def __str__(self):
        return self.result


    
class statistics_results(models.Model):
    id = models.AutoField(primary_key=True)
    statistics_id = models.ForeignKey(statistics, on_delete=models.CASCADE)
    table_name = models.CharField(max_length=100)
    no_of_columns = models.IntegerField()
    no_of_rows = models.IntegerField()
    def __str__(self):
        return self.statistics_id

class more_statistics_results(models.Model):
    id = models.AutoField(primary_key=True)
    statistics_id = models.ForeignKey(statistics, on_delete=models.CASCADE)
    table_name = models.CharField(max_length=100)
    column_name = models.CharField(max_length=100)
    null_percentage = models.CharField(max_length=100)
    distinct_values = models.CharField(max_length=100)
    null_values = models.IntegerField()
    maximum = models.CharField(max_length=100)
    minimum = models.CharField(max_length=100)
    def __str__(self):
        return self.statistics_id


class data_quality_results(models.Model):
    id = models.AutoField(primary_key=True)
    data_quality_id = models.ForeignKey(data_quality, on_delete=models.CASCADE)
    table_name = models.CharField(max_length=100)
    column_name = models.CharField(max_length=100)
    category_name = models.CharField(max_length=100)
    check_name = models.CharField(max_length=100)
    result = models.CharField(max_length=100)
    def __str__(self):
        return self.data_quality_id
    
    
    
    
    
    
    
    

