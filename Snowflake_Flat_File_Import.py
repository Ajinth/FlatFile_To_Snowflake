
# coding: utf-8

# # Purpose
# 
# The purpose of this python notebook is to read the list of excel files from a specific folder and stage it into a snowflake schema. 

# # Prerequisites

# 1) Anaconda installation of python - preferrably python 3
# 
# 2) Snowflake connector for python (pip install --upgrade snowflake-connector-python)
# 
# 3) Sqlalchemy connector for snowflake (pip install --upgrade snowflake-sqlalchemy) 

# # Instructions to use this Notebook
# 
# 1) Update the credentials for snowflake environments in the global variable section. The following variables should be updated - user, password, acount, database,schema, warehouse and role  
# 
# 2) Update the "skip_rows" variable to indicate the number of rows to be skipped while reading into a dataframe
# 
# 3) Update the "sheet" variable to indicate the sheet name to be read into a dataframe
# 
# 4) Copy the flat files (excels) to the same location as this python notebook 
# 
# 5) Update the Source_File excel with the list of files names (include extension) in column A. Provide the table name in column B 
# 
# 6)  To execute the notebook click on "Kernel" and select "Restart and Run all"
# 

# # Importing the needed Libraries

# In[ ]:


#Basic Python Libraries 
import pandas as pd
import os
import string
import pprint
import xlrd
import datetime 

#Python Libraries for Snowflake 
import snowflake.connector
from sqlalchemy import create_engine

#Code to hide warnings 
import warnings
warnings.filterwarnings('ignore')


# # Global Variables

# In[ ]:


# Generic Global Variables

'''Credentials for Snowflake Environments'''

user='<Enter the username>'
password='<Enter the password>'
account='<Enter the Snowflake account>'
database = '<Enter the database name>'
schema = '<Enter the schema name>'
warehouse = '<Enter the warehouse name>'
role='<Enter the role name>'


'''Flat File Details'''

file ='<Enter the flat file name>'
skip_rows=6
sheet='Main_Data'
batch_size = 5000

'''Variables for Connection Initializer'''
use_db = 'USE DATABASE ' + str(database)
use_warehouse = 'USE WAREHOUSE ' + str(warehouse)
use_schema = 'USE SCHEMA ' + str(schema)

# Initializing the lists to capture log metrics 
source_file_list = [] 
source_count_list = []
target_table_list =[]
target_table_count_list = [] 
load_date_time_list = []
zippped_list = []



# # Section for Generic Functions 

# In[ ]:


# Function to create the Snowflake Engine 
'''This function takes three inputs user, password and account and returns an engine object'''
def engine_creator(user, password, account): 
    engine_input_string = 'snowflake://' + str(user) + ':' + str(password)+ '@' + str(account)
    engine = create_engine(engine_input_string)
    return engine


# In[ ]:


# Function to create the Snowflake connection from the engine
'''This function takes the engine object as input and creates a connection object'''
def engine_connection(engine): 
    connection = engine.connect()
    return connection


# In[ ]:


#Function to initialize the connection 
'''This function takes connection, database, warehouse and schema as input and initializes the connection'''
def connection_setup(connection, use_db, use_warehouse, use_schema): 
    connection.execute(use_db)
    connection.execute(use_warehouse)
    connection.execute(use_schema)
    pass


# In[ ]:


#Function to commit to snowflake 
def commit_sql(connection): 
    connection.execute('Commit')
    pass 


# In[ ]:


# Function to create the Source Dictionary from the input parameter file 
'''This function takes a source file and creates a python dictionary '''
def source_dictionary_generator(file): 
    workbook=xlrd.open_workbook(file)
    sheet=workbook.sheet_by_index(0)
    
    col_a = sheet.col_values(0, 1)
    col_b = sheet.col_values(1, 1)
    
    source_dictionary = {a : b for a, b in zip(col_a, col_b)}
    
    return source_dictionary


# In[ ]:


#Function to create the dataframe
'''It takes a file name and the sheet name as arguments and returns a dataframe''' 
def create_df(file_name,sheet,skip_rows): 
    df = pd.read_excel(file_name, skiprows=skip_rows, sheet_name=sheet)
    return df 


# In[ ]:


# Function to bulk load into Snowflake 

def bulk_load(connection, source_dictionary,schema, sheet): 
    '''Loop to process one file at a time and load into snowflake'''
    
    for key, value in source_dictionary.items(): 
        
        df = create_df(key,sheet,skip_rows)
        
        '''Added to convert all values in a dataframe in string. Added to handle the SFDC-SLGX reports'''
        df = df.applymap(str)
        df_length = len(df)
        
        connection_setup(connection, use_db, use_warehouse, use_schema)
        
        '''Truncate table if already exists'''
        drop_sql = 'DROP TABLE IF EXISTS ' + '"' + str(database) + '"' + "." + '"' + str(schema) + '"' + "." + '"' + str(value) + '"'
        print(drop_sql)
        connection.execute(drop_sql)
        
        '''Batching the dataframe to process it in chunks of 5000 batch sizes'''
        print("LOADING SNAPSHOT {} INTO SNOWFLAKE".format(key))
        for batch_start in range(0,df_length, 5000): 
            batch_end = batch_start + batch_size 
            if batch_end > df_length: 
                batch_remaining = df_length - batch_start
                batch_end = batch_start + batch_remaining
                df_part = df.iloc[batch_start:batch_end]
                print ("LOADING BATCHES {} to {} now".format(batch_start, batch_end))
                df_part.to_sql(value, con=connection, schema=schema, if_exists='append', index=False)
                commit_sql(connection)
                
            else: 
                df_part = df.iloc[batch_start:batch_end]
                print ("LOADING BATCHES {} to {} now".format(batch_start, batch_end))
                df_part.to_sql(value, con=connection, schema=schema, if_exists='append', index=False)
                commit_sql(connection)
                   
        print("LOADED SNAPSHOT {} INTO SNOWFLAKE".format(key))
        source_count = len(df)
        target_count_sql = "SELECT COUNT(*) FROM " + '"' + str(database) + '"' + "." + '"' + str(schema) + '"' + "." + '"' + str(value) + '"'
        target_count = connection.execute(target_count_sql).fetchone()[0]
        load_date_time = datetime.datetime.today().strftime("%d-%B-%Y %H:%M:%S")
        
        '''Creating lists to store source fle name, source count, target table name, target table count, and load date-time'''
        source_file_list.append(key)
        source_count_list.append(source_count)
        target_table_list.append(value)
        target_table_count_list.append(target_count)
        load_date_time_list.append(load_date_time)
        
    '''Creating the dataframe to store the log information'''
    zippped_list = list(zip(source_file_list, source_count_list, target_table_list, target_table_count_list, load_date_time_list))
    df_log = pd.DataFrame(zippped_list, columns=['Source_File', 'Source_Count', 'Target_Table', 'Target_Count', 'Load_DateTime'])
        

    print ("-----------------------------------------------------")
    print ("LOAD INTO SNOWFLAKE COMPLETED, UPDATING LOG TABLE")
    print ("-----------------------------------------------------")
    
    return df_log
         


# In[ ]:


#Function to add logging 

def logger(connection,o_bulk_load):
    o_bulk_load.to_sql('IMPORT_LOG', con=connection, schema=schema, if_exists='append', index=False)
    commit_sql(connection)
    
    print ("-----------------------------------------------------")
    print ("LOG TABLE UPDATED, PROCESS COMPLETE. PLEASE VALIDATE SNOWFLAKE")
    print ("-----------------------------------------------------")
    
    pass 


# # Section to execute Functions

# In[ ]:


def main():
    engine = engine_creator(user, password, account)
    connection = engine_connection(engine)
    source_dictionary = source_dictionary_generator(file)
    o_bulk_load = bulk_load(connection, source_dictionary, schema, sheet)
    o_logger = logger(connection, o_bulk_load)
    pass
    


# In[ ]:


if __name__== "__main__":
  main()

