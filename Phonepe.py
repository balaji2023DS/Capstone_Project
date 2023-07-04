import streamlit as st
import pandas as pd
import mysql.connector
import os
import json
import plotly.express as px
import plotly.colors as pc
import geopandas as gpd
import matplotlib.pyplot as plt
import plotly.subplots as sp
import plotly.graph_objects as go
import git
import shutil

# Create layout using Streamlit columns
col1, col2, col3, col4 = st.columns(4)

@st.cache_data
# Clone the data from gitHub to local directory
def Phonepe_Clone(param=None):
    if param is None:
        repository_url = 'https://github.com/PhonePe/pulse.git'
        local_directory = r'D:\Guvi\Capstone_Project\PhonePe\Git'
        git.Repo.clone_from(repository_url, local_directory)

        unwanted_files = ['README.md', 'LICENSE', '.gitignore']
        for file_name in unwanted_files:
            file_path = os.path.join(local_directory, file_name)
            if os.path.exists(file_path):
                if os.path.isfile(file_path):
                    os.remove(file_path)
                else:
                    shutil.rmtree(file_path)


# aggregated-Transaction-Country-State
def Agg_Txn_Cntry_State(param=None):
    if param is None:
        path = r"D:\Guvi\Capstone_Project\PhonePe\Git\data\aggregated\transaction\country\india\state"
        clm = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [],
               'Transaction_amount': []}

        Agg_state_list = os.listdir(path)
        # Agg_state_list
        for i in Agg_state_list:
            p_i = path + "/" + i + "/"
            Agg_yr = os.listdir(p_i)
            for j in Agg_yr:
                p_j = p_i + "/" + j + "/"
                Agg_yr_list = os.listdir(p_j)
                for k in Agg_yr_list:
                    p_k = p_j + k
                    Data = open(p_k, 'r')
                    D = json.load(Data)
                    for z in D['data']['transactionData']:
                        Name = z['name']
                        count = z['paymentInstruments'][0]['count']
                        amount = z['paymentInstruments'][0]['amount']
                        clm['Transaction_type'].append(Name)
                        clm['Transaction_count'].append(count)
                        clm['Transaction_amount'].append(amount)
                        clm['State'].append(i)
                        clm['Year'].append(j)
                        clm['Quarter'].append(int(k.strip('.json')))
        # Succesfully created a dataframe
        Agg_Trans = pd.DataFrame(clm)
        Agg_Trans

        # print(mysql_db_connector)
        try:
            mysql_db_connector = mysql.connector.connect(host="localhost", user="root", password="mysql@123",
                                                         auth_plugin='mysql_native_password',
                                                         database="data_science", charset='utf8mb4')
            mysql_db_cursor = mysql_db_connector.cursor()
            sql = '''create table IF NOT EXISTS Agg_Txns_Country_State (
            State varchar(255), 
            Year varchar(255),
            Quarter varchar(255),
            Transaction_type varchar(255), 
            Transaction_count int, 
            Transaction_amount float
            )
            CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            '''
            mysql_db_cursor.execute(sql)

            Agg_Trans = Agg_Trans.fillna(0)
            for i, row in Agg_Trans.iterrows():
                sql1 = 'insert into Agg_Txns_Country_State(State,Year, Quarter,Transaction_type, Transaction_count,Transaction_amount) values (%s, %s, %s, %s, %s, %s)'
                mysql_db_cursor.execute(sql1, tuple(row))
                mysql_db_connector.commit()
            print(mysql_db_cursor.rowcount, "details inserted")
            # disconnecting from server
            mysql_db_connector.close()

        except:
            mysql_db_connector.close()


# aggregated-User-country-state
def Agg_User_Cntry_State(param=None):
    if param is None:
        path = r"D:\Guvi\Capstone_Project\PhonePe\Git\data\aggregated\user\country\india\state"
        clm = {'State': [], 'Year': [], 'Quarter': [], 'RegisteredUsers': [], 'appOpens': [], 'brand': [], 'count': [],
               'percentage': []
               }

        Agg_state_list = os.listdir(path)
        # Agg_state_list
        for i in Agg_state_list:
            p_i = path + "/" + i + "/"
            Agg_yr = os.listdir(p_i)
            for j in Agg_yr:
                p_j = p_i + "/" + j + "/"
                Agg_yr_list = os.listdir(p_j)
                # print(Agg_yr_list)
                for k in Agg_yr_list:
                    p_k = p_j + k
                    Data = open(p_k, 'r')
                    data_dict = json.load(Data)
                    # print(data_dict)
                    registered_users = data_dict['data']['aggregated']['registeredUsers']
                    app_opens = data_dict['data']['aggregated']['appOpens']
                    users_by_device = data_dict["data"]["usersByDevice"]
                    # print(users_by_device)
                    if users_by_device is not None:
                        # print(users_by_device)
                        for user_device in users_by_device:
                            brand = user_device["brand"]
                            count = user_device["count"]
                            percentage = user_device["percentage"]
                            # print(brand)
                            # print(count)
                            # print(percentage)
                            clm['RegisteredUsers'].append(registered_users)
                            clm['appOpens'].append(app_opens)
                            clm['brand'].append(brand)
                            clm['count'].append(count)
                            clm['percentage'].append(percentage)
                            clm['State'].append(i)
                            clm['Year'].append(j)
                            clm['Quarter'].append(int(k.strip('.json')))
                    else:
                        clm['RegisteredUsers'].append(registered_users)
                        clm['appOpens'].append(app_opens)
                        clm['brand'].append('None')
                        clm['count'].append('0')
                        clm['percentage'].append('0')
                        clm['State'].append(i)
                        clm['Year'].append(j)
                        clm['Quarter'].append(int(k.strip('.json')))

        # Succesfully created a dataframe
        Agg_User = pd.DataFrame(clm)
        Agg_User

        try:
            mysql_db_connector = mysql.connector.connect(host="localhost", user="root", password="mysql@123",
                                                         auth_plugin='mysql_native_password',
                                                         database="data_science", charset='utf8mb4')
            mysql_db_cursor = mysql_db_connector.cursor()
            sql = '''create table IF NOT EXISTS User_Txns_Country_State (
            State varchar(255), 
            Year varchar(255),
            Quarter varchar(255),
            RegisteredUsers bigint, 
            appOpens bigint, 
            brand varchar(255),
            count bigint,
            percentage float
            )
            CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            '''
            mysql_db_cursor.execute(sql)

            Agg_User = Agg_User.fillna(0)
            # Agg_User
            for i, row in Agg_User.iterrows():
                sql1 = 'insert into User_Txns_Country_State(State,Year, Quarter,RegisteredUsers,appOpens,brand,count,percentage) values (%s, %s, %s, %s, %s, %s, %s, %s)'
                mysql_db_cursor.execute(sql1, tuple(row))
                mysql_db_connector.commit()
            print(mysql_db_cursor.rowcount, "details inserted")
            # disconnecting from server
            mysql_db_connector.close()
        except:
            mysql_db_connector.close()


# Map-Transaction-Country-State
def Map_Txn_Cntry_State(param=None):
    if param is None:
        path = r"D:\Guvi\Capstone_Project\PhonePe\Git\data\map\transaction\hover\country\india\state"
        clm = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Transaction_count': [], 'Transaction_amount': []}

        Agg_state_list = os.listdir(path)
        # Agg_state_list
        for i in Agg_state_list:
            p_i = path + "/" + i + "/"
            Agg_yr = os.listdir(p_i)
            for j in Agg_yr:
                p_j = p_i + "/" + j + "/"
                Agg_yr_list = os.listdir(p_j)
                for k in Agg_yr_list:
                    p_k = p_j + k
                    Data = open(p_k, 'r')
                    D = json.load(Data)
                    for z in D['data']['hoverDataList']:
                        Name = z['name']
                        count = z['metric'][0]['count']
                        amount = z['metric'][0]['amount']

                        # clm['District'].append(Name.strip('district'))
                        clm['District'].append(Name)
                        clm['Transaction_count'].append(count)
                        clm['Transaction_amount'].append(amount)
                        clm['State'].append(i)
                        clm['Year'].append(j)
                        clm['Quarter'].append(int(k.strip('.json')))
        # Succesfully created a dataframe
        Map_Trans = pd.DataFrame(clm)
        Map_Trans

        try:
            mysql_db_connector = mysql.connector.connect(host="localhost", user="root", password="mysql@123",
                                                         auth_plugin='mysql_native_password',
                                                         database="data_science", charset='utf8mb4')

            mysql_db_cursor = mysql_db_connector.cursor()
            sql = '''create table IF NOT EXISTS Map_Txns_Country_State (
            State varchar(255), 
            Year varchar(255),
            Quarter varchar(255),
            District varchar(255), 
            Transaction_count int, 
            Transaction_amount float
            )
            CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            '''
            mysql_db_cursor.execute(sql)

            Map_Trans = Map_Trans.fillna(0)
            for i, row in Map_Trans.iterrows():
                sql1 = 'insert into Map_Txns_Country_State(State,Year, Quarter,District, Transaction_count,Transaction_amount) values (%s, %s, %s, %s, %s, %s)'
                mysql_db_cursor.execute(sql1, tuple(row))
                mysql_db_connector.commit()
            print(mysql_db_cursor.rowcount, "details inserted")
            # disconnecting from server
            mysql_db_connector.close()
        except:
            mysql_db_connector.close()


# Map-User-Country-State
def Map_User_Cntry_State(param=None):
    if param is None:
        path = r"D:\Guvi\Capstone_Project\PhonePe\Git\data\map\user\hover\country\india\state"
        clm = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'registeredUsers': [], 'appOpens': []}

        Agg_state_list = os.listdir(path)
        # Agg_state_list
        for i in Agg_state_list:
            p_i = path + "/" + i + "/"
            Agg_yr = os.listdir(p_i)
            for j in Agg_yr:
                p_j = p_i + "/" + j + "/"
                Agg_yr_list = os.listdir(p_j)
                for k in Agg_yr_list:
                    p_k = p_j + k
                    Data = open(p_k, 'r')
                    data_dict = json.load(Data)
                    # Access the hoverData dictionary
                    hover_data = data_dict["data"]["hoverData"]
                    # Iterate over the hoverData dictionary
                    for district, district_data in hover_data.items():
                        registered_users = district_data["registeredUsers"]
                        app_opens = district_data["appOpens"]

                        clm['District'].append(district)  # strip('district')
                        clm['registeredUsers'].append(registered_users)
                        clm['appOpens'].append(app_opens)
                        clm['State'].append(i)
                        clm['Year'].append(j)
                        clm['Quarter'].append(int(k.strip('.json')))
        # Succesfully created a dataframe
        Map_User = pd.DataFrame(clm)
        Map_User

        try:
            mysql_db_connector = mysql.connector.connect(host="localhost", user="root", password="mysql@123",
                                                         auth_plugin='mysql_native_password',
                                                         database="data_science", charset='utf8mb4')
            mysql_db_cursor = mysql_db_connector.cursor()
            sql = '''create table IF NOT EXISTS Map_User_Country_State (
            State varchar(255), 
            Year varchar(255),
            Quarter varchar(255),
            District varchar(255),
            RegisteredUsers bigint, 
            appOpens bigint
            )
            CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            '''
            mysql_db_cursor.execute(sql)

            Map_User = Map_User.fillna(0)
            # Map_User
            for i, row in Map_User.iterrows():
                sql1 = 'insert into Map_User_Country_State(State,Year, Quarter,District,RegisteredUsers,appOpens) values (%s, %s, %s, %s, %s, %s)'
                mysql_db_cursor.execute(sql1, tuple(row))
                mysql_db_connector.commit()
            print(mysql_db_cursor.rowcount, "details inserted")
            # disconnecting from server
            mysql_db_connector.close()
        except:
            mysql_db_connector.close()


# top-Transaction-Country-State-District
def Top_Txn_Cntry_State_Distrcit(param=None):
    if param is None:
        path = r"D:\Guvi\Capstone_Project\PhonePe\Git\data\top\transaction\country\india\state"
        clm = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Transaction_count': [], 'Transaction_amount': []}
        Agg_state_list = os.listdir(path)
        # Agg_state_list
        for i in Agg_state_list:
            p_i = path + "/" + i + "/"
            Agg_yr = os.listdir(p_i)
            for j in Agg_yr:
                p_j = p_i + "/" + j + "/"
                Agg_yr_list = os.listdir(p_j)
                for k in Agg_yr_list:
                    p_k = p_j + k
                    Data = open(p_k, 'r')
                    D = json.load(Data)
                    for z in D['data']['districts']:
                        Name = z['entityName']
                        count = z['metric']['count']
                        amount = z['metric']['amount']

                        clm['District'].append(Name)
                        clm['Transaction_count'].append(count)
                        clm['Transaction_amount'].append(amount)
                        clm['State'].append(i)
                        clm['Year'].append(j)
                        clm['Quarter'].append(int(k.strip('.json')))
        # Succesfully created a dataframe
        Top_Trans_Districts = pd.DataFrame(clm)
        Top_Trans_Districts

        try:
            mysql_db_connector = mysql.connector.connect(host="localhost", user="root", password="mysql@123",
                                                         auth_plugin='mysql_native_password',
                                                         database="data_science", charset='utf8mb4')
            mysql_db_cursor = mysql_db_connector.cursor()
            sql = '''create table IF NOT EXISTS Top_Txn_Cntry_State_Dist (
            State varchar(255), 
            Year varchar(255),
            Quarter varchar(255),
            District varchar(255),
            Transaction_count bigint, 
            Transaction_amount bigint
            )
            CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            '''
            mysql_db_cursor.execute(sql)

            Top_Trans_Districts = Top_Trans_Districts.fillna(0)
            # Top_Trans_Districts
            for i, row in Top_Trans_Districts.iterrows():
                sql1 = 'insert into Top_Txn_Cntry_State_Dist(State,Year, Quarter,District,Transaction_count,Transaction_amount) values (%s, %s, %s, %s, %s, %s)'
                mysql_db_cursor.execute(sql1, tuple(row))
                mysql_db_connector.commit()
            print(mysql_db_cursor.rowcount, "details inserted")
            # disconnecting from server
            mysql_db_connector.close()
        except:
            mysql_db_connector.close()


# top-Transaction-Country-State-Pincode
def Top_Txn_Cntry_State_Pincode(param=None):
    if param is None:
        path = r"D:\Guvi\Capstone_Project\PhonePe\Git\data\top\transaction\country\india\state"
        clm = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [], 'Pincode_Transaction_count': [],
               'Pincode_Transaction_amount': []}

        Agg_state_list = os.listdir(path)
        # Agg_state_list
        for i in Agg_state_list:
            p_i = path + "/" + i + "/"
            Agg_yr = os.listdir(p_i)
            for j in Agg_yr:
                p_j = p_i + "/" + j + "/"
                Agg_yr_list = os.listdir(p_j)
                for k in Agg_yr_list:
                    p_k = p_j + k
                    Data = open(p_k, 'r')
                    D = json.load(Data)

                    for p in D['data']['pincodes']:
                        pincodes = p['entityName']
                        pin_count = p['metric']['count']
                        pin_amount = p['metric']['amount']

                        clm['Pincode'].append(pincodes)
                        clm['Pincode_Transaction_count'].append(pin_count)
                        clm['Pincode_Transaction_amount'].append(pin_amount)
                        clm['State'].append(i)
                        clm['Year'].append(j)
                        clm['Quarter'].append(int(k.strip('.json')))
        # Succesfully created a dataframe
        Top_Trans_Pincode = pd.DataFrame(clm)
        Top_Trans_Pincode

        try:
            mysql_db_connector = mysql.connector.connect(host="localhost", user="root", password="mysql@123",
                                                         auth_plugin='mysql_native_password',
                                                         database="data_science", charset='utf8mb4')
            mysql_db_cursor = mysql_db_connector.cursor()
            sql = '''create table IF NOT EXISTS Top_Txn_Cntry_State_Pincode (
            State varchar(255), 
            Year varchar(255),
            Quarter varchar(255),
            Pincode int,
            Pincode_Transaction_count bigint,
            Pincode_Transaction_amount bigint
            )
            CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            '''
            mysql_db_cursor.execute(sql)

            Top_Trans_Pincode = Top_Trans_Pincode.fillna(0)
            # Top_Trans_Pincode
            for i, row in Top_Trans_Pincode.iterrows():
                sql1 = 'insert into Top_Txn_Cntry_State_Pincode(State,Year, Quarter,Pincode,Pincode_Transaction_count,Pincode_Transaction_amount) values ( %s, %s, %s,  %s, %s, %s)'
                mysql_db_cursor.execute(sql1, tuple(row))
                mysql_db_connector.commit()
            print(mysql_db_cursor.rowcount, "details inserted")
            # disconnecting from server
            mysql_db_connector.close()
        except:
            mysql_db_connector.close()


# top-User-Country-State-District
def Top_user_Cntry_State_District(param=None):
    if param is None:
        path = r"D:\Guvi\Capstone_Project\PhonePe\Git\data\top\user\country\india\state"
        clm = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'RegisteredUsers': []}
        Agg_state_list = os.listdir(path)
        # Agg_state_list
        for i in Agg_state_list:
            p_i = path + "/" + i + "/"
            Agg_yr = os.listdir(p_i)
            for j in Agg_yr:
                p_j = p_i + "/" + j + "/"
                Agg_yr_list = os.listdir(p_j)
                for k in Agg_yr_list:
                    p_k = p_j + k
                    Data = open(p_k, 'r')
                    D = json.load(Data)
                    for z in D['data']['districts']:
                        district = z['name']
                        registeredUsers = z['registeredUsers']

                        clm['District'].append(district)
                        clm['RegisteredUsers'].append(registeredUsers)
                        clm['State'].append(i)
                        clm['Year'].append(j)
                        clm['Quarter'].append(int(k.strip('.json')))
        # Succesfully created a dataframe
        Top_User_District = pd.DataFrame(clm)
        Top_User_District

        try:
            mysql_db_connector = mysql.connector.connect(host="localhost", user="root", password="mysql@123",
                                                         auth_plugin='mysql_native_password',
                                                         database="data_science", charset='utf8mb4')
            mysql_db_cursor = mysql_db_connector.cursor()
            sql = '''create table IF NOT EXISTS Top_User_Cntry_State_Dist (
            State varchar(255), 
            Year varchar(255),
            Quarter varchar(255),
            District varchar(255),
            RegisteredUsers bigint
            )
            CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            '''
            mysql_db_cursor.execute(sql)

            Top_User_District = Top_User_District.fillna(0)
            # Top_User
            for i, row in Top_User_District.iterrows():
                sql1 = 'insert into Top_User_Cntry_State_Dist(State,Year, Quarter,District,RegisteredUsers) values (%s, %s, %s, %s, %s)'
                mysql_db_cursor.execute(sql1, tuple(row))
                mysql_db_connector.commit()
            print(mysql_db_cursor.rowcount, "details inserted")
            # disconnecting from server
            mysql_db_connector.close()
        except:
            mysql_db_connector.close()


# top-User-Country-State-Pincode
def Top_User_Cntry_State_Pincode(param=None):
    if param is None:
        path = r"D:\Guvi\Capstone_Project\PhonePe\Git\data\top\user\country\india\state"
        clm = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [], 'Pincode_RegisteredUsers': []}
        Agg_state_list = os.listdir(path)
        # Agg_state_list
        for i in Agg_state_list:
            p_i = path + "/" + i + "/"
            Agg_yr = os.listdir(p_i)
            for j in Agg_yr:
                p_j = p_i + "/" + j + "/"
                Agg_yr_list = os.listdir(p_j)
                for k in Agg_yr_list:
                    p_k = p_j + k
                    Data = open(p_k, 'r')
                    D = json.load(Data)

                    for p in D['data']['pincodes']:
                        pincodes = p['name']
                        pin_registeredUsers = p['registeredUsers']

                        clm['Pincode'].append(pincodes)
                        clm['Pincode_RegisteredUsers'].append(pin_registeredUsers)
                        clm['State'].append(i)
                        clm['Year'].append(j)
                        clm['Quarter'].append(int(k.strip('.json')))
        # Succesfully created a dataframe
        Top_User_Pincode = pd.DataFrame(clm)
        Top_User_Pincode

        try:
            mysql_db_connector = mysql.connector.connect(host="localhost", user="root", password="mysql@123",
                                                         auth_plugin='mysql_native_password',
                                                         database="data_science", charset='utf8mb4')
            mysql_db_cursor = mysql_db_connector.cursor()
            sql = '''create table IF NOT EXISTS Top_User_Cntry_State_Pincode (
            State varchar(255), 
            Year varchar(255),
            Quarter varchar(255),
            Pincode int,
            Pincode_RegisteredUsers bigint
            )
            CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            '''
            mysql_db_cursor.execute(sql)

            Top_User_Pincode = Top_User_Pincode.fillna(0)
            # Top_User_Pincode
            for i, row in Top_User_Pincode.iterrows():
                sql1 = 'insert into Top_User_Cntry_State_Pincode(State,Year, Quarter,Pincode,Pincode_RegisteredUsers) values (%s, %s, %s, %s, %s)'
                mysql_db_cursor.execute(sql1, tuple(row))
                mysql_db_connector.commit()
            print(mysql_db_cursor.rowcount, "details inserted")
            # disconnecting from server
            mysql_db_connector.close()

        except:
            mysql_db_connector.close()


def Agg_Txn_Cntry_States1():
        try:
            mysql_db_connector = mysql.connector.connect(
                host="localhost", user="root", password="mysql@123", auth_plugin='mysql_native_password',
                database="data_science")
            # print(mysql_db_connector)
            mysql_cursor = mysql_db_connector.cursor()
            # sql="SELECT * FROM Agg_Txns_Country_State"
            sql = "SELECT State,Year,Quarter,Transaction_type,Transaction_count,Transaction_amount FROM Agg_Txns_Country_State"
            mysql_cursor.execute(sql)
            rows = mysql_cursor.fetchall()
            Agg_Txn_Cntry_State = pd.DataFrame(rows,
                                               columns=['State', 'Year', 'Quarter', 'Transaction_type', 'Transaction_count',
                                                        'Transaction_amount'])
            # print(Agg_Txn_Cntry_State.to_string())
            Agg_Txn_Cntry_State1 = Agg_Txn_Cntry_State.groupby(['State', 'Year', 'Quarter']).agg(
                {'Transaction_count': 'sum', 'Transaction_amount': 'sum'}).reset_index()
            Agg_Txn_Cntry_State1['Transaction_amount'] = Agg_Txn_Cntry_State1['Transaction_amount'] / 10000000
            Agg_Txn_Cntry_State1['Avg. transaction value'] = Agg_Txn_Cntry_State1['Transaction_amount'] * 10000000 / \
                                                             Agg_Txn_Cntry_State1['Transaction_count']
            # print(Agg_Txn_Cntry_State1.to_string())
            #Agg_Txn_Cntry_State2 = Agg_Txn_Cntry_State.groupby(['State', 'Year', 'Quarter', 'Transaction_type']).agg(
            #    {'Transaction_count': 'sum'}).reset_index()
            # print(Agg_Txn_Cntry_State2.to_string())
            mysql_db_connector.close()
            return Agg_Txn_Cntry_State1

        except:
            mysql_db_connector.close()

def Agg_Txn_Cntry_States2():
    try:
        mysql_db_connector = mysql.connector.connect(
            host="localhost", user="root", password="mysql@123", auth_plugin='mysql_native_password',
            database="data_science")
        # print(mysql_db_connector)
        mysql_cursor = mysql_db_connector.cursor()
        sql = "SELECT State,Year,Quarter,Transaction_type,Transaction_count,Transaction_amount FROM Agg_Txns_Country_State"
        mysql_cursor.execute(sql)
        rows = mysql_cursor.fetchall()
        Agg_Txn_Cntry_State = pd.DataFrame(rows,
                                           columns=['State', 'Year', 'Quarter', 'Transaction_type', 'Transaction_count',
                                                    'Transaction_amount'])
        # print(Agg_Txn_Cntry_State.to_string())

        Agg_Txn_Cntry_State2 = Agg_Txn_Cntry_State.groupby(['State', 'Year', 'Quarter', 'Transaction_type']).agg({'Transaction_count': 'sum'}).reset_index()
        # print(Agg_Txn_Cntry_State2.to_string())
        mysql_db_connector.close()
        return Agg_Txn_Cntry_State2

    except:
        mysql_db_connector.close()

def User_Txn_Cntry_State1():
    try:
        mysql_db_connector = mysql.connector.connect(
            host="localhost", user="root", password="mysql@123", auth_plugin='mysql_native_password',
            database="data_science")
        # print(mysql_db_connector)
        mysql_cursor = mysql_db_connector.cursor()
        # sql="SELECT * FROM Agg_Txns_Country_State"
        sql = "SELECT distinct State,Year,Quarter,RegisteredUsers,appOpens FROM User_Txns_Country_State"
        mysql_cursor.execute(sql)
        rows = mysql_cursor.fetchall()
        User_Txn_Cntry_State1 = pd.DataFrame(rows, columns=['State', 'Year', 'Quarter', 'RegisteredUsers', 'appOpens'])
        mysql_db_connector.close()

        return User_Txn_Cntry_State1

    except:
        mysql_db_connector.close()

def Dict_Txn_cnts():
    try:
        mysql_db_connector = mysql.connector.connect(
            host="localhost", user="root", password="mysql@123", auth_plugin='mysql_native_password',
            database="data_science")
        # print(mysql_db_connector)
        mysql_cursor = mysql_db_connector.cursor()
        sql = '''select State,Year,Quarter,District,round((Transaction_count/100000),2) as Transaction_count  
                 from Top_Txn_Cntry_State_Dist order by Transaction_count desc'''
        mysql_cursor.execute(sql)
        rows = mysql_cursor.fetchall()
        Dict_Txn_cnt = pd.DataFrame(rows, columns=['State','Year','Quarter','District', 'Transaction_count'])
        mysql_db_connector.close()

        return Dict_Txn_cnt

    except:
        mysql_db_connector.close()

def Pincode_Txn_cnts():
    try:
        mysql_db_connector = mysql.connector.connect(
            host="localhost", user="root", password="mysql@123", auth_plugin='mysql_native_password',
            database="data_science")
        # print(mysql_db_connector)
        mysql_cursor = mysql_db_connector.cursor()
        sql = '''select State,Year,Quarter,Pincode,round((Pincode_Transaction_count/100000),2) as Transaction_count
                 from Top_Txn_Cntry_State_Pincode order by Pincode_Transaction_count desc'''
        mysql_cursor.execute(sql)
        rows = mysql_cursor.fetchall()
        Pincode_Txn_cnt = pd.DataFrame(rows, columns=['State','Year','Quarter','Pincode', 'Transaction_count'])
        mysql_db_connector.close()

        return Pincode_Txn_cnt

    except:
        mysql_db_connector.close()

def Dict_Txn_cnt_User():
    try:
        mysql_db_connector = mysql.connector.connect(
            host="localhost", user="root", password="mysql@123", auth_plugin='mysql_native_password',
            database="data_science")
        # print(mysql_db_connector)
        mysql_cursor = mysql_db_connector.cursor()
        sql = '''select State,Year,Quarter,District,round((Registeredusers/100000),2) as Registeredusers
                from Top_User_Cntry_State_Dist order by Year,Quarter desc'''
        mysql_cursor.execute(sql)
        rows = mysql_cursor.fetchall()
        Dict_Txn_cnt_user = pd.DataFrame(rows, columns=['State','Year','Quarter','District', 'Registeredusers'])
        mysql_db_connector.close()

        return Dict_Txn_cnt_user

    except:
        mysql_db_connector.close()

def Pincode_Txn_cnt_User():
    try:
        mysql_db_connector = mysql.connector.connect(
            host="localhost", user="root", password="mysql@123", auth_plugin='mysql_native_password',
            database="data_science")
        # print(mysql_db_connector)
        mysql_cursor = mysql_db_connector.cursor()
        sql = '''select State,Year,Quarter,Pincode,round((Pincode_Registeredusers/100000),2) as Registeredusers 
                from Top_User_Cntry_State_Pincode order by Year,Quarter desc'''
        mysql_cursor.execute(sql)
        rows = mysql_cursor.fetchall()
        Pincode_Txn_cnt_user = pd.DataFrame(rows, columns=['State','Year','Quarter','Pincode', 'Registeredusers'])
        mysql_db_connector.close()

        return Pincode_Txn_cnt_user

    except:
        mysql_db_connector.close()


def GeoIndiaMap(df):
    fig = px.choropleth_mapbox(
        df,
        geojson='https://raw.githubusercontent.com/balaji2023DS/Phonepe/main/Ind_State.geojson',
        locations='State',
        color='Transaction_amount',
        hover_name='State',
        hover_data=['Transaction_count', 'Transaction_amount'],
        title='PhonePe Amounts Transactions',
        mapbox_style='carto-positron',
        featureidkey='properties.ST_NM',
        center={'lat': 24, 'lon': 77},
        color_continuous_scale=px.colors.diverging.PuOr,
        color_continuous_midpoint=0,
        zoom=3.5
    )

    # Update hovertemplate
    fig.update_traces(hovertemplate='<b>%{hovertext}</b><br>'
                                    'Transaction Count: %{customdata[0]}<br>'
                                    'Transaction Amount: %{customdata[1]}')
    # Set the projection of the map explicitly
    fig.update_geos(projection_type='equirectangular',
                    lataxis_range=[-90, 90],
                    lonaxis_range=[-180, 180],
                    fitbounds="locations"
                    )
    # Display the map in Streamlit
    st.plotly_chart(fig)

def GeoIndiaMap_User(df1):
    fig = px.choropleth_mapbox(
        df1,
        geojson='https://raw.githubusercontent.com/balaji2023DS/Phonepe/main/Ind_State.geojson',
        locations='State',
        color='RegisteredUsers',
        hover_name='State',
        hover_data=['RegisteredUsers', 'appOpens'],
        title='PhonePe User Transactions',
        mapbox_style='carto-positron',
        featureidkey='properties.ST_NM',
        center={'lat': 24, 'lon': 77},
        color_continuous_scale=px.colors.diverging.PuOr,
        color_continuous_midpoint=0,
        zoom=3.5
    )

    # Update hovertemplate
    fig.update_traces(hovertemplate='<b>%{hovertext}</b><br>'
                                    'RegisteredUsers: %{customdata[0]}<br>'
                                    'appOpens: %{customdata[1]}')
    # Set the projection of the map explicitly
    fig.update_geos(projection_type='equirectangular',
                    lataxis_range=[-90, 90],
                    lonaxis_range=[-180, 180],
                    fitbounds="locations"
                    )
    # Display the map in Streamlit
    st.plotly_chart(fig)

def BarTransaction(df2):
    fig = px.bar(df2, x="Transaction_type", y="Transaction_count",color="Transaction_count",
                  color_continuous_scale='Blues',
                  title="Transaction Type Year-Quarter transaction count"
                 )
    fig.update_yaxes(tickformat=',.0f', tickprefix='₹')
    st.plotly_chart(fig)
    #fig.show()

def PieDistrict(df3):
    fig = go.Figure(data=[go.Pie(labels=df3['District'], values=df3['Transaction_count'])])
    pie_colors = pc.qualitative.Plotly
    fig.update_traces(hoverinfo='label+percent', textinfo='value', textfont_size=12,
                      marker=dict(colors=pie_colors, line=dict(color='#FFFFFF', width=2)),
                      texttemplate="%{label}: ₹ %{value:.2f} lakh"
                      )
    fig.update_layout(title='Pie Chart for District Transactions')
    st.plotly_chart(fig)
    #fig.show()

def PiePincode(df4):
    fig = go.Figure(data=[go.Pie(labels=df4['Pincode'], values=df4['Transaction_count'])])
    pie_colors = pc.qualitative.Plotly
    fig.update_traces(hoverinfo='label+percent', textinfo='value', textfont_size=12,
                      marker=dict(colors=pie_colors, line=dict(color='#FFFFFF', width=2)),
                      texttemplate="%{label}: ₹ %{value:.2f} lakh"
                      )
    fig.update_layout(title=f'Pie Chart for Pincode Transactions')
    #fig.show()
    st.plotly_chart(fig)

def BarTransaction_User(df5):
    fig = px.bar(df5, x="appOpens", y="RegisteredUsers",color="RegisteredUsers",
                 color_continuous_scale='Blues',
                 title="Registered PhonePe users - appOpens"
                 )
    #fig.update_yaxes(tickformat=',.0f', tickprefix='₹')
    #fig.show()
    st.plotly_chart(fig)
def PieDistrict_User(df6):
    fig = go.Figure(data=[go.Pie(labels=df6['District'], values=df6['Registeredusers'])])
    pie_colors = pc.qualitative.Plotly
    fig.update_traces(hoverinfo='label+percent', textinfo='value', textfont_size=12,
                      marker=dict(colors=pie_colors, line=dict(color='#FFFFFF', width=2)),
                      texttemplate="%{label}: ₹ %{value:.2f} lakh"
                      )
    fig.update_layout(title='Pie Chart for District Registeredusers')
    #fig.show()
    st.plotly_chart(fig)

def PiePincode_User(df7):
    fig = go.Figure(data=[go.Pie(labels=df7['Pincode'], values=df7['Registeredusers'])])
    pie_colors = pc.qualitative.Plotly
    fig.update_traces(hoverinfo='label+percent', textinfo='value', textfont_size=12,
                      marker=dict(colors=pie_colors, line=dict(color='#FFFFFF', width=2)),
                      texttemplate="%{label}: ₹ %{value:.2f} lakh"
                      )
    fig.update_layout(title=f'Pie Chart for Pincode Registeredusers')
    #fig.show()
    st.plotly_chart(fig)

#########################################################################################################
def main():
    st.write("Phone Transactions Analysis")
    if (st.button(" Clone the data from Github to Local")):
            #Phonepe_Clone()
            st.write("Clone activity completed")

    if (st.button("Transform the data from Local to Mysql")):
            # Agg_Txn_Cntry_State()
            # Agg_User_Cntry_State()
            # Map_Txn_Cntry_State()
            # Map_User_Cntry_State()
            # Top_Txn_Cntry_State_Distrcit()
            # Top_Txn_Cntry_State_Pincode()
            # Top_user_Cntry_State_District()
            # Top_User_Cntry_State_Pincode()
            st.write("Data Transformation Completed")

    # Define the available options for the dropdown
    Type = ['Transactions', 'Users']
    selected_Type = st.selectbox('Type', Type)

    State = ['All India','Arunachal Pradesh', 'Assam', 'Chandigarh', 'Karnataka', 'Manipur', 'Meghalaya',
             'Mizoram', 'Nagaland', 'Punjab', 'Rajasthan', 'Sikkim', 'Tripura', 'Uttarakhand', 'Telangana', 'Bihar',
             'Kerala', 'Madhya Pradesh', 'Andaman & Nicobar', 'Gujarat', 'Lakshadweep', 'Odisha',
             'Dadra and Nagar Haveli and Daman and Diu', 'Ladakh', 'Jammu & Kashmir', 'Chhattisgarh', 'Delhi', 'Goa',
             'Haryana', 'Himachal Pradesh', 'Jharkhand', 'Tamil Nadu', 'Uttar Pradesh', 'West Bengal',
             'Andhra Pradesh', 'Puduchery', 'Maharashtra']

    selected_state = st.selectbox('States', State, index=0)

    Year = ['None','2018', '2019', '2020', '2021', '2022']
    selected_year = st.selectbox('Year', Year, index=0)

    Quarter = ['None','Q1', 'Q2', 'Q3', 'Q4']
    selected_quarter = st.selectbox('Quarter', Quarter, index=0)
    st.write(selected_Type)

    if(st.button("PhonePe Transaction State wise")):
        if(selected_Type== 'Transactions'):
                Agg_Txn_Cntry_State1=Agg_Txn_Cntry_States1()
                st.write(selected_state)
                st.write(selected_year)
                st.write(selected_quarter)
                # Filter the DataFrame based on the selected option
                if(selected_state ==  'All India' and selected_year == 'None' and selected_quarter ==  'None'):
                    st.write("All Data-Geo India-State Map")
                    st.write(Agg_Txn_Cntry_State1)
                    GeoIndiaMap(Agg_Txn_Cntry_State1)

                elif(selected_state ==  'All India' and selected_year != 'None' and selected_quarter !=  'None'):

                     # Create container for the charts
                     row1_container = st.container()
                     row2_container = st.container()

                     st.write("Year-Quarter wise")
                     no_quarter=str(selected_quarter.strip('Q'))
                     st.write(no_quarter)

                     filter_Agg_Txn_Cntry_State= Agg_Txn_Cntry_State1[(Agg_Txn_Cntry_State1['Year'] == selected_year) &
                                                                (Agg_Txn_Cntry_State1['Quarter'] == no_quarter)]
                     #st.write(filter_Agg_Txn_Cntry_State)


                     #Transactiontype-Transaction amount - Year - Quarter
                     Agg_Txn_Cntry_State2 = Agg_Txn_Cntry_States2()
                     filter_Agg_Txn_Cntry_State1 = Agg_Txn_Cntry_State2[(Agg_Txn_Cntry_State2['Year'] == selected_year) &
                                                                       (Agg_Txn_Cntry_State2['Quarter'] == no_quarter)]
                     filter_Agg_Txn_Cntry_State1 = filter_Agg_Txn_Cntry_State1.groupby(["Year", "Quarter", "Transaction_type"]).agg({"Transaction_count": "sum"}).reset_index()
                     #st.write(filter_Agg_Txn_Cntry_State1)

                     with row1_container:
                        GeoIndiaMap(filter_Agg_Txn_Cntry_State)
                     with row2_container:
                        BarTransaction(filter_Agg_Txn_Cntry_State1)

                elif (selected_state == 'All India' and selected_year == 'None' and selected_quarter != 'None'):
                    st.write("All Data-Geo India-State Map with Quarter")
                    no_quarter = str(selected_quarter.strip('Q'))
                    st.write(no_quarter)
                    filter_Agg_Txn_Cntry_State = Agg_Txn_Cntry_State1[(Agg_Txn_Cntry_State1['Quarter'] == no_quarter)]

                    st.write(filter_Agg_Txn_Cntry_State)
                    GeoIndiaMap(filter_Agg_Txn_Cntry_State)

                elif (selected_state == 'All India' and selected_year != 'None' and selected_quarter == 'None'):
                    st.write("All Data-Geo India-State Map with Year")
                    # no_quarter = str(selected_quarter.strip('Q'))
                    # st.write(no_quarter)
                    filter_Agg_Txn_Cntry_State = Agg_Txn_Cntry_State1[(Agg_Txn_Cntry_State1['Year'] == selected_year)]

                    st.write(filter_Agg_Txn_Cntry_State)
                    GeoIndiaMap(filter_Agg_Txn_Cntry_State)


                elif(selected_state != 'All India' and selected_year != 'None' and selected_quarter == 'None'):
                    st.write("All Data-Geo India-State Map with State-Year")
                    #no_quarter = str(selected_quarter.strip('Q'))
                    #st.write(no_quarter)
                    filter_Agg_Txn_Cntry_State = Agg_Txn_Cntry_State1[(Agg_Txn_Cntry_State1['Year'] == selected_year) &
                                                                      (Agg_Txn_Cntry_State1['State'] == selected_state)]

                    st.write(filter_Agg_Txn_Cntry_State)
                    GeoIndiaMap(filter_Agg_Txn_Cntry_State)


                elif(selected_state != 'All India' and selected_year == 'None' and selected_quarter != 'None'):
                    st.write("All Data-Geo India-State Map with State-Quarter")
                    no_quarter = str(selected_quarter.strip('Q'))
                    st.write(no_quarter)
                    filter_Agg_Txn_Cntry_State = Agg_Txn_Cntry_State1[(Agg_Txn_Cntry_State1['Quarter'] == no_quarter) &
                                                                      (Agg_Txn_Cntry_State1['State'] == selected_state)]

                    st.write(filter_Agg_Txn_Cntry_State)
                    GeoIndiaMap(filter_Agg_Txn_Cntry_State)

                elif (selected_state != 'All India' and selected_year == 'None' and selected_quarter == 'None'):
                    st.write("All Data-Geo India-State Map with State")
                    # no_quarter = str(selected_quarter.strip('Q'))
                    # st.write(no_quarter)
                    filter_Agg_Txn_Cntry_State = Agg_Txn_Cntry_State1[(Agg_Txn_Cntry_State1['State'] == selected_state)]
                    st.write(filter_Agg_Txn_Cntry_State)
                    GeoIndiaMap(filter_Agg_Txn_Cntry_State)
                else:
                    st.write("All Data-Geo India-State Map with State-Year-Quarter")

                    # Create container for the charts
                    row1_container = st.container()
                    row2_container = st.container()

                    # Create columns for the charts
                    col1, col2 = st.columns(2)
                    col3, col4 = st.columns(2)

                    no_quarter = str(selected_quarter.strip('Q'))
                    st.write(no_quarter)
                    filter_Agg_Txn_Cntry_State = Agg_Txn_Cntry_State1[(Agg_Txn_Cntry_State1['State'] == selected_state) &
                                                                      (Agg_Txn_Cntry_State1['Year'] == selected_year) &
                                                                      (Agg_Txn_Cntry_State1['Quarter'] == no_quarter)]
                    #st.write(filter_Agg_Txn_Cntry_State)

                    Agg_Txn_Cntry_State2 = Agg_Txn_Cntry_States2()
                    filter_Agg_Txn_Cntry_State1 = Agg_Txn_Cntry_State2[(Agg_Txn_Cntry_State2['State'] == selected_state) &
                                                                       (Agg_Txn_Cntry_State2['Year'] == selected_year) &
                                                                       (Agg_Txn_Cntry_State2['Quarter'] == no_quarter)]

                    filter_Agg_Txn_Cntry_State1 = filter_Agg_Txn_Cntry_State1.groupby(
                        ["State","Year", "Quarter", "Transaction_type"]).agg({"Transaction_count": "sum"}).reset_index()

                    Dict_Txn_cnt=Dict_Txn_cnts()

                    filter_Dict_Txn_cnt=Dict_Txn_cnt[(Dict_Txn_cnt['State'] == selected_state) &
                                                       (Dict_Txn_cnt['Year'] == selected_year) &
                                                       (Dict_Txn_cnt['Quarter'] == no_quarter)]
                    Pincode_Txn_cnt=Pincode_Txn_cnts()

                    filter_Pincode_Txn_cnt = Pincode_Txn_cnt[(Pincode_Txn_cnt['State'] == selected_state) &
                                                       (Pincode_Txn_cnt['Year'] == selected_year) &
                                                       (Pincode_Txn_cnt['Quarter'] == no_quarter)]


                    with row1_container:
                        with col1:
                            GeoIndiaMap(filter_Agg_Txn_Cntry_State)
                        with col2:
                            BarTransaction(filter_Agg_Txn_Cntry_State1)
                    with row2_container:
                        with col3:
                            PieDistrict(filter_Dict_Txn_cnt)
                        with col4:
                            PiePincode(filter_Pincode_Txn_cnt)

        else:
                st.write(selected_state)
                st.write(selected_year)
                st.write(selected_quarter)

                # Filter the DataFrame based on the selected option
                if (selected_state == 'All India' and selected_year == 'None' and selected_quarter == 'None'):
                    User_Txn_Cntry_State = User_Txn_Cntry_State1()
                    st.write("All Data-Geo India-State Map")
                    st.write(User_Txn_Cntry_State)
                    GeoIndiaMap_User(User_Txn_Cntry_State)

                elif (selected_state == 'All India' and selected_year != 'None' and selected_quarter != 'None'):
                    st.write("All Data-Geo India-State Map with Year and Quarter")
                    # Create container for the charts
                    row1_container = st.container()
                    row2_container = st.container()

                    no_quarter = str(selected_quarter.strip('Q'))
                    st.write(no_quarter)
                    User_Txn_Cntry_State = User_Txn_Cntry_State1()
                    filter_User_Txn_Cntry_State = User_Txn_Cntry_State[(User_Txn_Cntry_State['Year'] == selected_year) &
                                                                      (User_Txn_Cntry_State['Quarter'] == no_quarter)]

                    filter_User_Txn_Cntry_State_sum= filter_User_Txn_Cntry_State.groupby(['Year','Quarter']).agg({'RegisteredUsers':'sum','appOpens':'sum'}).reset_index()

                    # st.write(filter_User_Txn_Cntry_State)
                    # st.write(filter_User_Txn_Cntry_State_sum)

                    with row1_container:
                        GeoIndiaMap_User(filter_User_Txn_Cntry_State)
                    with row2_container:
                        BarTransaction_User(filter_User_Txn_Cntry_State_sum)

                elif(selected_state != 'All India' and selected_year != 'None' and selected_quarter != 'None'):
                    st.write("All Data-Geo India-State Map with Year and Quarter")

                    # Create container for the charts
                    row1_container = st.container()
                    row2_container = st.container()

                    # Create columns for the charts
                    col1, col2 = st.columns(2)

                    no_quarter = str(selected_quarter.strip('Q'))
                    st.write(no_quarter)
                    User_Txn_Cntry_State = User_Txn_Cntry_State1()
                    filter_User_Txn_Cntry_State = User_Txn_Cntry_State[(User_Txn_Cntry_State['State'] == selected_state) &
                                                                       (User_Txn_Cntry_State['Year'] == selected_year) &
                                                                       (User_Txn_Cntry_State['Quarter'] == no_quarter)]
                                                                       
    
                    #filter_User_Txn_Cntry_State_sum = filter_User_Txn_Cntry_State.groupby(['State','Year', 'Quarter']).agg({'RegisteredUsers': 'sum', 'appOpens': 'sum'}).reset_index()

                    Dict_Txn_cnt_User1=Dict_Txn_cnt_User()
                    filter_Dict_Txn_cnt_User = Dict_Txn_cnt_User1[(Dict_Txn_cnt_User1['State'] == selected_state) &
                                                                 (Dict_Txn_cnt_User1['Year'] == selected_year) &
                                                                 (Dict_Txn_cnt_User1['Quarter'] == no_quarter)]
                    Pincode_Txn_cnt_User1=Pincode_Txn_cnt_User()
                    filter_Pincode_Txn_cnt_User = Pincode_Txn_cnt_User1[(Pincode_Txn_cnt_User1['State'] == selected_state) &
                                                                    (Pincode_Txn_cnt_User1['Year'] == selected_year) &
                                                                    (Pincode_Txn_cnt_User1['Quarter'] == no_quarter)]


                    # st.write(filter_User_Txn_Cntry_State)
                    # st.write(filter_User_Txn_Cntry_State_sum)
                    # st.write(filter_Dict_Txn_cnt_User)
                    # st.write(filter_Pincode_Txn_cnt_User)

                    with row1_container:
                        GeoIndiaMap_User(filter_User_Txn_Cntry_State)

                    with row2_container:
                        with col1:
                            PieDistrict_User(filter_Dict_Txn_cnt_User)
                        with col2:
                            PiePincode_User(filter_Pincode_Txn_cnt_User)

                    #BarTransaction_User(filter_User_Txn_Cntry_State_sum)


if __name__ == '__main__':
    main()