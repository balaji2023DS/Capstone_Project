import streamlit as st
from googleapiclient.discovery import build
import pandas as pd
import numpy as np
import datetime as dt
api_key = 'AIzaSyBjUGCL3oLPN4xNN0xg9XINOuU2sOi6oHQ'  # Api key for youtube data api
#channel_ids = 'UCUNIcmZUrqrg5jO2I9D_0YA'
# channel_ids=['UCUNIcmZUrqrg5jO2I9D_0YA','UCBt6VrxAIb5jLh9HLDcdwtQ','UCwk47V1XafOrw83d_vwwGhg']
api_service_name = 'youtube'  # service name
api_version = 'v3'  # version name
youtube = build(api_service_name, api_version, developerKey=api_key)

import pymongo
import mysql.connector
client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
#print(client)
db= client['DB_Youtube']
#db.createCollection["data_Youtube_Channel"]
@st.cache_data
#function get channel details
def get_channel_stats(_youtube,channel_ids):
    all_data= {}
    request = youtube.channels().list(
                part="snippet,contentDetails,statistics", id=channel_ids)
                #id=','.join(channel_ids))
    response = request.execute()
    for i in range(len(response['items'])):
        channel_data= dict(Channel_Name= response['items'][i]['snippet']['title'],
                      Channel_Id= response['items'][i]['id'],
                      Subscription_Count= response['items'][i]['statistics']['subscriberCount'],
                      Channel_Views= response['items'][i]['statistics']['viewCount'],
                      Channel_Description= response['items'][i]['snippet']['description'],
                      Playlist_Id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads']
                      #Video_id=get_video_details(youtube,get_videos_stats(youtube,response['items'][i]['contentDetails']['relatedPlaylists']['uploads']))
                      )
        all_data.update(channel_data)
    return all_data


# function get all playlist id and playlsit name from channel
def get_playlist_info(youtube, channel_ids):
    all_playlists = []
    request = youtube.playlists().list(
        part="snippet,contentDetails",
        channelId=channel_ids,
        maxResults=25)
    response = request.execute()
    # return response
    for i in range(len(response['items'])):
        playlist_info = dict(Channel_Id=response['items'][i]['snippet']['channelId'],
                             Playlist_Id=response['items'][i]['id'],
                             Playlist_Name=response['items'][i]['snippet']['title']

                             )
        all_playlists.append(playlist_info)
    return all_playlists

#function get playlist_video details
def get_playlist_videos(_youtube, play_listid):
    all_play_video = []
    request = youtube.playlistItems().list(
        part="contentDetails", playlistId=play_listid,
        maxResults=50)
    response = request.execute()

    for i in range(len(response['items'])):
        playlist_video = dict(
            Playlist_Id=play_listid,
            Video_Id=response['items'][i]['contentDetails']['videoId']
        )
        all_play_video.append(playlist_video)
    next_Page_Token = response.get('nextPageToken')

    more_pages = True
    while more_pages:
        if next_Page_Token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                part="contentDetails", playlistId=play_listid,
                maxResults=50, pageToken=next_Page_Token)
            response = request.execute()

            for i in range(len(response['items'])):
                playlist_video = dict(Playlist_Id=play_listid,
                                      Video_Id=response['items'][i]['contentDetails']['videoId'])
                all_play_video.append(playlist_video)
            next_Page_Token = response.get('nextPageToken')

    return all_play_video

#function get video_stats details
def get_videos_stats(youtube, play_listid):
    videos_ids = []
    request = youtube.playlistItems().list(
        part="contentDetails", playlistId=play_listid,
        maxResults=50)
    response = request.execute()
    for i in range(len(response['items'])):
        videos_ids.append(response['items'][i]['contentDetails']['videoId'])
    next_Page_Token = response.get('nextPageToken')
    more_pages = True
    while more_pages:
        if next_Page_Token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                part="contentDetails", playlistId=play_listid,
                maxResults=50, pageToken=next_Page_Token)
            response = request.execute()
            for i in range(len(response['items'])):
                videos_ids.append(response['items'][i]['contentDetails']['videoId'])
            next_Page_Token = response.get('nextPageToken')

    return (videos_ids)


# function get video details
def get_video_details(youtube, video_ids):
    all_video_stats = {}

    for i in range(0, len(video_ids)):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_ids[i])
        response = request.execute()
        # return response

        for video in response['items']:
            video_stats = {
                "Video_Id_" + str(i + 1):
                    dict
                        (
                        Video_Id=video['id'],
                        Video_Name=video['snippet']['title'],
                        Video_Description=video['snippet']['description'],
                        # Tags= video['snippet']['tags'],
                        PublishedAt=video['snippet']['publishedAt'],
                        View_Count=video['statistics']['viewCount'],
                        Like_Count=video['statistics']['likeCount'],
                        # Dislike_Count= video['statistics']['dislikeCount'],
                        Favorite_Count=video['statistics']['favoriteCount'],
                        # Comment_Count= video['statistics']['commentCount'],
                        Duration=video['contentDetails']['duration'],
                        Thumbnail=video['snippet']['thumbnails']['default']['url'],
                        Caption_Status=video['contentDetails']['caption'],
                        Comments=get_comment_videoinfo(youtube, video_ids[i])
                    )
            }

            all_video_stats.update(video_stats)
            # print(all_video_stats)
            # break

    return all_video_stats


#getting channel id comments
def get_comment_videoinfo(youtube,video_id):
    all_comments={}
    try:
        request = youtube.commentThreads().list(
                    part="snippet,replies",
                    videoId=video_id,
                    maxResults=5
                    #videoId=','.join(video_id)
                    )
        video_response = request.execute()
        #print(len(video_response))
        #print(video_response)
        #for i in range(0,len(video_response)):
        for comment in video_response['items']:
            comment_stats= {
                            #"Video_Id" : comment['snippet']['topLevelComment']['snippet']['videoId'],
                            "Comment_Id" : comment['snippet']['topLevelComment']['id'],
                            "Comment_Text" : comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                            "Comment_Author" : comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            "Comment_PublishedAt" : comment['snippet']['topLevelComment']['snippet']['publishedAt']
                             }
            all_comments.update({"Comment_Id_"+ str(len(all_comments)+1):comment_stats})
        return all_comments
    except:
        comment_stats= dict(
                                #Video_Id=video_id,
                                Comment_Id=None,
                                Comment_Text= None,
                                Comment_Author= None,
                                Comment_PublishedAt = None
                                )

    #return all_comments

#########################################################################################################
channel_ids = st.text_input('enter the channel id:').split(',')
#st.write("Channel ids:")
st.write(channel_ids)
#st.write("multiselect list:")
selected_channel_ids=st.multiselect("Select channel id", channel_ids)
#st.write(selected_channel_ids)

if(st.button('get channel details')):
    for channel in selected_channel_ids:
            #st.write(channel)
            #dict_channel = {}
            #dict_channel.update(get_channel_stats(youtube, channel))
            #pd_channel = pd.DataFrame([get_channel_stats(youtube, channel)])
            #st.write(pd_channel)
            #st.write(pd_channel)

            total_playlist = get_playlist_info(youtube, channel)
            # print(total_playlist)
            # total_playlist = {k:[v] for k,v in total_playlist.items()}  # WORKAROUND
            pd_playlist = pd.DataFrame(total_playlist)
            # pd_playlist=pd.DataFrame(total_playlist)
            #pd_playlist

            video_ids = []
            # for i in range(len(pd_channel)):
            for i in range(len(pd_playlist)):
                #     #play_listid= pd_channel.iloc[i,5]
                #     play_listid=pd_playlist.iloc[i,1]
                #     #print(play_listid)
                video_ids.extend(get_videos_stats(youtube, pd_playlist.iloc[i, 1]))
            #    video_ids.extend(get_videos_stats(youtube,pd_channel.iloc[i,5]))
            # print(video_ids)
            # all_video_stats= get_video_details(youtube,video_ids)
            video_ids = list(set(video_ids))
            channel_complete = {}
            channel_complete.update(Channel_Name=get_channel_stats(youtube, channel))
            channel_complete.update(get_video_details(youtube, video_ids))
            #st.write(channel_complete)
            st.write("data inserted successfully:",db.data_Youtube_Channel.insert_one(channel_complete))
            channel_complete.clear()

if(st.button("Data migrate from mongodb to mysql")):
    # channel dataframe
    pd.set_option('display.max_columns', 20)
    pd.set_option('display.width', 2000)
    list_channel_cur = []
    channel_coll = db.data_Youtube_Channel.find({})
    for document in channel_coll:
        channel = {"Channel_Name": document['Channel_Name']['Channel_Name'],
                   "Channel_Id": document['Channel_Name']['Channel_Id'],
                   "Subscription_Count": document['Channel_Name']['Subscription_Count'],
                   "Channel_Views": document['Channel_Name']['Channel_Views'],
                   "Channel_Description": document['Channel_Name']['Channel_Description']
                   # "Playlist_Id" : document['Channel_Name']['Playlist_Id']
                   }
        list_channel_cur.append(channel)
    # print(f"Channel Name: {channel_name}")
    # print(f"Channel Id: {channel_id}")
    # print(f"Subscription Count: {subscription_count}")
    # print(f"Channel Views: {channel_views}")
    # print(f"Channel Description: {channel_description}")
    # print(f"Playlist Id: {playlist_id}")
    # print(list_channel_cur)
    # # # Converting to the DataFrame
    df_channel = pd.DataFrame(list_channel_cur)
    # print(df_channel)
    # df_channel.drop(df_channel.columns[5], axis=1, inplace=True)
    # print(df_channel)

    #insert channel data into mysql table
    mysql_db_connector= mysql.connector.connect(
    host="localhost",user="root",password="mysql@123",auth_plugin='mysql_native_password',database="data_science",charset='utf8mb4'
    )
    #print(mysql_db_connector)

    mysql_db_cursor = mysql_db_connector.cursor()
    sql='''create table IF NOT EXISTS Channel (
    Channel_id varchar(255) NOT NULL, 
    channel_name varchar(255),
    Subscription_Count int,
    channel_views int, 
    channel_description text, 
    PRIMARY KEY (Channel_id)
    )
    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
    '''
    mysql_db_cursor.execute(sql)

    df_channel=df_channel.fillna(0)
    for i,row in df_channel.iterrows():
        sql1 = 'insert into Channel(Channel_Name,Channel_Id, Subscription_Count,Channel_Views, Channel_Description) values (%s, %s, %s, %s, %s)'
        mysql_db_cursor.execute(sql1,tuple(row))
        mysql_db_connector.commit()
    print(mysql_db_cursor.rowcount, "details inserted")
    # disconnecting from server
    mysql_db_connector.close()

    #video dataframe
    pd.set_option('display.max_columns', 20)
    pd.set_option('display.width', 2000)
    Video_coll = {}
    list_video_cur = []
    for obj in db.data_Youtube_Channel.find({}, {'_id': 0, 'Channel_Name': 0}):
        Video_coll.update(obj)

    # print(Video_coll)
        for document in Video_coll.values():
            Video = {
                "Video_Id": document['Video_Id'],
                "Video_Name": document['Video_Name'],
                "Video_Description": document['Video_Description'],
                "PublishedAt": document['PublishedAt'],
                "View_Count": document['View_Count'],
                "Like_Count": document['Like_Count'],
                "Favorite_Count": document['Favorite_Count'],
                "Duration": document['Duration'],
                "Thumbnail": document['Thumbnail'],
                "Caption_Status": document['Caption_Status']
            }
            list_video_cur.append(Video)

    # Converting to the DataFrame
    df_video = pd.DataFrame(list_video_cur)
    # print(df_video)
    # adding new columns in video dataframe
    Playlist_id = np.array([])
    dislike_count = np.array([])
    comment_count = np.array([])
    df_video['dislike_count'] = pd.Series(dislike_count)
    df_video['comment_count'] = pd.Series(comment_count)
    df_video['PublishedAt'] = pd.to_datetime(df_video['PublishedAt']).dt.date
    df_video['Playlist_id'] = pd.Series(Playlist_id)
    # print(df_video.dtypes)
    # print(df_video.count())

    # insert video data into mysql table
    mysql_db_connector = mysql.connector.connect(
        host="localhost", user="root", password="mysql@123", auth_plugin='mysql_native_password',
        database="data_science", charset='utf8mb4')
    # print(mysql_db_connector)
    try:
        mysql_db_cursor = mysql_db_connector.cursor()
        sql = '''create table IF NOT EXISTS Video (
        Video_id varchar(255) NOT NULL,
        Playlist_id varchar(255), 
        Video_name text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
        Video_description text,
        published_date datetime,
        view_count int,
        like_count int,
        dislike_count int,
        favorite_count int,
        comment_count int,
        duration varchar(255), 
        thumbnail varchar(255), 
        caption_status varchar(255), 
        PRIMARY KEY (Video_id)
        -- ,FOREIGN KEY (Playlist_id) REFERENCES Playlist(Playlist_id)
        )
        CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        '''
        mysql_db_cursor.execute(sql)

        df_video = df_video.fillna(0)
        for i, row in df_video.iterrows():
            sql1 = '''insert into Video(Video_id, Video_name,Video_description,published_date,view_count,like_count,
                      favorite_count,duration,thumbnail,caption_status,dislike_count,comment_count,Playlist_id)
                      values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
            mysql_db_cursor.execute(sql1, tuple(row))
            mysql_db_connector.commit()
        print(mysql_db_cursor.rowcount, "details inserted")
        # disconnecting from server
        mysql_db_connector.close()

    except:
        mysql_db_connector.close()

    # videocomment dataframe
    pd.set_option('display.max_columns', 20)
    pd.set_option('display.width', 2000)
    list_comm_cur = []
    # print(Video_coll)
    for video_id in Video_coll:
        comments_none = Video_coll[video_id]['Comments']
        #    print(comments)
        if comments_none is not None:
            comments = {key: value for key, value in comments_none.items()
                        if value is not None
                        }
            # print(comments)
            for comment_id in comments:
                #        print(comment_id)
                comment = comments[comment_id]
                #         print(comment)
                comm = {
                    "Video_Id": Video_coll[video_id]['Video_Id'],
                    "Comment_Id": comment['Comment_Id'],
                    "Comment_Text": comment['Comment_Text'],
                    "Comment_Author": comment['Comment_Author'],
                    "Comment_PublishedAt": comment['Comment_PublishedAt']
                }
            #             break
            #             print(comm)

                list_comm_cur.append(comm)
    #         print(list_comm_cur)
    #         break

    # # Converting to the DataFrame
    df_videocomm = pd.DataFrame(list_comm_cur)
    df_videocomm['Comment_PublishedAt'] = pd.to_datetime(df_videocomm['Comment_PublishedAt']).dt.date
    df_videocomm = df_videocomm.drop_duplicates()
    # print(df_videocomm.count())

    # insert videocomment data into mysql table
    mysql_db_connector = mysql.connector.connect(
        host="localhost", user="root", password="mysql@123", auth_plugin='mysql_native_password',
        database="data_science", charset='utf8mb4'
    )
    # print(mysql_db_connector)

    mysql_db_cursor = mysql_db_connector.cursor()
    sql = 'create table IF NOT EXISTS Comment (Comment_id varchar(255) NOT NULL, Video_id varchar(255), Comment_Text text, Comment_author varchar(255), Comment_Published_date datetime, PRIMARY KEY (Comment_id), FOREIGN KEY (Video_id) REFERENCES Video(Video_id))'
    mysql_db_cursor.execute(sql)
    # df_videocomm_sample=df_videocomm.sample(25)
    df_videocomm = df_videocomm.fillna(0)
    for i, row in df_videocomm.iterrows():
        sql1 = 'insert into Comment( Video_id, Comment_id, Comment_Text,Comment_author, Comment_Published_date) values (%s, %s, %s, %s, %s)'
        mysql_db_cursor.execute(sql1, tuple(row))
        mysql_db_connector.commit()
    print(mysql_db_cursor.rowcount, "details inserted")
    # disconnecting from server
    mysql_db_connector.close()
