import pandas as pd
from googleapiclient.discovery import build
import streamlit as st
from datetime import timedelta
import re
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import mysql.connector as sql
from datetime import datetime


st.title("YouTube Data Harvesting and Warehousing")
st.subheader('Database: MongoDB and MySQL')
st.write('Developed By: Vivek S')
st.sidebar.header("Import Channel Details")

API_KEY = st.sidebar.text_input("Enter API Key")
submit_channel_id = st.sidebar.text_input("Enter Channel ID")
channel_ID_submit =st.sidebar.button("Submit")


#get channel details
def fetch_channel_details(channel_id):
  youtube = build('youtube', 'v3', developerKey=API_KEY)
  all_data = []
  request = youtube.channels().list(
            part="snippet,contentDetails,statistics,status",
            id=channel_id)
  response = request.execute()

  for item in response["items"]:
    data={'channel_name'    : item["snippet"]["title"],
          'channel_id'      : item['id'],
          'channel_playlist_id'     : item['contentDetails']['relatedPlaylists']['uploads'],
          'country'         : item["snippet"].get('country'),
          'channel_views'   : int(item['statistics']['viewCount']),
          'subscription'    : int(item['statistics']['subscriberCount']),
          'channel_uploads' : int(item['statistics']['videoCount']),
          'channel_age'     : item["snippet"]['publishedAt'],
          'channel_status'  : item['status']['privacyStatus']
          }
    all_data.append(data)
  return(pd.DataFrame(all_data))
  
#get playlist details
def fetch_playlist(channel_id):
  youtube = build('youtube', 'v3', developerKey=API_KEY)
  playlist=[]
  next_page_token = None
  while True:

    request = youtube.playlists().list(
          part="snippet,contentDetails",
          channelId = channel_id,
          pageToken=next_page_token
          )
    response = request.execute()

    for plist in response['items']:
      play = {'channel_id': plist['snippet']['channelId'],
              'playlist_id': plist['id'],
              'playlist_name': plist['snippet']['title']
              }
      playlist.append(play)

    next_page_token = response.get('nextPageToken')
    if response.get('nextPageToken') is None:
      break

  return(pd.DataFrame(playlist))
  
# to get the videos details of the channel
def fetch_videos(channel_id):
  youtube = build('youtube', 'v3', developerKey=API_KEY)
  vid_id = []

  pl_request = youtube.channels().list(
            part="contentDetails",
            id=channel_id)
  pl_response = pl_request.execute()
  playlistid = pl_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

  next_page_token = None
  while True:

    vi_request = youtube.playlistItems().list(
          part="snippet,contentDetails",
          maxResults=100,
          playlistId=playlistid,
          pageToken=next_page_token
      )
    vi_response = vi_request.execute()

    for item in vi_response['items']:
      video_id = item['contentDetails']['videoId']
      vid_id.append(video_id)

    next_page_token = vi_response.get('nextPageToken')
    if vi_response.get('nextPageToken') is None:
      break

  video_statistics = []
  for i in range(0,len(vid_id)):
    sat_request = youtube.videos().list(
                                        part  ="snippet,contentDetails,statistics",
                                        id    = vid_id[i]
                                        )
    sat_response = sat_request.execute()

    hours_pattern = re.compile(r'(\d+)H')
    minutes_pattern = re.compile(r'(\d+)M')
    seconds_pattern = re.compile(r'(\d+)S')
                         

    for vidstat in sat_response['items']:
      video_duration = vidstat['contentDetails']['duration']
      
      hours = hours_pattern.search(video_duration)
      minutes = minutes_pattern.search(video_duration)
      seconds = seconds_pattern.search(video_duration)
      
            
      hours = int(hours.group(1)) if hours else 0
      minutes = int(minutes.group(1)) if minutes else 0
      seconds = int(seconds.group(1)) if seconds else 0
      video_duration = timedelta(hours=hours, minutes=minutes, seconds=seconds).total_seconds()

      video_details = {
                      'channel_name'  : vidstat["snippet"]['channelTitle'],
                      'channel_id'    : vidstat["snippet"]['channelId'],
                      'video_id'      : vidstat['id'],
                      'video_title'   : vidstat['snippet']['title'],
                      'duration'      : video_duration,
                      'release_date'  : vidstat["snippet"]['publishedAt'],
                      'tags'          : vidstat["snippet"].get('tags'),
                      'thumbnail'     : vidstat["snippet"]['thumbnails']['default']['url'],
                      'video_quality' : vidstat['contentDetails']['definition'],
                      'views'         : int(vidstat['statistics']['viewCount']),
                      'likes'         : vidstat['statistics'].get('likeCount'),
                      'favorite'      : int(vidstat['statistics']['favoriteCount']),
                      'comment_count' : int(vidstat['statistics'].get('commentCount',0)),
                      'description'   : vidstat['snippet']['description'],
                      'caption_status': vidstat['contentDetails']['caption']
                      }
      video_statistics.append(video_details)
  return(pd.DataFrame(video_statistics))
 
# to get the comments details of the channel
def fetch_video_comments(channel_id):
  youtube = build('youtube', 'v3', developerKey=API_KEY)
  #to get the playlist
  pl_request    = youtube.channels().list(part="contentDetails",
                                          id=channel_id)
  pl_response   = pl_request.execute()
  playlistid    = pl_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

  #to get the video ID
  vide_id       = []
  vi_request    = youtube.playlistItems().list(
                  part="snippet,contentDetails",
                  maxResults=100,
                  playlistId=playlistid)

  vi_response   = vi_request.execute()

  for item in vi_response['items']:
    video_id    = item['contentDetails']['videoId']
    vide_id.append(video_id)
  #return(vid_id)
  # to get comments
  comments = []
  for i in range(0,len(vide_id)):
    c_request = youtube.commentThreads().list(
              part="snippet,replies",
              textFormat="plainText",
              maxResults=100,
              videoId=vide_id[i]
              )
    c_response = c_request.execute()

    #comments.append(c_response)
  #return(comments)
    for j in range(len(c_response['items'])):
      comment_details = {'video_id'     :c_response['items'][j]['snippet']['videoId'],
                         'comment_id'   :c_response['items'][j].get('id',0),
                         'author_name'       :c_response['items'][j]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                         'comments'      :c_response['items'][j]['snippet']['topLevelComment']['snippet']['textDisplay'],
                         'commented_date' :c_response['items'][j]['snippet']['topLevelComment']['snippet']['publishedAt']
                        }
      comments.append(comment_details)
  return(pd.DataFrame(comments))
  

# Fetch data from API and store it in MongoDB

uri = "mongodb+srv://vivek:YoutubeProject@cluster-youtube.odjjzux.mongodb.net/?retryWrites=true&w=majority"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Successfully connected to MongoDB!")
except Exception as e:
    print(e)

db = client["Youtube"]

def channel_details(Ch_ID):
    df1 = fetch_channel_details(Ch_ID)
    df2 = fetch_playlist(Ch_ID)
    df3 = fetch_videos(Ch_ID)
    df4 = fetch_video_comments(Ch_ID)

    # Concatenate along rows (axis=0)
    combined_df = pd.concat([df1, df2, df3, df4], axis=0, ignore_index=True) 
   # Data Cleaning
    combined_df['channel_name'].fillna(combined_df['channel_name'][0], inplace = True)
    combined_df['commented_date'] = pd.to_datetime(combined_df['commented_date']).dt.strftime('%Y-%m-%d')
    combined_df['channel_age'] = pd.to_datetime(combined_df['channel_age']).dt.year
    combined_df['channel_age'] = combined_df['channel_age'].fillna(combined_df['channel_age'][0]).astype(int)
    combined_df['release_date'] = pd.to_datetime(combined_df['release_date']).dt.year
    combined_df.fillna(0, inplace = True)
    combined_df['channel_views'] = combined_df['channel_views'].astype(int)
    combined_df['subscription']=combined_df['subscription'].astype(int)
    combined_df['channel_uploads']=combined_df['channel_uploads'].astype(int)
    combined_df['release_date'] = combined_df['release_date'].astype(int)
    combined_df['duration'] = combined_df['duration'].astype(int)
    combined_df['views'] = combined_df['views'].astype(int)
    combined_df['likes'] = combined_df['likes'].astype(int)
    combined_df['favorite'] = combined_df['favorite'].astype(int)
    combined_df['comment_count'] = combined_df['comment_count'].astype(int)
    
    
    # Create a dictionary to represent the main document
    main_document = {                    
                    'channel_details': df1.to_dict(orient='records'),
                    'playlist_details': df2.to_dict(orient='records'),
                    'video_details': df3.to_dict(orient='records'),
                    'comment_details': df4.to_dict(orient='records')
                    }

    # Access the desired collection within the database
    collection_name = combined_df['channel_name'][0]  # Adjust the collection name as needed
    collection = db[collection_name]

    # Insert the main document into the collection
    collection.insert_one(main_document)
    st.success(collection_name + " Channel details inserted to MongoDB Successfully")
    
    st.snow()
    return 

if channel_ID_submit:
    channel_details(submit_channel_id)

# function to migrating data from mongodb to mysql

def migrate_data_to_mysql(mongodb_collection_name):
    

    # establishing to MySQL
    conn = sql.connect(user='root',
                    password='A@sD#F45',
                    host='localhost',
                    database = 'youtube')
    if conn:
        print('Connected to MySQL successfully')
    else:
        print('Connection Not established')

    #cursor = conn.cursor()

    # establishing connection to mongodb
    uri = "mongodb+srv://vivek:YoutubeProject@cluster-youtube.odjjzux.mongodb.net/?retryWrites=true&w=majority"

    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged. Successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    db = client["Youtube"]
    collection = db[mongodb_collection_name]


    # Create a MySQL cursor
    cursor = conn.cursor()

    try:
        # Fetch channel details from MongoDB
        channel_data = collection.find_one({}).get("channel_details", [{}])[0]

        # Insert channel data into MySQL
        sql_channel = """
            INSERT INTO channel 
            (channel_name, channel_id, country, channel_views, subscription, channel_uploads, 
            channel_status, channel_playlist_id) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        channel_values = (
            channel_data.get("channel_name", "N/A"),
            channel_data.get("channel_id", "N/A"),
            channel_data.get("country", "N/A"),
            int(channel_data.get("channel_views", 0)),
            int(channel_data.get("subscription", 0)),
            int(channel_data.get("channel_uploads", 0)),
            channel_data.get("channel_status", "N/A"),
            channel_data.get("channel_playlist_id", "N/A"),
        )
        cursor.execute(sql_channel, channel_values)

        # Fetch video details from MongoDB
        video_data = collection.find_one({}).get("video_details", [])

        # Insert video data into MySQL
        sql_video = """
            INSERT INTO video 
            (channel_name, channel_id, video_id, video_title, duration, release_date, thumbnail, video_quality, 
            views, likes, favorite, comment_count, description, caption_status) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        video_values = [
            (
                video.get("channel_name", "N/A"),
                video.get("channel_id", "N/A"),
                video.get("video_id", "N/A"),
                video.get("video_title", "N/A"),
                int(video.get("duration", 0)),
                datetime.strptime(video.get("release_date", None),"%Y-%m-%dT%H:%M:%S%z"),
                video.get("thumbnail", "N/A"),
                video.get("video_quality", "N/A"),
                int(video.get("views", 0)),
                int(video.get("likes", 0)),
                int(video.get("favorite", 0)),
                int(video.get("comment_count", 0)),
                video.get("description", "N/A"),
                video.get("caption_status", "N/A"),
            ) for video in video_data
            ]
        cursor.executemany(sql_video, video_values)

        # Fetch playlist details from MongoDB
        playlist_data = collection.find_one({}).get("playlist_details", [])

        if playlist_data:
            
            # Insert playlist data into MySQL
            sql_playlist = """
                INSERT INTO playlist 
                (channel_id, playlist_id, playlist_name) 
                VALUES (%s, %s, %s)
            """
            playlist_values = [
                (
                    playlist.get("channel_id", "N/A"),
                    playlist.get("playlist_id", "N/A"),
                    playlist.get("playlist_name", "N/A"),
                ) for playlist in playlist_data
            ]
            cursor.executemany(sql_playlist, playlist_values)

        # Fetch comment details from MongoDB
        comment_data = collection.find_one({}).get("comment_details", [])

        # Insert comment data into MySQL
        sql_comment = """
            INSERT INTO comment 
            (video_id, comment_id, author_name, comments, commented_date) 
            VALUES (%s, %s, %s, %s, %s)
        """
        comment_values = [
            (
                comment.get("video_id", "N/A"),
                comment.get("comment_id", "N/A"),
                comment.get("author_name", "N/A"),
                comment.get("comments", "N/A"),
                datetime.strptime(comment.get("commented_date", None),"%Y-%m-%dT%H:%M:%S%z"),
            ) for comment in comment_data
        ]
        cursor.executemany(sql_comment, comment_values)

        # Commit the changes to MySQL
        conn.commit()
        print(channel_data.get("channel_name", "N/A"), "Data migration from MongoDB to MySQL was successful")
     
    except Exception as e:
        if Exception:
            print('Unsuccessful!,',channel_data.get("channel_name", "N/A"),'already migrated to MySQL or channel has disabled some features')

    finally:
        # Close the MySQL connection
        cursor.close()
        conn.close()
    return print("All Data migration from MongoDB to MySQL successful")


# Function to migrate all the data from MongoDB to MySQL
def migrate_mondodb_to_mysql():
    # Access the specified database
    database = client['Youtube']

    # Get the list of collection names in the connected database
    collection_names = database.list_collection_names()

    # for loop to migrate all the data from MongoDB to MySQL
    for name in collection_names: 
        migrate_data_to_mysql(name)
    
    return print(name," Channel Data migrated from MongoDB to MySQL successful")

st.sidebar.subheader("Data Migration: MongoDB to MySQL")
migrate = st.sidebar.button("Migrate")
if migrate:
   migrate_mondodb_to_mysql()

# MYSQL QUERY

# Connecting to MySQL

# establishing to MySQL
conn = sql.connect(user='root',
                        password='A@sD#F45',
                        host='localhost',
                        database = 'youtube')
if conn:
    print('Connected to MySQL successfully')
else:
    print('Connection Not established')

cursor = conn.cursor()

# MySQL query to fetch data from MySQL database

# 1. What are the names of all the videos and their corresponding channels?
def get_channel_video_names():
    
    sql_channel_name =   """SELECT 
                                channel_name 
                            FROM 
                                channel 
                            WHERE 
                                channel_name is not null;"""
    cursor.execute(sql_channel_name)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    result = pd.DataFrame(result, columns = ['Channel Name'], index=None)
    return result


# 2. Which channels have the most number of videos, and how many videos do they have?

def get_channel_max_video_count():
    sql_channel_video_count = """SELECT 
                                    channel_name, 
                                    channel_uploads 
                                FROM 
                                    channel 
                                ORDER BY 
                                    channel_uploads desc"""
    cursor.execute(sql_channel_video_count)
    result = cursor.fetchall()
    result = pd.DataFrame(result, columns = ['Channel Name', 'Videos'])
    
    # Streamlit bar chart
    st.bar_chart(result.set_index('Channel Name'))
   
    return result  


# 3. What are the top 10 most viewed videos and their respective channels?

def get_top10_most_viewed_videos():
    sql_top10_videos =  """SELECT 
                                channel_name, 
                                video_title, 
                                views
                           FROM 
                                video
                           ORDER BY 
                                views desc
                           limit 10"""

    cursor.execute(sql_top10_videos)
    result = cursor.fetchall()
    result = pd.DataFrame(result, columns = ['Channel Name', 'Video Title', 'Views'])
    x_data = result['Video Title'].tolist()
    y_data = result['Views'].tolist()
    result_chart = pd.DataFrame({'Video Title': x_data, 'Views': y_data})
    # Streamlit bar chart
    st.bar_chart(result_chart.set_index('Video Title'))
    
    return result


# 4. How many comments were made on each video, and what are their corresponding video names?

def get_video_comment_count():
    sql_video_comment_count = """SELECT channel_name, video_title, comment_count
                                FROM video
                                ORDER BY comment_count desc"""
    cursor.execute(sql_video_comment_count)
    result = cursor.fetchall()
    result = pd.DataFrame(result, columns = ['Channel Name', 'Video Title', 'Comment Count'])
    return result


# 5. Which videos have the highest number of likes, and what are their corresponding channel names?

def get_max_video_likes():
    sql_max_video_likes =    """SELECT v.channel_name, v.video_title, v.likes
                                FROM video v
                                JOIN (
                                    SELECT channel_id, MAX(likes) AS max_likes
                                    FROM video
                                    GROUP BY channel_id
                                    ) max_likes_per_channel
                                ON v.channel_id = max_likes_per_channel.channel_id
                                AND v.likes = max_likes_per_channel.max_likes
                                order by v.likes DESC"""
    cursor.execute(sql_max_video_likes)
    result = cursor.fetchall()
    result = pd.DataFrame(result, columns = ['Channel Name', 'Video Title', 'Likes'])
    return result

    
# 6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?

def get_max_likes_allvideos():
    sql_max_likes_allvideos = """select channel_name, video_title, likes
                                from video
                                order by likes desc
                                limit 10; """   # Top 10 videos
    cursor.execute(sql_max_likes_allvideos)
    result = cursor.fetchall()
    result = pd.DataFrame(result, columns = ['Channel Name', 'Video Title', 'Likes'])
    return result


# 7. What is the total number of views for each channel, and what are their corresponding channel names?

def get_max_views():
    sql_max_views = """select channel_name, channel_views
                        from channel
                        order by channel_views desc; """
    
    cursor.execute(sql_max_views)
    result = cursor.fetchall()
    result = pd.DataFrame(result, columns = ['Channel Name', 'Channel Views'])
    return result


# 8. What are the names of all the channels that have published videos in the year 2022?

def get_video_year():
    sql_video_year = """select distinct channel_name
                        from video
                        where year(release_date) = 2022"""
    cursor.execute(sql_video_year)
    result = cursor.fetchall()
    result = pd.DataFrame(result, columns = ['Channel Name'])
    return result



# 9. What is the average duration of all videos in each channel, and what are their corresponding channel names?

def get_avg_duration():
    sql_avg_duration = """SELECT channel_name, avg(duration) AS avg_time
                            FROM video
                            group by channel_name 
                            order by avg_time desc"""
    cursor.execute(sql_avg_duration)
    result = cursor.fetchall()
    result = pd.DataFrame(result, columns = ['Channel Name', 'Average Duration'])
    
    return result

# 10. Which videos have the highest number of comments, and what are their corresponding channel names?

def get_max_comments():
    sql_max_comments_count = """select channel_name, video_title, comment_count
                                from video
                                order by comment_count desc
                                limit 10"""                    # Top 10 comments
    
    cursor.execute(sql_max_comments_count)
    result = cursor.fetchall()
    result = pd.DataFrame(result, columns = ['Channel Name', 'Video Title', 'Channel Comment Count'])
    return result


sql_query =st.selectbox( "Select option", ('Select Option','1. What are the names of all the videos and their corresponding channels?',
                                '2. Which channels have the most number of videos, and how many videos do they have?',
                                '3. What are the top 10 most viewed videos and their respective channels?',
                                '4. How many comments were made on each video, and what are their corresponding video names?',
                                '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                                '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                                '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                                '8. What are the names of all the channels that have published videos in the year 2022?',
                                '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                '10. Which videos have the highest number of comments, and what are their corresponding channel names?'
                            ))

if sql_query == '1. What are the names of all the videos and their corresponding channels?':
   st.table(get_channel_video_names())
elif sql_query == '2. Which channels have the most number of videos, and how many videos do they have?':
   st.table(get_channel_max_video_count())
elif sql_query == '3. What are the top 10 most viewed videos and their respective channels?':
   st.table(get_top10_most_viewed_videos())
elif sql_query == '4. How many comments were made on each video, and what are their corresponding video names?':
   st.table(get_video_comment_count())
elif sql_query == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
   st.table(get_max_video_likes())
elif sql_query == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
   st.table(get_max_likes_allvideos())
elif sql_query == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
   st.table(get_max_views())
elif sql_query == '8. What are the names of all the channels that have published videos in the year 2022?':
   st.table(get_video_year())
elif sql_query == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
   st.table(get_avg_duration())
elif sql_query == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
   st.table(get_max_comments())
else: pass


# ------------------END OF THE CODE---------------------------------------
