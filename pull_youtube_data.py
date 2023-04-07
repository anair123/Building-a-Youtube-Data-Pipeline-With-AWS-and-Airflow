from googleapiclient.discovery import build
from datetime import date
import pandas as pd
import boto3
import io
import config
import time

def pull_data(region_code):
    
    YOUTUBE_API_KEY = config.YOUTUBE_API_KEY
    AWS_KEY_ID = config.AWS_KEY_ID
    AWS_SECRET = config.AWS_SECRET

    api_service_name = "youtube"
    api_version = "v3"
    # create an API client
    youtube = build(
        api_service_name, api_version, developerKey=YOUTUBE_API_KEY)
    
    # make a request for the top videos
    request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        chart="mostPopular",
        regionCode=region_code,
        maxResults=50
    )
    response = request.execute()

    # get current date
    today = date.today()

    # store response in a data frame
    df = pd.DataFrame(response['items'])
    df = pd.concat([df.drop('snippet', axis=1), pd.DataFrame(df['snippet'].tolist())], axis=1)
    df = pd.concat([df.drop('statistics', axis=1), pd.DataFrame(df['statistics'].tolist())], axis=1)


    # replace the category id with the name
    dict_category = {1 : 'Film & Animation', 2 : 'Autos & Vehicles', 10 : 'Music', 15 : 'Pets & Animals', 17 : 'Sports', 18 : 'Short Movies', 19 : 'Travel & Events',
                     20 : 'Gaming', 21 : 'Videoblogging', 22 : 'People & Blogs', 23 : 'Comedy', 24 : 'Entertainment', 25 : 'News & Politics', 26 : 'Howto & Style', 27 : 'Education',
                     28 : 'Science & Technology', 29 : 'Nonprofits & Activism', 30 : 'Movies', 31 : 'Anime/Animation', 32 : 'Action/Adventure', 33 : 'Classics', 34 : 'Comedy',
                     35 : 'Documentary', 36 : 'Drama', 37 : 'Family', 38 : 'Foreign', 39 : 'Horror', 40 : 'Sci-Fi/Fantasy', 41 : 'Thriller', 42 : 'Shorts', 43 : 'Shows', 44 : 'Trailers'}
    df['categoryId'] = df['categoryId'].astype(int)
    df['category'] = df['categoryId'].map(dict_category)
    df.insert(0, "date_of_extraction", today)
    df.insert(1, "country", region_code)

    # columns to keep
    columns = ['date_of_extraction', 'country','id', 'title', 'description', 'channelId', 'channelTitle', 'category', 'viewCount', 'likeCount', 'favoriteCount', 'commentCount']
    df = df[columns]
    df = df.dropna()

    int_col = df.columns[-4:]
    for col in int_col:
        df[col] = df[col].astype(int)
    str_col = df.columns[:-4]
    for col in str_col:
        df[col] = df[col].astype(str)
    

    df = df[['date_of_extraction',  'country','id', 'title',  'channelId','channelTitle', 'category', 'viewCount', 'likeCount', 'favoriteCount', 'commentCount']]

    print(len(df.columns))
    print(df.dtypes)
   
    # create a client
    s3 = boto3.client(service_name='s3',
                    region_name='us-east-1',
                    aws_access_key_id = AWS_KEY_ID,
                    aws_secret_access_key = AWS_SECRET)

    # upload pandas data frame to bucket
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, sep=',')
    s3.put_object(Bucket='youtube-data-storage', 
                  Body=csv_buffer.getvalue(), 
                  Key=f'data/{region_code} videos {today}.csv')

    return None

def write_queries():

    AWS_KEY_ID = config.AWS_KEY_ID
    AWS_SECRET = config.AWS_SECRET
    # Initialize Athena client
    athena_client = boto3.client('athena',
                    region_name='us-east-1',
                    aws_access_key_id = AWS_KEY_ID,
                    aws_secret_access_key = AWS_SECRET)

    # Set the database and table name
    database_name = 'default'
    table_name = 'youtube_videos'
    bucket = 'youtube-data-storage'
    query_dir = 'query-output'

    # Set the S3 output location for query results
    s3_output_location = f's3://{bucket}/{query_dir}/'

    query = '''
        WITH category_count AS (

        SELECT DATE(date_of_extraction) AS date_of_extraction, country, category, COUNT(*) AS num_videos
        FROM youtube_videos
        GROUP BY DATE(date_of_extraction), country,category
        ),
        category_rank AS (
        SELECT date_of_extraction, country, category, num_videos,
                RANK() OVER(PARTITION BY  date_of_extraction, country ORDER BY num_videos DESC) AS rk
        FROM category_count)

        SELECT date_of_extraction,country, category AS most_popular_category, num_videos
        FROM category_rank
        WHERE rk = 1
        ORDER BY date_of_extraction, country;
        '''
    
    # Run the query in Athena
    query_execution = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database_name
        },
        ResultConfiguration={
            'OutputLocation': s3_output_location
        }
    )

    # Get the query execution ID
    query_execution_id = query_execution['QueryExecutionId']
    print('Query Execution ID:', query_execution_id)

    time.sleep(10)

    # Get the query results
    query_results = athena_client.get_query_results(
        QueryExecutionId=query_execution_id)

    # Extract the results and save to a local file
    with open(f'most_popular_categories.csv', 'w', encoding="utf-8") as f:
        for row in query_results['ResultSet']['Rows']:
            f.write(','.join([data['VarCharValue'] for data in row['Data']]) + '\n')

    # Upload the query results to S3
    s3_client = boto3.client('s3',
                             region_name='us-east-1',
                            aws_access_key_id = AWS_KEY_ID,
                            aws_secret_access_key = AWS_SECRET)
    s3_client.upload_file('most_popular_categories.csv', 'youtube-data-storage', 'query-output/most_liked_videos.csv')

    # delete Athena's generated query and metadata file
    s3_client.delete_object(
    Bucket=bucket,
    Key=f'query-output/{query_execution_id}.csv')

    s3_client.delete_object(
    Bucket=bucket,
    Key=f'query-output/{query_execution_id}.csv.metadata')

if __name__ == '__main__':
    # India, United States, Brazil, Indonesia, Mexico
    # codes: IN, US, BR, ID, MX
    #pull_data(region_code='IN') # India
    #pull_data(region_code='US') # United States
    #pull_data(region_code='BR') # Brazil
    #pull_data(region_code='ID') # Indonesia
    #pull_data(region_code='MX') # Mexico

    write_queries()



# ATHENA CODE
'''
CREATE DATABASE youtube
'''


'''
CREATE EXTERNAL TABLE youtube_videos (
  date_of_extraction STRING,
  country STRING,
  video_id STRING,
  video_name STRING,
  channel_id STRING,
  channel_name STRING,
  category STRING,
  view_count INT,
  like_count INT,
  favorite_count INT,
  comment_count INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
LOCATION 's3://youtube-data-storage/data/'
TBLPROPERTIES ('skip.header.line.count'='1')
'''