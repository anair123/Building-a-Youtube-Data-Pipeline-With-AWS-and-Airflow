import boto3
import config
import time

def run_query():

    AWS_KEY_ID = config.AWS_KEY_ID
    AWS_SECRET = config.AWS_SECRET
    # Initialize Athena client
    athena_client = boto3.client('athena',
                    region_name='us-east-1',
                    aws_access_key_id = AWS_KEY_ID,
                    aws_secret_access_key = AWS_SECRET)

    # Set the database and table name
    database_name = 'youtube'
    table_name = 'youtube_videos'
    bucket = 'youtube-data-storage'
    query_dir = 'query-output'

    # Set the S3 output location for query results
    s3_output_location = f's3://{bucket}/{query_dir}/'

    query = '''
        WITH category_count AS (

        SELECT DATE(date_of_extraction) AS date_of_extraction, 
            CASE country 
                WHEN 'US' THEN 'United States' 
                WHEN 'ID' THEN 'Indonesia'
                WHEN 'IN' THEN 'India'
                WHEN 'MX' THEN 'Mexico'
                WHEN 'BR' THEN 'Brazil' END AS country,
            category, 
            COUNT(*) AS num_videos
        FROM youtube_videos
        GROUP BY DATE(date_of_extraction), country,category
        ),
        category_rank AS (
        SELECT date_of_extraction, country, category, num_videos,
                RANK() OVER(PARTITION BY  date_of_extraction, country ORDER BY num_videos DESC) AS rk
        FROM category_count)

        SELECT date_of_extraction,
            country, 
            category AS most_popular_category, num_videos
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
    time.sleep(10)

    # Get the query results
    query_results = athena_client.get_query_results(
        QueryExecutionId=query_execution_id)
    
    # Extract the results and save to a local file
    file_name = 'most_popular_categories.csv'
    with open(file_name, 'w', encoding="utf-8") as f:
        for row in query_results['ResultSet']['Rows']:
            f.write(','.join([data['VarCharValue'] for data in row['Data']]) + '\n')

    # Upload the query results to S3
    s3_client = boto3.client('s3',
                            region_name='us-east-1',
                            aws_access_key_id = AWS_KEY_ID,
                            aws_secret_access_key = AWS_SECRET)
    s3_client.upload_file(file_name, bucket, f'{query_dir}/{file_name}')

    # delete Athena's generated csv and metadata files
    s3_client.delete_object(
        Bucket=bucket,
        Key=f'query-output/{query_execution_id}.csv')
    s3_client.delete_object(
        Bucket=bucket,
        Key=f'query-output/{query_execution_id}.csv.metadata')

    return None

if __name__ =='__main__':
    run_query()