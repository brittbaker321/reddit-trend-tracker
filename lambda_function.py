import os
import json
import praw
import boto3
import csv
import io
from datetime import datetime, timedelta
from collections import Counter
from snowflake.connector import connect
from snowflake.connector.errors import Error, OperationalError, ProgrammingError

try:
    import config as cfg  # Local development settings
except ImportError:
    import config_template as cfg  # Template settings for deployment

def get_secrets():
    """Retrieve secrets from AWS Secrets Manager"""
    start = datetime.now()
    print(f"Starting secrets retrieval at {start}")
    
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=cfg.REGION_NAME)
    
    try:
        response = client.get_secret_value(SecretId=cfg.SECRET_NAME)
        secret = json.loads(response['SecretString'])
        end = datetime.now()
        print(f"Secrets retrieval took: {end - start}")
        return secret
    except Exception as e:
        print(f"Error retrieving secrets: {str(e)}")
        raise

def get_snowflake_connection():
    """Establish Snowflake connection using secrets"""
    start = datetime.now()
    print(f"Starting Snowflake connection at {start}")
    
    secrets = get_secrets()
    try:
        conn = connect(
            user=secrets['user'],
            password=secrets['password'],
            account=secrets['account'],
            warehouse=secrets['warehouse'],
            database=secrets['database'],
            schema=secrets['schema']
        )
        
        # Explicitly set database and schema
        cur = conn.cursor()
        cur.execute(f"USE DATABASE {secrets['database']}")
        cur.execute(f"USE SCHEMA {secrets['schema']}")
        cur.close()
        
        end = datetime.now()
        print(f"Snowflake connection took: {end - start}")
        return conn
    except Error as e:
        print(f"Snowflake error: {str(e)}")
        raise

def get_reddit_connection():
    """Establish Reddit connection using environment variables"""
    start = datetime.now()
    print(f"Starting Reddit connection at {start}")
    
    try:
        reddit = praw.Reddit(
            client_id=os.environ['REDDIT_CLIENT_ID'],
            client_secret=os.environ['REDDIT_CLIENT_SECRET'],
            user_agent=os.environ['REDDIT_USER_AGENT']
        )
        end = datetime.now()
        print(f"Reddit connection took: {end - start}")
        return reddit
    except Exception as e:
        print(f"Reddit connection error: {str(e)}")
        raise

def load_keywords_from_s3():
    """Load keywords from S3 CSV file"""
    start = datetime.now()
    print(f"Starting S3 keywords load at {start}")
    print(f"Attempting to load from bucket: {cfg.BUCKET_NAME}, key: {cfg.KEYWORDS_KEY}")
    
    s3 = boto3.client('s3')
    try:
        # List bucket contents for debugging
        print("Listing bucket contents:")
        list_start = datetime.now()
        response = s3.list_objects_v2(Bucket=cfg.BUCKET_NAME, Prefix='data_eng/')
        for obj in response.get('Contents', []):
            print(f"Found object: {obj['Key']}")
        print(f"Listing bucket contents took: {datetime.now() - list_start}")
        
        # Get and process the file
        get_start = datetime.now()
        obj = s3.get_object(Bucket=cfg.BUCKET_NAME, Key=cfg.KEYWORDS_KEY)
        csv_content = obj['Body'].read().decode('utf-8')
        print(f"Getting S3 object took: {datetime.now() - get_start}")
        
        # Process keywords
        process_start = datetime.now()
        keywords = set()
        for row in csv.reader(io.StringIO(csv_content)):
            keywords.update(word.lower().strip() for word in row if word.strip())
        print(f"Processing keywords took: {datetime.now() - process_start}")
        
        end = datetime.now()
        print(f"Total S3 keywords load took: {end - start}")
        print(f"Loaded {len(keywords)} keywords")
        return keywords
    except Exception as e:
        print(f"S3 error: {str(e)}")
        raise

def analyze_reddit_trends():
    """Analyze Reddit trends based on keyword mentions for previous day"""
    start = datetime.now()
    print(f"Starting Reddit analysis at {start}")
    
    try:
        # Get connections and keywords
        conn_start = datetime.now()
        reddit = get_reddit_connection()
        keywords = load_keywords_from_s3()
        print(f"Getting connections and keywords took: {datetime.now() - conn_start}")
        
        trends = Counter()
        subreddit = reddit.subreddit(cfg.SUBREDDIT_NAME)
        
        # Calculate yesterday's date range
        today = datetime.utcnow()
        yesterday = today - timedelta(days=1)
        yesterday_start = int(datetime(yesterday.year, yesterday.month, yesterday.day).timestamp())
        yesterday_end = int(datetime(today.year, today.month, today.day).timestamp())
        
        print(f"Collecting posts from {datetime.fromtimestamp(yesterday_start)} to {datetime.fromtimestamp(yesterday_end)}")
        
        # Use timestamp-based filtering
        posts = subreddit.new(limit=cfg.POST_LIMIT)
        
        # Process posts
        posts_start = datetime.now()
        post_count = 0
        for post in posts:
            # Skip posts not from yesterday
            if not (yesterday_start <= post.created_utc <= yesterday_end):
                continue
                
            post_count += 1
            # Process title and text
            title = post.title.lower()
            trends.update(keyword for keyword in keywords if keyword in title)
            
            if post.selftext:
                text = post.selftext.lower()
                trends.update(keyword for keyword in keywords if keyword in text)
            
            # Process comments
            try:
                post.comments.replace_more(limit=0)
                comments = post.comments.list()[:cfg.INITIAL_COMMENT_FETCH]
                top_comments = sorted(comments, 
                                   key=lambda x: x.score if hasattr(x, 'score') else 0, 
                                   reverse=True)[:cfg.TOP_COMMENTS_LIMIT]
                
                for comment in top_comments:
                    if not hasattr(comment, 'created_utc') or not (yesterday_start <= comment.created_utc <= yesterday_end):
                        continue
                    comment_text = comment.body.lower() if hasattr(comment, 'body') else ""
                    trends.update(keyword for keyword in keywords if keyword in comment_text)
            except Exception as e:
                print(f"Error processing comments for post {post.id}: {str(e)}")
            
            if post_count % 10 == 0:
                print(f"Processed {post_count} posts at {datetime.now()}")
        
        print(f"Processing posts took: {datetime.now() - posts_start}")
        
        end = datetime.now()
        print(f"Total Reddit analysis took: {end - start}")
        print(f"Found {len(trends)} trending keywords from {post_count} posts")
        return dict(trends), yesterday.date()
    except Exception as e:
        print(f"Reddit analysis error: {str(e)}")
        raise

def save_to_snowflake(trends_data, snapshot_date):
    """Save trends data to Snowflake"""
    start = datetime.now()
    print(f"Starting Snowflake save at {start}")
    
    conn = get_snowflake_connection()
    cur = conn.cursor()
    
    try:
        # Check if we already have data for this date
        check_start = datetime.now()
        cur.execute("""
        SELECT COUNT(*) 
        FROM REDDIT_TRENDS 
        WHERE SNAPSHOT_DATE = %s
        """, (snapshot_date,))
        count = cur.fetchone()[0]
        
        if count > 0:
            print(f"Data already exists for {snapshot_date}, skipping insertion")
            return
            
        print(f"Data check took: {datetime.now() - check_start}")
        
        # Prepare and insert data
        insert_start = datetime.now()
        snapshot_time = datetime.utcnow()
        records = [(snapshot_time, snapshot_date, keyword, count) 
                  for keyword, count in trends_data.items()]
        
        cur.executemany("""
        INSERT INTO REDDIT_TRENDS (SNAPSHOT_TIME, SNAPSHOT_DATE, KEYWORD, MENTION_COUNT)
        VALUES (%s, %s, %s, %s)
        """, records)
        
        conn.commit()
        print(f"Data insertion took: {datetime.now() - insert_start}")
        
        end = datetime.now()
        print(f"Total Snowflake save took: {end - start}")
        print(f"Inserted {len(records)} records for {snapshot_date}")
        
    except Exception as e:
        print(f"Snowflake save error: {str(e)}")
        raise
    finally:
        cur.close()
        conn.close()

def lambda_handler(event, context):
    """AWS Lambda handler"""
    overall_start = datetime.now()
    print(f"Starting Lambda execution at {overall_start}")
    
    try:
        # Main execution
        trends, snapshot_date = analyze_reddit_trends()
        save_to_snowflake(trends, snapshot_date)
        
        overall_end = datetime.now()
        print(f"Total Lambda execution took: {overall_end - overall_start}")
        
        return {
            'statusCode': 200,
            'body': json.dumps('Successfully processed and saved Reddit trends'),
            'executionTime': str(overall_end - overall_start),
            'date_processed': str(snapshot_date)
        }
    except Exception as e:
        print(f"Lambda execution error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }

if __name__ == "__main__":
    lambda_handler({}, {})
