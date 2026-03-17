import pandas as pd
import isodate
"""----------------------------------------------------------------------------------------------------------------------------------------------------"""
def transform_channel_comments(channel_comments):
    transformed = []
    #flatten channel comments into a dictionary for easier analysis/saving to CSv
    for comment in channel_comments:
        channel_id = comment['snippet']['channelId']
        comment_id = comment['id']
        author_display_name = comment['snippet']['topLevelComment']['snippet']['authorDisplayName']
        author_channel_id = comment['snippet']['topLevelComment']['snippet'].get('authorChannelId', {}).get('value')
        text_display = comment['snippet']['topLevelComment']['snippet']['textDisplay']
        like_count = comment['snippet']['topLevelComment']['snippet']['likeCount']
        published_at = comment['snippet']['topLevelComment']['snippet']['publishedAt']
        transformed.append((channel_id, comment_id, author_display_name, author_channel_id, text_display, like_count, published_at))
    
    # Create a DataFrame from the transformed list of tuples
    df = pd.DataFrame(transformed, columns=['channel_id', 'comment_id', 'author_display_name', 'author_channel_id', 'text_display', 'like_count', 'published_at'])
    
    return df

"""----------------------------------------------------------------------------------------------------------------------------------------------------"""
def transform_video_comments(video_comments):
    transformed = []
    #flatten video comments into a dictionary for easier analysis/saving to CSV
    for comment in video_comments:
        video_id = comment['snippet']['videoId']
        comment_id = comment['id']
        author_display_name = comment['snippet']['topLevelComment']['snippet']['authorDisplayName']
        author_channel_id = (comment['snippet']['topLevelComment']['snippet'].get('authorChannelId', {}).get('value'))
        text_display = comment['snippet']['topLevelComment']['snippet']['textDisplay']
        like_count = comment['snippet']['topLevelComment']['snippet']['likeCount']
        published_at = comment['snippet']['topLevelComment']['snippet']['publishedAt']
        transformed.append((video_id, comment_id, author_display_name, author_channel_id, text_display, like_count, published_at))

    # Create a DataFrame from the transformed list of tuples
    df = pd.DataFrame(transformed, columns=['video_id', 'comment_id', 'author_display_name', 'author_channel_id', 'text_display', 'like_count', 'published_at'])
    
    return df

"""----------------------------------------------------------------------------------------------------------------------------------------------------"""
def transform_channel_details(channel_details):
    transformed = []
    #flatten channel details into a dictionary for easier analysis/saving to CSV
    for item in channel_details:
        channel_id = item.get('id')
        title = item['snippet'].get('title')
        description = item['snippet'].get('description')
        subscriber_count = item['statistics'].get('subscriberCount')
        video_count = item['statistics'].get('videoCount')
        transformed.append((channel_id, title, description, subscriber_count, video_count))

    # Create a DataFrame from the transformed list of tuples
    df = pd.DataFrame(transformed, columns=['channel_id', 'title', 'description', 'subscriber_count', 'video_count'])
    
    return df

"""----------------------------------------------------------------------------------------------------------------------------------------------------"""
def transform_video_details(video_details):
  # Check if video_details is empty or None
    if not video_details:
      return pd.DataFrame()
    
    transformed = []
    #flatten video details into a dictionary for easier analysis/saving to CSV
    for item in video_details:
        video_id = item.get('id')
        published_date = item['snippet'].get('publishedAt')
        channel_id = item['snippet'].get('channelId')
        title = item['snippet'].get('title')
        description = item['snippet'].get('description')
        tags = item['snippet'].get('tags', [])
        channel_title = item['snippet'].get('channelTitle')
        view_count = item['statistics'].get('viewCount')
        like_count = item['statistics'].get('likeCount')
        comment_count = item['statistics'].get('commentCount')
        duration = item['contentDetails'].get('duration')
        transformed.append((video_id, published_date, channel_id, title, description, tags, channel_title, view_count, like_count, comment_count, duration))
      
# Create a DataFrame from the transformed list of tuples
    df = pd.DataFrame(transformed, columns=['video_id', 'published_date', 'channel_id', 'title', 'description', 'tags', 'channel_title', 'view_count', 'like_count', 'comment_count', 'duration'])
    df['published_date'] = pd.to_datetime(df['published_date'])

# Convert view_count, like_count, and comment_count to numeric values, coercing errors to NaN and then filling NaN with 0 before converting to integers
    df['view_count'] = pd.to_numeric(df['view_count'], errors='coerce')
    df['like_count'] = pd.to_numeric(df['like_count'], errors='coerce')
    df['comment_count'] = pd.to_numeric(df['comment_count'], errors='coerce')
    df[['view_count', 'like_count', 'comment_count']] = df[['view_count', 'like_count', 'comment_count']].fillna(0).astype(int)

# Convert ISO 8601 duration to total seconds and rename column
    df['duration'] = (df['duration'].apply(lambda x: isodate.parse_duration(x).total_seconds()))
    df = df.rename(columns={'duration': 'duration_seconds'})

# drop duplicates after running through the loop to ensure we only have unique video/channel combinations in the final DataFrame, then reset index to clean up after dropping duplicates
    df = df.reset_index(drop=True)

# Strip leading and trailing whitespace from string columns
    df['title'] = df['title'].str.strip()
    df['description'] = df['description'].str.strip()
    df['channel_title'] = df['channel_title'].str.strip()

# Convert tags list to a comma-separated string, handling cases where tags may be missing or not a list
    df['tags'] = df['tags'].apply(lambda x: ', '.join(x) if isinstance(x, list) else '')

# Calculate like-to-view and comment-to-view ratios, handling division by zero by replacing zero view counts with NaN before division and then filling resulting NaN with 0
    df['like_view_ratio'] = (df['like_count'] / df['view_count'].replace(0, pd.NA)).round(4)
    df['comment_view_ratio'] = (df['comment_count'] / df['view_count'].replace(0, pd.NA)).round(4)

# Calculate video age in days by subtracting the published date from the current date and converting the result to days
    df['video_age_days'] = (pd.Timestamp.now() - df['published_date']).dt.days

# Remove underscores from column names for better readability
    df.columns = df.columns.str.replace('_', '')
    

    return df

"""----------------------------------------------------------------------------------------------------------------------------------------------------"""
def transform_search_results(search_results):
    transformed = []
    
    for item in search_results:
        # flatten search results into a DataFrame for easier analysis/saving to CSV
        video_id = item['id'].get('videoId')
        channel_id = item['snippet'].get('channelId')
        video_title = item['snippet'].get('title')
        description = item['snippet'].get('description')
        published_date = item['snippet'].get('publishedAt')
        channel_title = item['snippet'].get('channelTitle')
        transformed.append((channel_id, channel_title, video_id, video_title, description, published_date))
    
    # Create a DataFrame from the transformed list of tuples
    df = pd.DataFrame(transformed, columns=['channel_id', 'channel_title', 'video_id', 'video_title', 'description', 'published_date'])
    
#drop duplicates after running through the loop to ensure we only have unique video/channel combinations in the final DataFrame
    df = df.drop_duplicates(subset=['video_id'])

# Convert published_date to datetime format and sort by published_date in descending order to have the most recent videos at the top, then reset index to clean up after dropping duplicates
    df['published_date'] = pd.to_datetime(df['published_date'])

# Strip leading and trailing whitespace from string columns
    df['video_title'] = df['video_title'].str.strip()
    df['description'] = df['description'].str.strip()
    df['channel_title'] = df['channel_title'].str.strip()

#reset index to clean up after dropping duplicates and sorting
    df = df.reset_index(drop=True)

# Remove underscores from column names for better readability
    df.columns = df.columns.str.replace('_', '')

    return df


