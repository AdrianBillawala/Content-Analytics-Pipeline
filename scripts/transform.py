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
        snippet = item.get('snippet', {})
        statistics = item.get('statistics', {})
        content_details = item.get('contentDetails', {})

        published_date = snippet.get('publishedAt')
        channel_id = snippet.get('channelId')
        title = snippet.get('title')
        description = snippet.get('description')
        tags = snippet.get('tags', [])
        channel_title = snippet.get('channelTitle')
        view_count = statistics.get('viewCount')
        like_count = statistics.get('likeCount')
        comment_count = statistics.get('commentCount')
        duration = content_details.get('duration')
        transformed.append((video_id, published_date, channel_id, title, description, tags, channel_title, view_count, like_count, comment_count, duration))
      
    # Create a DataFrame from the transformed list of tuples
    df = pd.DataFrame(transformed, columns=['video_id', 'published_date', 'channel_id', 'title', 'description', 'tags', 'channel_title', 'view_count', 'like_count', 'comment_count', 'duration'])
    df['published_date'] = pd.to_datetime(df['published_date'], utc=True)

    # Convert view_count, like_count, and comment_count to numeric values, coercing errors to NaN and then filling NaN with 0 before converting to integers
    df['view_count'] = pd.to_numeric(df['view_count'], errors='coerce')
    df['like_count'] = pd.to_numeric(df['like_count'], errors='coerce')
    df['comment_count'] = pd.to_numeric(df['comment_count'], errors='coerce')
    df[['view_count', 'like_count', 'comment_count']] = df[['view_count', 'like_count', 'comment_count']].fillna(0).astype(int)

    # Convert ISO 8601 duration to seconds safely (missing/invalid values become NA).
    def _duration_to_seconds(value):
        if not value:
            return pd.NA
        try:
            return isodate.parse_duration(value).total_seconds()
        except (TypeError, ValueError, AttributeError):
            return pd.NA

    df['duration_seconds'] = df['duration'].apply(_duration_to_seconds)
    df['duration_seconds'] = pd.to_numeric(df['duration_seconds'], errors='coerce')
    df = df.drop(columns=['duration'])

    #drop duplicates after running through the loop to ensure we only have unique video/channel combinations in the final DataFrame, then reset index to clean up after dropping duplicates
    df = df.drop_duplicates(subset=['video_id']).reset_index(drop=True)

    # Strip leading and trailing whitespace from string columns
    df['title'] = df['title'].str.strip()
    df['description'] = df['description'].str.strip()
    df['channel_title'] = df['channel_title'].str.strip()

    # Normalize tags so each cell contains a list of stripped strings.
    df['tags'] = df['tags'].apply(
        lambda x: [tag.strip() for tag in x if isinstance(tag, str)] if isinstance(x, list)
        else [tag.strip() for tag in x.split(',')] if isinstance(x, str) and x
        else []
    )

    # Calculate like-to-view and comment-to-view ratios while preserving numeric dtype.
    safe_view_count = df['view_count'].astype(float).replace(0, float('nan'))
    df['like_view_ratio'] = (df['like_count'] / safe_view_count).round(4)
    df['comment_view_ratio'] = (df['comment_count'] / safe_view_count).round(4)

    # Calculate video age in days by subtracting the published date from the current date and converting the result to days
    df['video_age_days'] = (pd.Timestamp.now(tz='UTC') - df['published_date']).dt.days
    

    return df

"""----------------------------------------------------------------------------------------------------------------------------------------------------"""

def transform_search_results(search_results):
    transformed = []

    if not search_results:
        return pd.DataFrame()

    # ✅ detect once
    first_kind = search_results[0].get('id', {}).get('kind')

    if first_kind == 'youtube#video':
        search_type = 'video'
    elif first_kind == 'youtube#channel':
        search_type = 'channel'
    else:
        return pd.DataFrame()

    for item in search_results:
        snippet = item.get('snippet', {})
        item_id = item.get('id', {})
        kind = item_id.get('kind')

        # enforce consistency
        if search_type == 'video' and kind != 'youtube#video':
            continue
        if search_type == 'channel' and kind != 'youtube#channel':
            continue

        if search_type == 'video':
            transformed.append((
                snippet.get('channelId'),
                snippet.get('channelTitle'),
                item_id.get('videoId'),
                snippet.get('title'),
                snippet.get('description'),
                snippet.get('publishedAt')
            ))

        else:  # channel
            transformed.append((
                item_id.get('channelId'),
                snippet.get('title'),
                snippet.get('description'),
                snippet.get('publishedAt')
            ))

    # -------------------------
    # Build DataFrame
    # -------------------------
    if search_type == 'video':
        df = pd.DataFrame(transformed, columns=[
            'channel_id', 'channel_title', 'video_id',
            'video_title', 'description', 'published_date'
        ])
        df = df.drop_duplicates(subset=['video_id'])
        string_cols = ['channel_title', 'video_title', 'description']

    else:
        df = pd.DataFrame(transformed, columns=[
            'channel_id', 'channel_title', 'description', 'published_date'
        ])
        df = df.drop_duplicates(subset=['channel_id'])
        string_cols = ['channel_title', 'description']

    if df.empty:
        return df

    # Convert published_date to datetime, coercing errors to NaT (Not a Time) for invalid formats
    df['published_date'] = pd.to_datetime(df['published_date'], utc=True, errors='coerce')

    # Strip leading and trailing whitespace from string columns, filling NaN with empty strings first to avoid errors.
    for col in string_cols:
        df[col] = df[col].fillna('').str.strip()

    return df.reset_index(drop=True)


