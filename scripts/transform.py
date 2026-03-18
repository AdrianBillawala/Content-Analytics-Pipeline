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
def transform_youtube_video_comments(video_comments):
    transformed = []
    #flatten video comments into a dictionary for easier analysis/saving to CSV
    for comment in video_comments:
        top_level_comment = comment.get('snippet', {}).get('topLevelComment', {})
        snippet = top_level_comment.get('snippet', {})

        video_id = comment.get('snippet', {}).get('videoId')
        comment_id = top_level_comment.get('id')
        author_display_name = snippet.get('authorDisplayName')
        text_display = snippet.get('textDisplay')
        like_count = snippet.get('likeCount')
        published_at = snippet.get('publishedAt')
        transformed.append((video_id, comment_id, author_display_name, text_display, like_count, published_at))

    # Create a DataFrame from the transformed list of tuples
    df = pd.DataFrame(transformed, columns=['video_id', 'comment_id', 'author_display_name', 'text_display', 'like_count', 'published_at'])
    
    # Convert published_at to datetime, coercing errors to NaT (Not a Time) for invalid formats
    df['published_at'] = pd.to_datetime(df['published_at'], utc=True, format='ISO8601', errors='coerce')
    #drop duplicates after running through the loop to ensure we only have unique video/comment combinations in the final DataFrame, then reset index to clean up after dropping duplicates
    df = df.drop_duplicates(subset=['comment_id']).reset_index(drop=True)
    # Strip leading and trailing whitespace from text_display column, filling NaN with empty strings first to avoid errors.
    df['text_display'] = df['text_display'].fillna('').str.strip()
    # Convert like_count to numeric and sort comments by likes in descending order to identify the most engaging comments for each video
    df['like_count'] = pd.to_numeric(df['like_count'], errors='coerce').fillna(0).astype(int)
    df = df.sort_values(by='like_count', ascending=False).reset_index(drop=True)

    return df

"""----------------------------------------------------------------------------------------------------------------------------------------------------"""
def transform_youtube_channel_details(channel_details):
    transformed = []
    #flatten channel details into a dictionary for easier analysis/saving to CSV
    for item in channel_details:
        snippet = item.get('snippet', {})
        statistics = item.get('statistics', {})

        channel_id = item.get('id')
        title = snippet.get('title')
        description = snippet.get('description')
        published_date = snippet.get('publishedAt')
        view_count = statistics.get('viewCount')
        subscriber_count = statistics.get('subscriberCount')
        video_count = statistics.get('videoCount')

        transformed.append((channel_id, title, description, published_date, view_count, subscriber_count, video_count))

    # Create a DataFrame from the transformed list of tuples
    df = pd.DataFrame(transformed, columns=['channel_id', 'title', 'description', 'published_date', 'view_count', 'subscriber_count', 'video_count'])

    #drop duplicates after running through the loop to ensure we only have unique channel combinations in the final DataFrame, then reset index to clean up after dropping duplicates
    df = df.drop_duplicates(subset=['channel_id']).reset_index(drop=True)

    #clean datetime formatting and strip leading/trailing whitespace from string columns
    df['published_date'] = pd.to_datetime(df['published_date'], utc=True, format='ISO8601', errors='coerce')
    df['title'] = df['title'].str.strip()
    df['description'] = df['description'].str.strip()

    #channel age in days by subtracting the published date from the current date and converting the result to days
    df['published_date'] = pd.to_datetime(df['published_date'], utc=True)
    df['channel_age_days'] = (pd.Timestamp.now(tz='UTC') - df['published_date']).dt.days

    # Convert view_count, subscriber_count, and video_count to numeric values, coercing errors to NaN and then filling NaN with 0 before converting to integers
    df['view_count'] = pd.to_numeric(df['view_count'], errors='coerce').fillna(0).astype(int)
    df['subscriber_count'] = pd.to_numeric(df['subscriber_count'], errors='coerce').fillna(0).astype(int)
    df['video_count'] = pd.to_numeric(df['video_count'], errors='coerce').fillna(0).astype(int)

    # Generate new columns with comparisons between channel_age, view_count, subscriber_count, and video_count to identify trends and patterns in channel growth and engagement. 
    df['views_per_video'] = df['view_count'] / df['video_count'].replace(0, 1)
    df['views_per_sub'] = df['view_count'] / df['subscriber_count'].replace(0, 1)
    df['subs_per_video'] = df['subscriber_count'] / df['video_count'].replace(0, 1)
    df['videos_per_day'] = df['video_count'] / df['channel_age_days'].replace(0, 1)

    return df

"""----------------------------------------------------------------------------------------------------------------------------------------------------"""
def transform_youtube_video_details(video_details):
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
    df['published_date'] = pd.to_datetime(df['published_date'], utc=True, format='ISO8601', errors='coerce')

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

def transform_youtube_search_results(search_results):
    transformed = []

    if not search_results:
        return pd.DataFrame()

    # Determine the type of search results based on the 'kind' field of the first item. We assume all items in the results are of the same type (either videos or channels) for consistency.
    first_kind = search_results[0].get('id', {}).get('kind')

    if first_kind == 'youtube#video':
        search_type = 'video'
    elif first_kind == 'youtube#channel':
        search_type = 'channel'
    else:
        return pd.DataFrame()
    # Loop through each item in the search results and extract relevant fields based on the determined type (video or channel). We also enforce consistency by checking the 'kind' field of each item against the expected type.
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
    df['published_date'] = pd.to_datetime(df['published_date'], utc=True, format='ISO8601', errors='coerce')

    # Strip leading and trailing whitespace from string columns, filling NaN with empty strings first to avoid errors.
    for col in string_cols:
        df[col] = df[col].fillna('').str.strip()

    return df.reset_index(drop=True)


