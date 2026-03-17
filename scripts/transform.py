import pandas as pd
"""
transform_channel_details()
transform_video_comments()
transform_channel_comments()
"""
def transform_channel_comments(channel_comments):
    transformed = []
    #flatten channel comments into a dictionary for easier analysis/saving to CSV
    for channel_id, comments in channel_comments.items():
        for comment in comments:
            comment_id = comment['id']
            author_display_name = comment['snippet']['topLevelComment']['snippet']['authorDisplayName']
            author_channel_id = comment['snippet']['topLevelComment']['snippet']['authorChannelId']['value']
            text_display = comment['snippet']['topLevelComment']['snippet']['textDisplay']
            like_count = comment['snippet']['topLevelComment']['snippet']['likeCount']
            published_at = comment['snippet']['topLevelComment']['snippet']['publishedAt']
            transformed.append((channel_id, comment_id, author_display_name, author_channel_id, text_display, like_count, published_at))
    
    # Create a DataFrame from the transformed list of tuples
    df = pd.DataFrame(transformed, columns=['channel_id', 'comment_id', 'author_display_name', 'author_channel_id', 'text_display', 'like_count', 'published_at'])
    
    return df


def transform_video_comments(video_comments):
    transformed = []
    #flatten video comments into a dictionary for easier analysis/saving to CSV
    for video_id, comments in video_comments.items():
        for comment in comments:
            comment_id = comment['id']
            author_display_name = comment['snippet']['topLevelComment']['snippet']['authorDisplayName']
            author_channel_id = comment['snippet']['topLevelComment']['snippet']['authorChannelId']['value']
            text_display = comment['snippet']['topLevelComment']['snippet']['textDisplay']
            like_count = comment['snippet']['topLevelComment']['snippet']['likeCount']
            published_at = comment['snippet']['topLevelComment']['snippet']['publishedAt']
            transformed.append((video_id, comment_id, author_display_name, author_channel_id, text_display, like_count, published_at))

    # Create a DataFrame from the transformed list of tuples
    df = pd.DataFrame(transformed, columns=['video_id', 'comment_id', 'author_display_name', 'author_channel_id', 'text_display', 'like_count', 'published_at'])
    
    return df

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

def transform_video_details(video_details):
  # Check if video_details is empty or None
    if not video_details:
      return None
    
    transformed = []
    #flatten video details into a dictionary for easier analysis/saving to CSV
    for item in video_details:
        video_id = item.get('id')
        published_at = item['snippet'].get('publishedAt')
        channel_id = item['snippet'].get('channelId')
        title = item['snippet'].get('title')
        description = item['snippet'].get('description')
        tags = item['snippet'].get('tags', [])
        channel_title = item['snippet'].get('channelTitle')
        view_count = item['statistics'].get('viewCount')
        like_count = item['statistics'].get('likeCount')
        comment_count = item['statistics'].get('commentCount')
        duration = item['contentDetails'].get('duration')
        transformed.append((video_id, published_at, channel_id, title, description, tags, channel_title, view_count, like_count, comment_count, duration))
      
    # Create a DataFrame from the transformed list of tuples
    df = pd.DataFrame(transformed, columns=['video_id', 'published_at', 'channel_id', 'title', 'description', 'tags', 'channel_title', 'view_count', 'like_count', 'comment_count', 'duration'])
    
    return df


def transform_search_results(search_results):
    transformed = []
    # flatten search results into a DataFrame for easier analysis/saving to CSV
    for item in search_results:
        video_id = item['id'].get('videoId')
        channel_id = item['snippet'].get('channelId')
        title = item['snippet'].get('title')
        description = item['snippet'].get('description')
        published_at = item['snippet'].get('publishedAt')
        channel_title = item['snippet'].get('channelTitle')
        transformed.append((channel_id, channel_title, video_id, title, description, published_at))
    
    # Create a DataFrame from the transformed list of tuples
    df = pd.DataFrame(transformed, columns=['channel_id', 'channel_title', 'video_id', 'title', 'description', 'published_at'])
    
    return df


