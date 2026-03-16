from googleapiclient.discovery import build
import json

# Build and return Youtube API client
def build_youtube_client(api_key):
    API_client = build('youtube', 'v3', developerKey="AIzaSyD5QVio3lQ716PLv6p2AL3Jr6S74nvoNpg")
    return API_client

"""
General search query with mutable parameters. Use to find intial target sources for further data extraction.
Parameter Options: 
max_results: 0-50
order: relevance, title, viewCount, date, rating
type: video, channel, playlist
"""
def search_query(youtube, query, part="snippet", max_results="50", order="viewCount", type="video"):
    search_response = youtube.search().list(
        q=query,
        part=part,
        maxResults=max_results,
        order=order,
        type=type
    ).execute()

    return search_response['items']

"""
    Extract specified video details. Pass fields as list to select which data to return. Mutable variables for efficiency/cleanliness
    
    Available fields: 'basic', 'stats', 'content', 'all'
    Or specify individual fields: ['video_id', 'title', 'description', 'published_at', 'channel_title', 
    'channel_id', 'tags', 'view_count', 'like_count', 'dislike_count', 'comment_count', 'duration', 'definition', 'caption']
    """
def extract_video_details(youtube, video_id, fields=None):

    # Determine which parts to request from API
    if fields is None or 'all' in fields:
        part = "snippet,statistics,contentDetails"
    else:
        part = "snippet"  # Always need snippet
        if any(field in ['view_count', 'like_count', 'comment_count'] for field in fields):
            part += ",statistics"
        if any(field in ['duration', 'definition'] for field in fields):
            part += ",contentDetails"
    
    video_response = youtube.videos().list(
        part=part,
        id=video_id
    ).execute()

    if 'items' in video_response and len(video_response['items']) > 0:
        video_data = video_response['items'][0]
        
        # Define all possible fields
        all_fields = {
            'video_id': video_id,
            'title': video_data['snippet']['title'],
            'description': video_data['snippet']['description'],
            'published_at': video_data['snippet']['publishedAt'],
            'channel_title': video_data['snippet']['channelTitle'],
            'channel_id': video_data['snippet']['channelId'],
            'tags': video_data['snippet'].get('tags', []),
            'view_count': video_data.get('statistics', {}).get('viewCount', 0),
            'like_count': video_data.get('statistics', {}).get('likeCount', 0),
            'dislike_count': video_data.get('statistics', {}).get('dislikeCount', 0),
            'comment_count': video_data.get('statistics', {}).get('commentCount', 0),
            'duration': video_data.get('contentDetails', {}).get('duration'),
            'definition': video_data.get('contentDetails', {}).get('definition'),
            'caption': video_data.get('contentDetails', {}).get('caption'),
        }
        
        # Return selected fields
        if fields is None:
            return all_fields  # Return all by default
        elif fields == 'basic':
            return {k: v for k, v in all_fields.items() if k in ['video_id', 'title', 'description', 'published_at', 'channel_title', 'channel_id']}
        elif fields == 'stats':
            return {k: v for k, v in all_fields.items() if k in ['video_id', 'title','view_count', 'like_count', 'dislike_count', 'comment_count', 'duration']}
        elif isinstance(fields, list):
            return {k: v for k, v in all_fields.items() if k in fields}
        else:
            return all_fields
    else:
        return None

"""
    Get channel information with flexible field selection.

    Args:
        youtube: YouTube API client
        channel_ids: Single channel ID string or list of channel IDs
        Avaliable fields: 'basic', 'stats'
            Or list specific fields: ['title', 'subscriberCount', etc.]

    Returns:
        Raw channel data from API, filtered by selected fields
    """
possible_channel_fields = ['title', 'description', 'publishedAt', 'country', 'thumbnails', 'subscriberCount', 'videoCount', 'viewCount']
def get_channel_details(youtube, channel_ids, fields=None):
    # Handle single channel ID
    if isinstance(channel_ids, str):
        channel_ids = [channel_ids]
        single_channel = True
    else:
        single_channel = False

    # Build API request parameters
    request_params = {
        'id': ",".join(channel_ids)
    }

    if fields is None:
        # Get all data
        request_params['part'] = "snippet,statistics"
    elif fields == 'basic':
        request_params['part'] = "snippet"
        # Use API fields parameter for server-side filtering
        request_params['fields'] = "items(id,snippet(title,description,publishedAt))"
    elif fields == 'stats':
        request_params['part'] = "snippet,statistics"
        request_params['fields'] = "items(id,snippet(title),statistics(subscriberCount,videoCount,viewCount))"
    elif isinstance(fields, list):
        # Dynamically build parts and fields based on requested fields
        parts = []
        api_fields = "items("

        # Always include ID
        api_fields += "id"

        # Check which parts are needed
        snippet_fields = []
        stat_fields = []

        for field in fields:
            if field in ['title', 'description', 'publishedAt', 'published_at', 'country', 'thumbnails']:
                if 'snippet' not in parts:
                    parts.append('snippet')
                if field == 'title':
                    snippet_fields.append('title')
                elif field == 'description':
                    snippet_fields.append('description')
                elif field in ['publishedAt', 'published_at']:
                    snippet_fields.append('publishedAt')
                elif field == 'country':
                    snippet_fields.append('country')
                elif field == 'thumbnails':
                    snippet_fields.append('thumbnails')
            elif field in ['subscriberCount', 'subscriber_count', 'videoCount', 'video_count', 'viewCount', 'view_count']:
                if 'statistics' not in parts:
                    parts.append('statistics')
                if field in ['subscriberCount', 'subscriber_count']:
                    stat_fields.append('subscriberCount')
                elif field in ['videoCount', 'video_count']:
                    stat_fields.append('videoCount')
                elif field in ['viewCount', 'view_count']:
                    stat_fields.append('viewCount')

        request_params['part'] = ",".join(parts)

        # Build fields parameter
        if snippet_fields:
            api_fields += f",snippet({','.join(snippet_fields)})"
        if stat_fields:
            api_fields += f",statistics({','.join(stat_fields)})"

        api_fields += ")"
        request_params['fields'] = api_fields
    else:
        request_params['part'] = "snippet"

    response = youtube.channels().list(**request_params).execute()

    if fields is None:
        # Return raw API response
        return response['items'][0] if single_channel else response['items']

    # For filtered requests, return the already filtered data
    return response['items'][0] if single_channel else response['items']

"""
    Get comments from individual videos or a list of video IDs.
    
    Args:
        youtube: YouTube API client
        video_ids: Single video ID string or list of video ID strings
        max_results: Maximum comments per video (default: 100)
    
    Returns:
        Dictionary with video IDs as keys and comment lists as values.
        Each comment contains: text, published_at, like_count
    """
def get_video_comments(youtube, video_ids, max_results=100):
   
    # Handle single video ID
    if isinstance(video_ids, str):
        video_ids = [video_ids]
    
    all_comments = {}
    
    for video_id in video_ids:
        try:
            response = youtube.commentThreads().list(
                videoId=video_id,
                part='snippet',
                maxResults=max_results,
                order='relevance'
            ).execute()
            
            comments = []
            for item in response.get('items', []):
                comment = item['snippet']['topLevelComment']['snippet']
                comment_data = {
                    'text': comment['textDisplay'],
                    'published_at': comment['publishedAt'],
                    'like_count': comment.get('likeCount', 0)
                }
                comments.append(comment_data)
            
            all_comments[video_id] = comments
            
        except Exception as e:
            # Handle videos with disabled comments or other errors
            all_comments[video_id] = []
            print(f"Could not get comments for video {video_id}: {str(e)}")
    
    return all_comments

"""
    Get comments on videos from a specific channel.
    
    Args:
        youtube: YouTube API client
        channel_id: Channel ID string
        max_results: Maximum comments to return (default: 100)
    
    Returns:
        List of comments on the channel's videos.
        Each comment contains: text, published_at, like_count, video_id, reply_count
    """
def get_channel_comments(youtube, channel_id, max_results=100):

    try:
        response = youtube.commentThreads().list(
            allThreadsRelatedToChannelId=channel_id,
            part='snippet',
            maxResults=max_results,
            order='time'
        ).execute()
        
        comments = []
        for item in response.get('items', []):
            comment = item['snippet']['topLevelComment']['snippet']
            video_id = item['snippet']['videoId']
            
            comment_data = {
                'text': comment['textDisplay'],
                'published_at': comment['publishedAt'],
                'like_count': comment.get('likeCount', 0),
                'video_id': video_id,
                'reply_count': item['snippet'].get('totalReplyCount', 0)
            }
            comments.append(comment_data)
        
        return comments
        
    except Exception as e:
        print(f"Could not get comments for channel {channel_id}: {str(e)}")
        return []