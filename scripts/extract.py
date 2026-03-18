from googleapiclient.discovery import build
"""----------------------------------------------------------------------------------------------------------------------------------------------------"""
# Build and return Youtube API client
def build_youtube_client(api_key):
    api_client = build('youtube', 'v3', developerKey=api_key)
    return api_client

"""----------------------------------------------------------------------------------------------------------------------------------------------------"""
# Search for default = videos based on a query and return the results
def youtube_search_query(youtube, query, part="snippet", max_results=50, order="viewCount", resource_type="video"):
    page_counter = 0
    max_pages = 4  # Limit to 4 pages of results (200 videos) to avoid hitting API limits
    all_items = []
    next_page_token = None
    
    # Loop through pages of results until we reach the max page limit or run out of results
    while page_counter < max_pages:
        search_response = youtube.search().list(
            q=query,
            part=part,
            maxResults=max_results,
            order=order,
            type=resource_type,
            pageToken = next_page_token           
        ).execute()
        all_items.extend(search_response.get('items', []))
        page_counter += 1
        next_page_token = search_response.get('nextPageToken')
        if not next_page_token: # If there's no next page token, we've reached the end of the results
            break

    return all_items

"""----------------------------------------------------------------------------------------------------------------------------------------------------"""
# batched call for video details to handle video ID lists longer than 50 (API limit)
def get_youtube_video_details(youtube, video_ids):
    if isinstance(video_ids, str):
        video_ids = [video_ids]

    all_video_details = []
    for i in range(0, len(video_ids), 50):  # Process in batches of 50
        batch_ids = video_ids[i:i + 50]
        response = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            id=','.join(batch_ids) 
        ).execute()
        all_video_details.extend(response.get('items', []))

    return all_video_details

"""----------------------------------------------------------------------------------------------------------------------------------------------------"""
# batched call for channel details to handle channel ID lists longer than 50 (API limit)
def get_youtube_channel_details(youtube, channel_ids):
    if isinstance(channel_ids, str):
        channel_ids = [channel_ids]

    all_channel_details = []
    for i in range(0, len(channel_ids), 50):  # Process in batches of 50
        batch_ids = channel_ids[i:i + 50]
        response = youtube.channels().list(
            part='snippet,statistics',
            id=','.join(batch_ids)
        ).execute()
        all_channel_details.extend(response.get('items', []))

    return all_channel_details

"""----------------------------------------------------------------------------------------------------------------------------------------------------"""
# Get video top level comments for a list of video IDs
def get_youtube_video_comments(youtube, video_ids, max_results=100):

    if isinstance(video_ids, str):
        video_ids = [video_ids]

    all_raw_items = []  # This will hold all comments across all videos

    for video_id in video_ids:
        try:
            next_page_token = None

            while True:  # Loop to handle pagination for comments
                response = youtube.commentThreads().list(
                    videoId=video_id,
                    part="snippet",
                    maxResults=max_results,
                    order="relevance",
                    pageToken = next_page_token
                ).execute()

                all_raw_items.extend(response.get("items", [])) # Add the current page of comments to the video-specific list
                next_page_token = response.get("nextPageToken")
                if not next_page_token: 
                    break
            
        except Exception as e:
            # We print the error but keep going to the next video
            print(f"Skipping video {video_id} due to error: {e}")
            continue 

    return all_raw_items # Return the full bucket AFTER the loop

"""----------------------------------------------------------------------------------------------------------------------------------------------------"""
# Get comment threads related to a channel
def get_youtube_channel_comments(youtube, channel_ids, max_results=100):
    if isinstance(channel_ids, str):
        channel_ids = [channel_ids]

    all_raw_items = []  # This will hold all comments across all channels

    for channel_id in channel_ids:
        try:
            next_page_token = None
            while True:
                response = youtube.commentThreads().list(
                    allThreadsRelatedToChannelId=channel_id,
                    part="snippet",
                    maxResults=max_results,
                    order="time",
                    pageToken=next_page_token
                ).execute()

                all_raw_items.extend(response.get("items", []))
                next_page_token = response.get("nextPageToken")

                if not next_page_token:
                    break

        except Exception as e:
            print(f"Could not get comments for channel {channel_id}: {e}")
            continue

    return all_raw_items