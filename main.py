import os
from scripts.extract import build_youtube_client, search_query, get_video_details, get_channel_details, get_video_comments

api_key = os.getenv("YOUTUBE_API_KEY")

youtube = build_youtube_client(api_key)


