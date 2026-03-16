from googleapiclient.discovery import build

# Build and return Youtube API client
def build_youtube_client(api_key):
    api_client = build('youtube', 'v3', developerKey=api_key)
    return api_client


def search_query(youtube, query, part="snippet", max_results=50, order="viewCount", resource_type="video"):
    search_response = youtube.search().list(
        q=query,
        part=part,
        maxResults=max_results,
        order=order,
        type=resource_type
    ).execute()

    return search_response.get('items', [])


def get_video_details(youtube, video_id):

    video_response = youtube.videos().list(
        part="snippet,statistics,contentDetails",
        id=video_id
    ).execute()

    items = video_response.get('items', [])
    return items[0] if items else None


def get_channel_details(youtube, channel_ids):
    # Handle single channel ID
    if isinstance(channel_ids, str):
        channel_ids = [channel_ids]

    response = youtube.channels().list(
        part='snippet,statistics',
        id=','.join(channel_ids)
    ).execute()

    return response.get('items', [])


def get_video_comments(youtube, video_ids, max_results=100):
    """Get top-level comments for one or more videos."""
    if isinstance(video_ids, str):
        video_ids = [video_ids]

    all_comments = {}

    for video_id in video_ids:
        try:
            response = youtube.commentThreads().list(
                videoId=video_id,
                part="snippet",
                maxResults=max_results,
                order="relevance"
            ).execute()

            comments = []
            for item in response.get("items", []):
                snippet = item.get("snippet", {})
                top_comment = snippet.get("topLevelComment", {}).get("snippet", {})

                comments.append({
                    "text": top_comment.get("textDisplay", ""),
                    "published_at": top_comment.get("publishedAt", ""),
                    "like_count": top_comment.get("likeCount", 0)
                })

            all_comments[video_id] = comments

        except Exception as e:
            all_comments[video_id] = []
            print(f"Could not get comments for video {video_id}: {e}")

    return all_comments


def get_channel_comments(youtube, channel_id, max_results=100):
    """Get comment threads related to a channel."""
    try:
        response = youtube.commentThreads().list(
            allThreadsRelatedToChannelId=channel_id,
            part="snippet",
            maxResults=max_results,
            order="time"
        ).execute()

        comments = []
        for item in response.get("items", []):
            snippet = item.get("snippet", {})
            top_comment = snippet.get("topLevelComment", {}).get("snippet", {})

            comments.append({
                "text": top_comment.get("textDisplay", ""),
                "published_at": top_comment.get("publishedAt", ""),
                "like_count": top_comment.get("likeCount", 0),
                "video_id": snippet.get("videoId"),
                "reply_count": snippet.get("totalReplyCount", 0)
            })

        return comments

    except Exception as e:
        print(f"Could not get comments for channel {channel_id}: {e}")
        return []