import pandas as pd
"""
transform_video_details()
{
  "kind": "youtube#video",
  "etag": "abc123",
  "id": "X1Y2Z3",

  "snippet": {
    "publishedAt": "2024-01-05T12:00:00Z",
    "channelId": "UC123456",
    "title": "10 AI Tools You Should Be Using",
    "description": "These AI tools will save you hours...",
    "tags": [
      "AI",
      "machine learning",
      "technology"
    ],
    "categoryId": "28",
    "channelTitle": "Tech Explained"
  },

  "statistics": {
    "viewCount": "2039483",
    "likeCount": "104293",
    "favoriteCount": "0",
    "commentCount": "4021"
  },

  "contentDetails": {
    "duration": "PT12M45S",
    "dimension": "2d",
    "definition": "hd",
    "caption": "false"
  }
}
transform_channel_details()
transform_video_comments()
transform_channel_comments()
"""
# flatten search results into a DataFrame for easier analysis/saving to CSV
def transform_search_results(search_results):
    transformed = []
    for item in search_results:
        video_id = item['id'].get('videoId')
        channel_id = item['snippet'].get('channelId')
        title = item['snippet'].get('title')
        description = item['snippet'].get('description')
        published_at = item['snippet'].get('publishedAt')
        channel_title = item['snippet'].get('channelTitle')
        transformed.append((channel_id, channel_title, video_id, title, description, published_at))
    df = pd.DataFrame(transformed, columns=['channel_id', 'channel_title', 'video_id', 'title', 'description', 'published_at'])
    return df

def extract_video_ids(df):
    video_ids = df['video_id'].to_list()

    return video_ids

def extract_channel_ids(df):
    channel_ids = df['channel_id'].to_list()

    return channel_ids


