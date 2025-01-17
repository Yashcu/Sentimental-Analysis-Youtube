import logging
from googleapiclient.discovery import build
import pandas as pd
import re

# Initialize logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Replace with your actual API key
YOUTUBE_API_KEY = "AIzaSyBm7KN2TMpov1FvkUv_f8oSs3tG6Vu9Vfc"


def get_video_id(video_url):
    """Extracts the YouTube video ID from a URL."""
    try:
        match = re.search(r'(?:youtu\.be\/|youtube\.com\/(?:embed\/|v\/|watch\?v=|watch\?.+&v=))([\w-]{11})', video_url)
        if match:
            return match.group(1)
        else:
            raise ValueError("Invalid YouTube URL.")
    except Exception as e:
        logging.error(f"Error extracting video ID: {e}")
        return None


def fetch_replies(parent_id, youtube):
    """Fetches replies for a given parent comment ID."""
    replies = []
    next_page_token = None
    try:
        while True:
            request = youtube.comments().list(
                part="snippet",
                parentId=parent_id,
                maxResults=100,  # Maximum allowed
                pageToken=next_page_token
            )
            response = request.execute()
            for reply in response.get('items', []):
                snippet = reply['snippet']
                replies.append({
                    'comment_id': reply['id'],
                    'text': snippet.get('textDisplay', ''),
                    'author_display_name': snippet.get('authorDisplayName', ''),
                    'author_channel_url': snippet.get('authorChannelUrl', ''),
                    'published_at': snippet.get('publishedAt', ''),
                    'updated_at': snippet.get('updatedAt', ''),
                    'parent_id': parent_id
                })

            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
        return replies
    except Exception as e:
        logging.error(f"Error fetching replies for {parent_id}: {e}")
        return []


def get_comments_and_replies(video_id, api_key, max_results=500):
    """Fetches all comments and their replies for a given YouTube video ID."""
    youtube = build('youtube', 'v3', developerKey=api_key, cache_discovery=False)
    all_comments = []
    next_page_token = None

    try:
        while True:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=100,
                pageToken=next_page_token
            )
            response = request.execute()
            for item in response.get('items', []):
                top_comment = item['snippet']['topLevelComment']['snippet']
                parent_id = item['id']
                all_comments.append({
                    'comment_id': parent_id,
                    'text': top_comment.get('textDisplay', ''),
                    'author_display_name': top_comment.get('authorDisplayName', ''),
                    'author_channel_url': top_comment.get('authorChannelUrl', ''),
                    'published_at': top_comment.get('publishedAt', ''),
                    'updated_at': top_comment.get('updatedAt', ''),
                    'parent_id': None  # Top-level comments have no parent
                })

                # Fetch replies for the top-level comment
                replies = fetch_replies(parent_id, youtube)
                all_comments.extend(replies)

            next_page_token = response.get('nextPageToken')
            if not next_page_token or len(all_comments) >= max_results:
                break

        logging.info(f"Fetched {len(all_comments)} comments and replies for video ID: {video_id}")
        return all_comments
    except Exception as e:
        logging.error(f"Error fetching comments: {e}")
        return []


def clean_text(text):
    """Cleans text by removing URLs, mentions, and special characters."""
    text = re.sub(r'http\S+|www\S+', '', text)  # Remove URLs
    text = re.sub(r'@\w+', '', text)  # Remove mentions
    text = re.sub(r'[^a-zA-Z0-9\s.,?!;\'\"]', '', text)  # Remove special characters
    return ' '.join(text.lower().split())  # Convert to lowercase and strip whitespace


def refine_comments(comments):
    """Refines and cleans the comments."""
    if not comments:
        return pd.DataFrame()

    df = pd.DataFrame(comments)
    df['text'] = df['text'].apply(clean_text)
    return df


def fetch_and_process_comments(video_url, api_key, max_results=500):
    """Fetches and processes all comments and their replies from a YouTube URL."""
    video_id = get_video_id(video_url)
    if not video_id:
        return pd.DataFrame()

    comments = get_comments_and_replies(video_id, api_key, max_results)
    if comments:
        refined_comments_df = refine_comments(comments)
        return refined_comments_df
    else:
        return pd.DataFrame()


if __name__ == '__main__':
    video_url = "https://www.youtube.com/watch?v=aBvj7W9SX4M"  # Replace with your target video URL
    api_key = YOUTUBE_API_KEY  # Replace with your API Key

    # Fetch and process comments
    comments_df = fetch_and_process_comments(video_url, api_key, max_results=1000)

    if not comments_df.empty:
        logging.info("Processed comments and replies:")
        print(comments_df[['comment_id', 'parent_id', 'text']])
        comments_df.to_csv("comments_with_replies.csv", encoding="utf-8", index=False)
        logging.info("Comments and replies saved in comments_with_replies.csv")
    else:
        logging.warning("No comments were processed for the given video URL.")
