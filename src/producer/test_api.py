import os
from googleapiclient.discovery import build
from dotenv import load_dotenv

# Load API key from .env
load_dotenv("config/.env")
API_KEY = os.getenv("YOUTUBE_API_KEY")

if not API_KEY:
    raise ValueError("No API key found. Check config/.env")

# Build YouTube client
youtube = build("youtube", "v3", developerKey=API_KEY)

print("Testing YouTube API...")

# Search for a popular tech video
search_response = youtube.search().list(
    q="vaatividya",
    part="id,snippet",
    type="video",
    order="viewCount",
    maxResults=1
).execute()

if not search_response.get("items"):
    print("No videos found!")
    exit()

video = search_response["items"][0]
video_id = video["id"]["videoId"]
video_title = video["snippet"]["title"]

print(f"\nFound video: {video_title}")
print(f"Video ID: {video_id}")

# Get comments
comments_response = youtube.commentThreads().list(
    part="snippet",
    videoId=video_id,
    maxResults=5,
    order="relevance"
).execute()

print(f"\n--- Top 5 Comments ---")
for item in comments_response.get("items", []):
    snippet = item["snippet"]["topLevelComment"]["snippet"]
    print(f"\nAuthor: {snippet['authorDisplayName']}")
    print(f"Text: {snippet['textDisplay'][:100]}...")
    print(f"Likes: {snippet.get('likeCount', 0)}")
    print("-" * 50)

print("\nAPI test successful!")