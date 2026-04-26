from googleapiclient.discovery import build
from dotenv import load_dotenv
import os

load_dotenv()
API_KEY = os.getenv("YOUTUBE_API")  # Paste your key here

youtube = build("youtube", "v3", developerKey=API_KEY)

# Test: Search for a popular video
request = youtube.search().list(
    q="mr beast",
    part="id,snippet",
    type="video",
    order="relevance",
    maxResults=1
)
response = request.execute()

video_id = response["items"][0]["id"]["videoId"]
video_title = response["items"][0]["snippet"]["title"]
print(f"Found video: {video_title}")
print(f"Video ID: {video_id}")

# Test: Get comments for that video
comments_request = youtube.commentThreads().list(
    part="snippet",
    videoId=video_id,
    maxResults=5,
    order="time"
)
comments_response = comments_request.execute()

print(f"\n--- Top 5 Comments ---")
for item in comments_response.get("items", []):
    snippet = item["snippet"]["topLevelComment"]["snippet"]
    print(f"Author: {snippet['authorDisplayName']}")
    print(f"Text: {snippet['textDisplay'][:100]}...")
    print(f"Likes: {snippet.get('likeCount', 0)}")
    print("-" * 40)