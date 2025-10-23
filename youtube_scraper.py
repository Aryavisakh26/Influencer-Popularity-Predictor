# ------------------------------------------------------------
# YouTube Influencer Data Collector (Extended Version + Comments + Age)
# ------------------------------------------------------------
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import time
import os
from datetime import datetime

# ======= 1. API KEYS ========================================================
API_KEYS = [
    "AIzaSyAtLl0N0Y62WIlf_dikqB0y9Nbw5H-QhUE",
    "AIzaSyBFPfatLBivZaPQ9L9Np6kRjMoZbpdKVKA",
    "AIzaSyDtryeoavb5SA995UAu_ee1lLwC5vsOenA",
    "AIzaSyDGPX4-eqJdYfBDJoZmU4ddRI-ycvHl51Q",
    "AIzaSyD0iwc4w69ZGZsBa9kbhSaOd4Vv3jiVn0s",
]
current_key_index = 0


def get_youtube_service():
    global current_key_index
    api_key = API_KEYS[current_key_index]
    return build("youtube", "v3", developerKey=api_key)


def rotate_key():
    global current_key_index
    current_key_index += 1
    if current_key_index >= len(API_KEYS):
        print("⚠️ All API keys exhausted. Waiting 1 hour before retrying...")
        time.sleep(3600)
        current_key_index = 0
    print(f"🔑 Switching to API key #{current_key_index + 1}")


# ======= 2. SEARCH KEYWORDS =================================================
search_keywords = [
    "Indian vlogger",
    "Indian tech YouTuber",
    "Indian beauty channel",
    "Indian food YouTuber",
    "Indian travel vlogger",
    "Indian fitness channel",
    "Indian gamer",
    "Indian education channel",
    "Indian music channel",
    "Indian comedy YouTuber",
    "Indian finance channel",
    "Indian review channel",
]

MAX_CHANNELS_PER_KEYWORD = 500

# ======= 3. LOAD EXISTING DATA IF ANY ======================================
partial_file = "youtube_influencers_partial.csv"
if os.path.exists(partial_file):
    df_existing = pd.read_csv(partial_file)
    collected_ids = set(df_existing["Channel_ID"].tolist())
    data = df_existing.to_dict("records")
    keyword_counts = df_existing.groupby("Keyword")["Channel_ID"].count().to_dict()
    print(
        f"🔄 Resuming from saved file. Already collected {len(collected_ids)} channels."
    )
else:
    collected_ids = set()
    data = []
    keyword_counts = {}


# ======= 4. GET CHANNEL IDS =================================================
def get_channel_ids(keyword, max_pages=10):
    already_collected = keyword_counts.get(keyword, 0)
    if already_collected >= MAX_CHANNELS_PER_KEYWORD:
        print(
            f"✅ Skipping '{keyword}' (already collected {already_collected} channels)."
        )
        return []

    channel_ids = []
    next_page_token = None
    pages_fetched = 0

    while pages_fetched < max_pages:
        try:
            youtube = get_youtube_service()
            request = youtube.search().list(
                q=keyword,
                type="channel",
                part="snippet",
                maxResults=50,
                regionCode="IN",
                pageToken=next_page_token,
            )
            response = request.execute()

            for item in response.get("items", []):
                cid = item["snippet"]["channelId"]
                if (
                    cid not in collected_ids
                    and len(channel_ids) + already_collected < MAX_CHANNELS_PER_KEYWORD
                ):
                    channel_ids.append(cid)
                if len(channel_ids) + already_collected >= MAX_CHANNELS_PER_KEYWORD:
                    break

            next_page_token = response.get("nextPageToken")
            if (
                not next_page_token
                or len(channel_ids) + already_collected >= MAX_CHANNELS_PER_KEYWORD
            ):
                break

            pages_fetched += 1
            time.sleep(1)

        except HttpError as e:
            print(f"HttpError: {e}")
            rotate_key()
            continue

    print(f"Collected {len(channel_ids)} new channels for '{keyword}'")
    return channel_ids


# ======= 5. COLLECT CHANNEL IDS FOR ALL KEYWORDS ============================
all_channel_ids = []
for keyword in search_keywords:
    ids = get_channel_ids(keyword, max_pages=10)
    for cid in ids:
        all_channel_ids.append((cid, keyword))
    time.sleep(2)


# ======= Helper: Get Comment Count from Recent Videos =======================
def get_comment_count(upload_playlist_id, max_videos=10):
    """Fetch total comments from the last few uploaded videos."""
    try:
        youtube = get_youtube_service()
        request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=upload_playlist_id,
            maxResults=max_videos,
        )
        response = request.execute()

        video_ids = [
            item["contentDetails"]["videoId"] for item in response.get("items", [])
        ]
        if not video_ids:
            return 0

        total_comments = 0
        for i in range(0, len(video_ids), 50):
            ids_chunk = video_ids[i : i + 50]
            vid_request = youtube.videos().list(
                part="statistics",
                id=",".join(ids_chunk),
            )
            vid_response = vid_request.execute()
            for vid in vid_response.get("items", []):
                stats = vid.get("statistics", {})
                total_comments += int(stats.get("commentCount", 0))

        return total_comments

    except Exception as e:
        print(f"⚠️ Error getting comment count: {e}")
        return 0


# ======= 6. GET CHANNEL DETAILS (EXTENDED) ==================================
for i in range(0, len(all_channel_ids), 50):
    batch = all_channel_ids[i : i + 50]
    cids = [x[0] for x in batch]
    keywords_batch = [x[1] for x in batch]
    try:
        youtube = get_youtube_service()
        request = youtube.channels().list(
            part="snippet,statistics,brandingSettings,contentDetails,topicDetails",
            id=",".join(cids),
        )
        response = request.execute()

        for idx, item in enumerate(response.get("items", [])):
            cid = item["id"]
            keyword = keywords_batch[idx]

            stats = item.get("statistics", {})
            snippet = item.get("snippet", {})
            branding = item.get("brandingSettings", {}).get("channel", {})
            content = item.get("contentDetails", {})
            topics = item.get("topicDetails", {}).get("topicCategories", [])

            # Account age calculation
            published_at = snippet.get("publishedAt", "")
            account_age_years = 0
            if published_at:
                try:
                    created_date = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ")
                    account_age_years = round(
                        (datetime.now() - created_date).days / 365, 2
                    )
                except:
                    account_age_years = 0

            # Comment count from recent videos
            uploads_id = content.get("relatedPlaylists", {}).get("uploads", "")
            comment_count = (
                get_comment_count(uploads_id, max_videos=10) if uploads_id else 0
            )

            if cid not in collected_ids:
                data.append(
                    {
                        "Channel_ID": cid,
                        "Keyword": keyword,
                        "Channel_Name": snippet.get("title", ""),
                        "Subscribers": int(stats.get("subscriberCount", 0)),
                        "Total_Views": int(stats.get("viewCount", 0)),
                        "Total_Videos": int(stats.get("videoCount", 0)),
                        "Comment_Count": comment_count,
                        "Account_Age_Years": account_age_years,
                        "Description": snippet.get("description", ""),
                        "PublishedAt": published_at,
                        "Country": snippet.get("country", "IN"),
                        "Custom_URL": branding.get("customUrl", ""),
                        "Keywords": branding.get("keywords", ""),
                        "Profile_Country": branding.get("country", ""),
                        "Playlist_ID": uploads_id,
                        "Topic_Categories": ", ".join(topics) if topics else "N/A",
                        "Banner_Image": branding.get("image", {}).get(
                            "bannerExternalUrl", ""
                        ),
                        "Default_Language": snippet.get("defaultLanguage", "N/A"),
                    }
                )
                collected_ids.add(cid)
                keyword_counts[keyword] = keyword_counts.get(keyword, 0) + 1

        # Save progress
        pd.DataFrame(data).to_csv(partial_file, index=False)
        print(f"💾 Partial data saved. Total collected: {len(collected_ids)}")
        time.sleep(2)

    except HttpError as e:
        print(f"HttpError while fetching details: {e}")
        rotate_key()
        continue


# ======= 7. FINAL DATAFRAME & POPULARITY LABEL ==============================
df = pd.DataFrame(data)
df["Popularity_Label"] = pd.cut(
    df["Subscribers"],
    bins=[0, 100000, 500000, 1000000000],
    labels=["Low", "Medium", "High"],
)

# ======= 8. SAVE FINAL CSV ==================================================
df.to_csv("youtube_influencers_extended.csv", index=False)
print(f"✅ Data saved to youtube_influencers_extended.csv | Total channels: {len(df)}")
