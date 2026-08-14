"""
YouTube Data Lakehouse and Analysis — enhanced UI/UX + analysis build.

Same Extract -> MongoDB (data lake) -> MySQL (warehouse) -> Analyze pipeline
as app.py, restructured for usability:
  - tabbed layout instead of one long scrolling page
  - live connection-status feedback instead of console-only prints
  - idempotent migration (INSERT IGNORE + per-row error isolation) so
    re-running is always safe and reports exactly what changed
  - cached DB connections/queries instead of reconnecting on every call
  - a filterable analysis dashboard with several new analyses beyond the
    original 10 canned queries, charted per the project's dataviz method
    (single validated categorical palette, color assigned by job:
    identity vs magnitude vs polarity, never eyeballed)
  - CSV export on every result table
"""

import re
from collections import Counter
from datetime import datetime
from decimal import Decimal

import altair as alt
import pandas as pd
import streamlit as st
from googleapiclient.discovery import build
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import mysql.connector as sql

st.set_page_config(page_title="YouTube Data Lakehouse", page_icon="📺", layout="wide")

# =====================================================================
# Palette — validated categorical hues (see dataviz skill / palette.md).
# Marks use these; chart chrome (background/gridlines/text) is left to
# Streamlit's own theme="streamlit" adapter so it matches light/dark
# automatically instead of fighting it.
# =====================================================================
PALETTE_LIGHT = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100",
                  "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
PALETTE_DARK = ["#3987e5", "#d95926", "#199e70", "#c98500",
                 "#d55181", "#008300", "#9085e9", "#e66767"]


def get_palette():
    base = st.get_option("theme.base")
    return PALETTE_DARK if base == "dark" else PALETTE_LIGHT


def series_color(idx=0):
    return get_palette()[idx % len(get_palette())]


def compact_number(n):
    """1,284 / 12.9K / 4.2M / 1.1B — for stat tiles, per the dataviz figure spec."""
    try:
        n = float(n)
    except (TypeError, ValueError):
        return "0"
    sign = "-" if n < 0 else ""
    n = abs(n)
    if n >= 1e9:
        return f"{sign}{n / 1e9:.1f}B"
    if n >= 1e6:
        return f"{sign}{n / 1e6:.1f}M"
    if n >= 1e3:
        return f"{sign}{n / 1e3:.1f}K"
    return f"{sign}{n:,.0f}"


def download_button(df, filename, label="⬇️ Download CSV", key=None):
    if df is None or df.empty:
        return
    st.download_button(label, df.to_csv(index=False).encode("utf-8"),
                        file_name=filename, mime="text/csv", key=key)


# =====================================================================
# Chart builders — one hue per job. Ranking/magnitude charts below are
# a single metric across nominal categories (channel, video) -> ONE
# series, slot-1 hue, no legend needed (color-formula.md: swapping the
# category order wouldn't change meaning, so it's nominal categorical,
# same slot for every bar). The only place multiple distinct series
# appear is the upload-trend line chart, which gets the full ordered
# palette + a legend.
# =====================================================================
def bar_chart(df, cat, val, title="", cat_title=None, val_title=None, horizontal=True):
    if df is None or df.empty:
        return None
    color = series_color(0)
    mark = alt.Chart(df).mark_bar(cornerRadiusEnd=4, color=color)
    tooltip = [alt.Tooltip(f"{cat}:N", title=cat_title or cat),
               alt.Tooltip(f"{val}:Q", title=val_title or val, format=",")]
    if horizontal:
        chart = mark.encode(
            y=alt.Y(f"{cat}:N", sort="-x", title=cat_title or cat),
            x=alt.X(f"{val}:Q", title=val_title or val, axis=alt.Axis(format=",")),
            tooltip=tooltip,
        )
    else:
        chart = mark.encode(
            x=alt.X(f"{cat}:N", sort="-y", title=cat_title or cat),
            y=alt.Y(f"{val}:Q", title=val_title or val, axis=alt.Axis(format=",")),
            tooltip=tooltip,
        )
    return chart.properties(title=title, height=max(220, 24 * min(len(df), 15))).configure_view(strokeWidth=0)


def scatter_chart(df, x, y, label, title="", x_title=None, y_title=None):
    if df is None or df.empty:
        return None
    color = series_color(0)
    chart = (
        alt.Chart(df)
        .mark_point(filled=True, size=90, color=color, stroke="white", strokeWidth=2)
        .encode(
            x=alt.X(f"{x}:Q", title=x_title or x, axis=alt.Axis(format=",")),
            y=alt.Y(f"{y}:Q", title=y_title or y, axis=alt.Axis(format=",")),
            tooltip=[alt.Tooltip(f"{label}:N", title="Channel"),
                     alt.Tooltip(f"{x}:Q", title=x_title or x, format=","),
                     alt.Tooltip(f"{y}:Q", title=y_title or y, format=",")],
        )
        .properties(title=title, height=380)
        .configure_view(strokeWidth=0)
    )
    return chart


def line_chart_multi(df, x, y, series_col, title="", y_title=None):
    if df is None or df.empty:
        return None
    series = sorted(df[series_col].unique().tolist())
    palette = get_palette()
    color_scale = alt.Scale(domain=series, range=palette[: len(series)])
    chart = (
        alt.Chart(df)
        .mark_line(strokeWidth=2, point=alt.OverlayMarkDef(size=70, filled=True))
        .encode(
            x=alt.X(f"{x}:O", title=x),
            y=alt.Y(f"{y}:Q", title=y_title or y, axis=alt.Axis(format=",")),
            color=alt.Color(f"{series_col}:N", scale=color_scale, legend=alt.Legend(title="Channel")),
            tooltip=[alt.Tooltip(f"{series_col}:N", title="Channel"),
                     alt.Tooltip(f"{x}:O", title="Year"),
                     alt.Tooltip(f"{y}:Q", title=y_title or y)],
        )
        .properties(title=title, height=380)
        .configure_view(strokeWidth=0)
    )
    return chart


def histogram_chart(df, col, title="", x_title=None):
    if df is None or df.empty:
        return None
    color = series_color(0)
    chart = (
        alt.Chart(df)
        .mark_bar(color=color)
        .encode(
            x=alt.X(f"{col}:Q", bin=alt.Bin(maxbins=20), title=x_title or col, axis=alt.Axis(format=",")),
            y=alt.Y("count():Q", title="Number of videos", axis=alt.Axis(format=",")),
            tooltip=[alt.Tooltip(f"{col}:Q", bin=alt.Bin(maxbins=20), title=x_title or col),
                     alt.Tooltip("count():Q", title="Videos")],
        )
        .properties(title=title, height=320)
        .configure_view(strokeWidth=0)
    )
    return chart


def ordinal_bar_chart(df, x, y, title="", x_title=None, y_title=None):
    """Vertical bar chart with a chronological/ordinal x-axis (e.g. year) —
    unlike bar_chart(), does not sort bars by value."""
    if df is None or df.empty:
        return None
    color = series_color(0)
    chart = (
        alt.Chart(df)
        .mark_bar(cornerRadiusEnd=4, color=color)
        .encode(
            x=alt.X(f"{x}:O", title=x_title or x),
            y=alt.Y(f"{y}:Q", title=y_title or y, axis=alt.Axis(format=",")),
            tooltip=[alt.Tooltip(f"{x}:O", title=x_title or x),
                     alt.Tooltip(f"{y}:Q", title=y_title or y, format=",")],
        )
        .properties(title=title, height=320)
        .configure_view(strokeWidth=0)
    )
    return chart


# =====================================================================
# Secrets / connection handling — cached, with graceful in-UI failure
# instead of a raw traceback crashing the whole app on load.
# =====================================================================
REQUIRED_SECRETS = ["MONGODB_URI", "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_HOST", "MYSQL_DATABASE"]


def missing_secrets():
    try:
        return [k for k in REQUIRED_SECRETS if k not in st.secrets]
    except Exception:
        return REQUIRED_SECRETS


@st.cache_resource(show_spinner=False)
def get_mongo_client():
    try:
        client = MongoClient(st.secrets["MONGODB_URI"], server_api=ServerApi("1"))
        client.admin.command("ping")
        return client, None
    except Exception as e:
        return None, str(e)


@st.cache_resource(show_spinner=False)
def get_mysql_connection():
    # NOTE: a single connection is shared across all sessions in this process
    # (st.cache_resource is process-wide). Fine for a personal/demo deployment;
    # a multi-user production app would want a pool instead.
    try:
        conn = sql.connect(
            user=st.secrets["MYSQL_USER"],
            password=st.secrets["MYSQL_PASSWORD"],
            host=st.secrets["MYSQL_HOST"],
            database=st.secrets["MYSQL_DATABASE"],
            use_pure=True,  # avoid the C-extension / pyarrow native crash (see app.py history)
        )
        return conn, None
    except Exception as e:
        return None, str(e)


def run_query(sql_text, params=None):
    conn, err = get_mysql_connection()
    if conn is None:
        return pd.DataFrame(), err
    try:
        cur = conn.cursor()
        cur.execute(sql_text, params or ())
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        cur.close()
        df = pd.DataFrame(rows, columns=cols)
        # MySQL returns DECIMAL for arithmetic expressions (AVG, division, etc.);
        # pandas' Decimal dtype doesn't mix with float ops (e.g. .round()), so
        # normalize those columns to float once here instead of at every call site.
        for col in df.columns:
            if df[col].apply(lambda v: isinstance(v, Decimal)).any():
                df[col] = df[col].astype(float)
        return df, None
    except Exception as e:
        return pd.DataFrame(), str(e)


@st.cache_data(ttl=300, show_spinner=False)
def cached_query(sql_text, params=None):
    return run_query(sql_text, params)


def channel_where(selected, column="channel_name"):
    if not selected:
        return "", ()
    placeholders = ",".join(["%s"] * len(selected))
    return f"WHERE {column} IN ({placeholders})", tuple(selected)


# =====================================================================
# Extraction (YouTube API -> DataFrames). Same logic as app.py, with
# api_key passed explicitly instead of read from a module global, and
# every numeric field defensively coalesced (a video with e.g. hidden
# likes previously produced a None that silently broke migration later
# — see app.py history).
# =====================================================================
def yt_client(api_key):
    return build("youtube", "v3", developerKey=api_key)


def fetch_channel_details(youtube, channel_id):
    request = youtube.channels().list(part="snippet,contentDetails,statistics,status", id=channel_id)
    response = request.execute()
    rows = []
    for item in response["items"]:
        rows.append({
            "channel_name": item["snippet"]["title"],
            "channel_id": item["id"],
            "channel_playlist_id": item["contentDetails"]["relatedPlaylists"]["uploads"],
            "country": item["snippet"].get("country"),
            "channel_views": int(item["statistics"].get("viewCount") or 0),
            "subscription": int(item["statistics"].get("subscriberCount") or 0),
            "channel_uploads": int(item["statistics"].get("videoCount") or 0),
            "channel_age": item["snippet"]["publishedAt"],
            "channel_status": item["status"]["privacyStatus"],
        })
    return pd.DataFrame(rows)


def fetch_playlist(youtube, channel_id):
    playlists, next_page_token = [], None
    while True:
        response = youtube.playlists().list(
            part="snippet,contentDetails", channelId=channel_id, pageToken=next_page_token
        ).execute()
        for plist in response["items"]:
            playlists.append({
                "channel_id": plist["snippet"]["channelId"],
                "playlist_id": plist["id"],
                "playlist_name": plist["snippet"]["title"],
            })
        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break
    return pd.DataFrame(playlists)


_HOURS = re.compile(r"(\d+)H")
_MINUTES = re.compile(r"(\d+)M")
_SECONDS = re.compile(r"(\d+)S")


def _parse_duration_seconds(iso_duration):
    hours = _HOURS.search(iso_duration)
    minutes = _MINUTES.search(iso_duration)
    seconds = _SECONDS.search(iso_duration)
    h = int(hours.group(1)) if hours else 0
    m = int(minutes.group(1)) if minutes else 0
    s = int(seconds.group(1)) if seconds else 0
    return h * 3600 + m * 60 + s


def fetch_videos(youtube, channel_id):
    pl_response = youtube.channels().list(part="contentDetails", id=channel_id).execute()
    playlist_id = pl_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    video_ids, next_page_token = [], None
    while True:
        vi_response = youtube.playlistItems().list(
            part="snippet,contentDetails", maxResults=100,
            playlistId=playlist_id, pageToken=next_page_token,
        ).execute()
        video_ids.extend(item["contentDetails"]["videoId"] for item in vi_response["items"])
        next_page_token = vi_response.get("nextPageToken")
        if not next_page_token:
            break

    rows = []
    for vid in video_ids:
        response = youtube.videos().list(part="snippet,contentDetails,statistics", id=vid).execute()
        for vidstat in response["items"]:
            stats = vidstat["statistics"]
            rows.append({
                "channel_name": vidstat["snippet"]["channelTitle"],
                "channel_id": vidstat["snippet"]["channelId"],
                "video_id": vidstat["id"],
                "video_title": vidstat["snippet"]["title"],
                "duration": _parse_duration_seconds(vidstat["contentDetails"]["duration"]),
                "release_date": vidstat["snippet"]["publishedAt"],
                "tags": vidstat["snippet"].get("tags"),
                "thumbnail": vidstat["snippet"]["thumbnails"]["default"]["url"],
                "video_quality": vidstat["contentDetails"]["definition"],
                "views": int(stats.get("viewCount") or 0),
                "likes": int(stats.get("likeCount") or 0),
                "favorite": int(stats.get("favoriteCount") or 0),
                "comment_count": int(stats.get("commentCount") or 0),
                "description": vidstat["snippet"]["description"],
                "caption_status": vidstat["contentDetails"]["caption"],
            })
    return pd.DataFrame(rows)


def fetch_video_comments(youtube, channel_id):
    pl_response = youtube.channels().list(part="contentDetails", id=channel_id).execute()
    playlist_id = pl_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    video_ids, next_page_token = [], None
    while True:
        vi_response = youtube.playlistItems().list(
            part="snippet,contentDetails", maxResults=100,
            playlistId=playlist_id, pageToken=next_page_token,
        ).execute()
        video_ids.extend(item["contentDetails"]["videoId"] for item in vi_response["items"])
        next_page_token = vi_response.get("nextPageToken")
        if not next_page_token:
            break

    comments = []
    for vid in video_ids:
        try:
            c_response = youtube.commentThreads().list(
                part="snippet,replies", textFormat="plainText", maxResults=100, videoId=vid
            ).execute()
        except Exception:
            continue  # comments disabled on this video — skip, don't abort the whole channel
        for item in c_response.get("items", []):
            top = item["snippet"]["topLevelComment"]["snippet"]
            comments.append({
                "video_id": item["snippet"]["videoId"],
                "comment_id": item.get("id", 0),
                "author_name": top["authorDisplayName"],
                "comments": top["textDisplay"],
                "commented_date": top["publishedAt"],
            })
    return pd.DataFrame(comments)


def load_channel_to_mongo(mongo_client, api_key, channel_id):
    youtube = yt_client(api_key)
    df1 = fetch_channel_details(youtube, channel_id)
    if df1.empty:
        raise ValueError("No channel found for that ID — double-check it (starts with 'UC').")
    df2 = fetch_playlist(youtube, channel_id)
    df3 = fetch_videos(youtube, channel_id)
    df4 = fetch_video_comments(youtube, channel_id)

    channel_name = df1["channel_name"][0]
    main_document = {
        "channel_details": df1.to_dict(orient="records"),
        "playlist_details": df2.to_dict(orient="records"),
        "video_details": df3.to_dict(orient="records"),
        "comment_details": df4.to_dict(orient="records"),
    }
    db = mongo_client["Youtube"]
    db[channel_name].insert_one(main_document)
    return channel_name, len(df2), len(df3), len(df4)


# =====================================================================
# Migration MongoDB -> MySQL — idempotent (INSERT IGNORE) so re-running
# is always safe, with per-row error isolation so one bad video/comment
# can't silently abort the whole channel (both were real bugs found
# debugging this project — see commit history).
# =====================================================================
def _parse_dt(value):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S%z")
    except ValueError:
        return None


def migrate_channel(mongo_db, mysql_conn, collection_name):
    result = {
        "Channel": collection_name, "Status": "", "New rows": "", "Notes": "",
        "Videos in MySQL": 0, "Playlists in MySQL": 0, "Comments in MySQL": 0,
    }
    doc = mongo_db[collection_name].find_one({})
    if not doc:
        result["Status"] = "❌ Failed"
        result["Notes"] = "No document found in MongoDB for this collection"
        return result

    cur = mysql_conn.cursor()
    errors = []
    new_channel = new_video = new_playlist = new_comment = 0

    try:
        cd = doc.get("channel_details", [{}])[0]
        channel_id = cd.get("channel_id", "N/A")
        cur.execute(
            """INSERT IGNORE INTO channel
               (channel_name, channel_id, country, channel_views, subscription,
                channel_uploads, channel_status, channel_playlist_id)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (cd.get("channel_name", "N/A"), channel_id, cd.get("country", "N/A"),
             int(cd.get("channel_views") or 0), int(cd.get("subscription") or 0),
             int(cd.get("channel_uploads") or 0), cd.get("channel_status", "N/A"),
             cd.get("channel_playlist_id", "N/A")),
        )
        new_channel = cur.rowcount
    except Exception as e:
        mysql_conn.rollback()
        result["Status"] = "❌ Failed"
        result["Notes"] = f"channel: {e}"
        cur.close()
        return result

    video_rows = []
    for v in doc.get("video_details", []):
        try:
            video_rows.append((
                v.get("channel_name", "N/A"), v.get("channel_id", "N/A"), v.get("video_id", "N/A"),
                v.get("video_title", "N/A"), int(v.get("duration") or 0),
                _parse_dt(v.get("release_date")), v.get("thumbnail", "N/A"), v.get("video_quality", "N/A"),
                int(v.get("views") or 0), int(v.get("likes") or 0), int(v.get("favorite") or 0),
                int(v.get("comment_count") or 0), v.get("description", "N/A"), v.get("caption_status", "N/A"),
            ))
        except Exception as e:
            errors.append(f"video {v.get('video_id', '?')}: {e}")
    if video_rows:
        try:
            cur.executemany(
                """INSERT IGNORE INTO video
                   (channel_name, channel_id, video_id, video_title, duration, release_date,
                    thumbnail, video_quality, views, likes, favorite, comment_count, description, caption_status)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                video_rows,
            )
            new_video = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        except Exception as e:
            errors.append(f"video batch: {e}")

    playlist_rows = []
    for p in doc.get("playlist_details", []):
        try:
            playlist_rows.append((p.get("channel_id", "N/A"), p.get("playlist_id", "N/A"), p.get("playlist_name", "N/A")))
        except Exception as e:
            errors.append(f"playlist {p.get('playlist_id', '?')}: {e}")
    if playlist_rows:
        try:
            cur.executemany(
                "INSERT IGNORE INTO playlist (channel_id, playlist_id, playlist_name) VALUES (%s,%s,%s)",
                playlist_rows,
            )
            new_playlist = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        except Exception as e:
            errors.append(f"playlist batch: {e}")

    comment_rows = []
    for c in doc.get("comment_details", []):
        try:
            comment_rows.append((
                c.get("video_id", "N/A"), c.get("comment_id", "N/A"), c.get("author_name", "N/A"),
                c.get("comments", "N/A"), _parse_dt(c.get("commented_date")),
            ))
        except Exception as e:
            errors.append(f"comment {c.get('comment_id', '?')}: {e}")
    if comment_rows:
        try:
            cur.executemany(
                """INSERT IGNORE INTO comment (video_id, comment_id, author_name, comments, commented_date)
                   VALUES (%s,%s,%s,%s,%s)""",
                comment_rows,
            )
            new_comment = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        except Exception as e:
            errors.append(f"comment batch: {e}")

    mysql_conn.commit()

    # authoritative post-migration totals (don't trust rowcount alone for status)
    cur.execute("SELECT COUNT(*) FROM video WHERE channel_id=%s", (channel_id,))
    result["Videos in MySQL"] = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM playlist WHERE channel_id=%s", (channel_id,))
    result["Playlists in MySQL"] = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM comment c JOIN video v ON c.video_id=v.video_id WHERE v.channel_id=%s",
        (channel_id,),
    )
    result["Comments in MySQL"] = cur.fetchone()[0]
    cur.close()

    added_anything = new_channel or new_video or new_playlist or new_comment
    if errors:
        result["Status"] = "⚠️ Partial" if added_anything else "⚠️ Partial (nothing new)"
        result["Notes"] = f"{len(errors)} row(s) skipped — " + "; ".join(errors[:3]) + (" …" if len(errors) > 3 else "")
    elif added_anything:
        result["Status"] = "✅ Migrated"
    else:
        result["Status"] = "ℹ️ Already up to date"
    result["New rows"] = f"+{new_channel}ch / +{new_video}v / +{new_playlist}pl / +{new_comment}cm"
    return result


# =====================================================================
# Sidebar — connection status, always visible, plus cache controls.
# =====================================================================
with st.sidebar:
    st.header("📡 Status")
    missing = missing_secrets()
    if missing:
        st.error(f"Missing secrets: {', '.join(missing)}\n\nSee `.streamlit/secrets.toml.example`.")
        mongo_client, mongo_err = None, "secrets not configured"
        mysql_conn, mysql_err = None, "secrets not configured"
    else:
        mongo_client, mongo_err = get_mongo_client()
        mysql_conn, mysql_err = get_mysql_connection()

    st.markdown(f"**MongoDB** — {'🟢 connected' if mongo_client else '🔴 ' + str(mongo_err)}")
    st.markdown(f"**MySQL** — {'🟢 connected' if mysql_conn else '🔴 ' + str(mysql_err)}")

    if st.button("🔄 Retry connections", width="stretch"):
        get_mongo_client.clear()
        get_mysql_connection.clear()
        st.rerun()
    if st.button("🧹 Clear analysis cache", width="stretch"):
        cached_query.clear()
        st.toast("Analysis cache cleared — results will refetch.")

    st.divider()
    st.caption("YouTube Data Lakehouse and Analysis · Developed by Vivek S")

# =====================================================================
# Main layout
# =====================================================================
st.title("📺 YouTube Data Lakehouse and Analysis")
st.caption("Extract via YouTube Data API v3 → MongoDB (data lake) → MySQL (warehouse) → Analyze")

tab_overview, tab_extract, tab_migrate, tab_analysis, tab_reports = st.tabs(
    ["🏠 Overview", "📥 Extract & Load", "🔄 Migrate", "📊 Analysis Dashboard", "📋 Standard Reports"]
)

# ---------------------------------------------------------------- Overview
with tab_overview:
    if mysql_conn is None:
        st.info("Connect MySQL (see sidebar) to see the warehouse overview.")
    else:
        counts, err = cached_query(
            "SELECT (SELECT COUNT(*) FROM channel) c, (SELECT COUNT(*) FROM video) v, "
            "(SELECT COUNT(*) FROM playlist) p, (SELECT COUNT(*) FROM comment) cm"
        )
        totals, _ = cached_query("SELECT SUM(subscription), SUM(channel_views) FROM channel")
        if err:
            st.error(err)
        elif counts.empty:
            st.info("No data in the warehouse yet — extract a channel and migrate it first.")
        else:
            c = counts.iloc[0]
            subs, views = (totals.iloc[0, 0] or 0, totals.iloc[0, 1] or 0) if not totals.empty else (0, 0)
            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric("Channels", compact_number(c["c"]))
            col2.metric("Videos", compact_number(c["v"]))
            col3.metric("Playlists", compact_number(c["p"]))
            col4.metric("Comments", compact_number(c["cm"]))
            col5.metric("Total subscribers", compact_number(subs))

            st.divider()
            st.subheader("Channels in the warehouse")
            df_channels, err = cached_query(
                "SELECT channel_name AS Channel, subscription AS Subscribers, "
                "channel_views AS Views, channel_uploads AS Uploads FROM channel ORDER BY subscription DESC"
            )
            if err:
                st.error(err)
            else:
                st.dataframe(df_channels, width="stretch", hide_index=True)
                download_button(df_channels, "channels_overview.csv", key="dl_overview")

# ---------------------------------------------------------- Extract & Load
with tab_extract:
    st.subheader("Extract a channel and load it into MongoDB")
    col1, col2 = st.columns(2)
    api_key = col1.text_input("YouTube Data API v3 key", type="password",
                               help="From Google Cloud Console → APIs & Services → Credentials")
    channel_id = col2.text_input("Channel ID", placeholder="UCxxxxxxxxxxxxxxxxxxxxxx")

    go = st.button("Extract & Load", type="primary", disabled=(mongo_client is None))
    if mongo_client is None:
        st.caption("⚠️ MongoDB isn't connected — fix that in the sidebar first.")

    if go:
        if not api_key or not channel_id:
            st.error("Enter both an API key and a channel ID.")
        else:
            status = st.status("Extracting channel data…", expanded=True)
            try:
                status.write("Fetching channel profile…")
                name, n_playlists, n_videos, n_comments = load_channel_to_mongo(mongo_client, api_key, channel_id)
                status.update(label=f"Done — {name}", state="complete")
                st.success(f"**{name}**: {n_videos} videos, {n_playlists} playlists, "
                           f"{n_comments} comments loaded to MongoDB.")
                st.balloons()
            except Exception as e:
                status.update(label="Failed", state="error")
                st.error(f"Extraction failed: {e}")

# -------------------------------------------------------------- Migrate
with tab_migrate:
    st.subheader("Migrate MongoDB collections into the MySQL warehouse")
    if mongo_client is None or mysql_conn is None:
        st.warning("Both MongoDB and MySQL need to be connected (see sidebar).")
    else:
        mongo_db = mongo_client["Youtube"]
        collections = mongo_db.list_collection_names()
        if not collections:
            st.info("No channels in MongoDB yet — extract one first.")
        else:
            selected = st.multiselect("Channels to migrate", collections, default=collections)
            st.caption("Migration is idempotent — safe to re-run any time; it only adds rows "
                       "that aren't already there, and reports exactly what changed per channel.")
            if st.button("Migrate selected", type="primary", disabled=not selected):
                progress = st.progress(0.0, text="Starting…")
                rows = []
                for i, name in enumerate(selected):
                    progress.progress(i / len(selected), text=f"Migrating {name}…")
                    rows.append(migrate_channel(mongo_db, mysql_conn, name))
                progress.progress(1.0, text="Done")
                result_df = pd.DataFrame(rows)
                st.dataframe(result_df, width="stretch", hide_index=True)
                cached_query.clear()
                st.caption("Analysis cache cleared automatically so the dashboard reflects this migration.")

# ------------------------------------------------------------- Analysis
with tab_analysis:
    if mysql_conn is None:
        st.info("Connect MySQL (see sidebar) to see the analysis dashboard.")
    else:
        df_names, err = cached_query("SELECT channel_name FROM channel ORDER BY channel_name")
        all_channels = df_names["channel_name"].tolist() if not df_names.empty else []
        if not all_channels:
            st.info("No data in the warehouse yet — extract and migrate a channel first.")
        else:
            selected_channels = st.multiselect(
                "Filter by channel (empty = all channels)", all_channels, default=[]
            )
            where, params = channel_where(selected_channels)

            st.markdown("#### Channel rankings")
            colA, colB = st.columns(2)
            with colA:
                df, err = cached_query(
                    f"SELECT channel_name, subscription FROM channel {where} ORDER BY subscription DESC", params
                )
                st.altair_chart(bar_chart(df, "channel_name", "subscription",
                                           "Subscribers by channel", "Channel", "Subscribers"),
                                 width="stretch")
                with st.expander("View data"):
                    st.dataframe(df, width="stretch", hide_index=True)
                download_button(df, "subscribers_by_channel.csv", key="dl_subs")
            with colB:
                df, err = cached_query(
                    f"SELECT channel_name, channel_views FROM channel {where} ORDER BY channel_views DESC", params
                )
                st.altair_chart(bar_chart(df, "channel_name", "channel_views",
                                           "Total views by channel", "Channel", "Views"),
                                 width="stretch")
                with st.expander("View data"):
                    st.dataframe(df, width="stretch", hide_index=True)
                download_button(df, "views_by_channel.csv", key="dl_views")

            st.markdown("#### Views vs. subscribers")
            df, err = cached_query(f"SELECT channel_name, channel_views, subscription FROM channel {where}", params)
            chart = scatter_chart(df, "channel_views", "subscription", "channel_name",
                                   "Do more views mean more subscribers?", "Views", "Subscribers")
            if chart is not None:
                st.altair_chart(chart, width="stretch")
                with st.expander("View data"):
                    st.dataframe(df, width="stretch", hide_index=True)

            st.markdown("#### Top videos")
            t1, t2, t3 = st.tabs(["Most viewed", "Most liked", "Most commented"])
            with t1:
                df, err = cached_query(
                    f"SELECT channel_name, video_title, views FROM video {where} ORDER BY views DESC LIMIT 10", params
                )
                st.altair_chart(bar_chart(df, "video_title", "views", "Top 10 most-viewed videos",
                                           "Video", "Views"), width="stretch")
                st.dataframe(df, width="stretch", hide_index=True)
                download_button(df, "top10_most_viewed.csv", key="dl_viewed")
            with t2:
                df, err = cached_query(
                    f"SELECT channel_name, video_title, likes FROM video {where} ORDER BY likes DESC LIMIT 10", params
                )
                st.altair_chart(bar_chart(df, "video_title", "likes", "Top 10 most-liked videos",
                                           "Video", "Likes"), width="stretch")
                st.dataframe(df, width="stretch", hide_index=True)
                download_button(df, "top10_most_liked.csv", key="dl_liked")
            with t3:
                df, err = cached_query(
                    f"SELECT channel_name, video_title, comment_count FROM video {where} "
                    f"ORDER BY comment_count DESC LIMIT 10", params
                )
                st.altair_chart(bar_chart(df, "video_title", "comment_count", "Top 10 most-commented videos",
                                           "Video", "Comments"), width="stretch")
                st.dataframe(df, width="stretch", hide_index=True)
                download_button(df, "top10_most_commented.csv", key="dl_commented")

            st.markdown("#### Engagement rate (top 10 videos)")
            st.caption("(likes + comments) / views × 100 — which videos punch above their view count.")
            connector = "AND" if where else "WHERE"
            df, err = cached_query(
                f"""SELECT channel_name, video_title,
                        (likes + comment_count) / NULLIF(views, 0) * 100 AS engagement_rate
                    FROM video {where}
                    {connector} views > 0
                    ORDER BY engagement_rate DESC LIMIT 10""",
                params,
            )
            if not df.empty:
                df["engagement_rate"] = df["engagement_rate"].round(2)
            st.altair_chart(bar_chart(df, "video_title", "engagement_rate", "Top 10 by engagement rate",
                                       "Video", "Engagement %"), width="stretch")
            st.dataframe(df, width="stretch", hide_index=True)
            download_button(df, "top10_engagement.csv", key="dl_engagement")

            st.markdown("#### Upload activity over time")
            connector = "AND" if where else "WHERE"
            df, err = cached_query(
                f"""SELECT channel_name, YEAR(release_date) AS year, COUNT(*) AS videos
                    FROM video {where}
                    {connector} release_date IS NOT NULL
                    GROUP BY channel_name, YEAR(release_date) ORDER BY year""",
                params,
            )
            if df.empty:
                st.info("No dated videos to chart yet.")
            elif df["channel_name"].nunique() > 6:
                st.caption("More than 6 channels selected — showing the combined total. "
                           "Narrow the channel filter above to ≤6 to see per-channel lines.")
                agg = df.groupby("year", as_index=False)["videos"].sum()
                st.altair_chart(
                    ordinal_bar_chart(agg, "year", "videos", "Videos published per year (all selected channels)",
                                       "Year", "Videos"),
                    width="stretch",
                )
            else:
                st.altair_chart(
                    line_chart_multi(df, "year", "videos", "channel_name",
                                      "Videos published per year", "Videos"),
                    width="stretch",
                )
            with st.expander("View data"):
                st.dataframe(df, width="stretch", hide_index=True)

            st.markdown("#### Video duration distribution")
            df, err = cached_query(f"SELECT duration / 60.0 AS duration_minutes FROM video {where}", params)
            st.altair_chart(histogram_chart(df, "duration_minutes", "Video duration distribution",
                                             "Duration (minutes)"), width="stretch")

            st.markdown("#### Average video duration per channel")
            df, err = cached_query(
                f"SELECT channel_name, AVG(duration) / 60.0 AS avg_minutes FROM video {where} "
                f"GROUP BY channel_name ORDER BY avg_minutes DESC",
                params,
            )
            if not df.empty:
                df["avg_minutes"] = df["avg_minutes"].round(1)
            st.altair_chart(bar_chart(df, "channel_name", "avg_minutes", "Average video duration per channel",
                                       "Channel", "Minutes"), width="stretch")
            st.dataframe(df, width="stretch", hide_index=True)
            download_button(df, "avg_duration_per_channel.csv", key="dl_avgdur")

            st.markdown("#### Top commenters")
            comment_where = ""
            if selected_channels:
                placeholders = ",".join(["%s"] * len(selected_channels))
                comment_where = f"WHERE v.channel_name IN ({placeholders})"
            df, err = cached_query(
                f"""SELECT c.author_name AS commenter, COUNT(*) AS comments
                    FROM comment c JOIN video v ON c.video_id = v.video_id
                    {comment_where}
                    GROUP BY c.author_name ORDER BY comments DESC LIMIT 15""",
                params,
            )
            st.altair_chart(bar_chart(df, "commenter", "comments", "Top 15 commenters",
                                       "Commenter", "Comments"), width="stretch")
            st.dataframe(df, width="stretch", hide_index=True)
            download_button(df, "top_commenters.csv", key="dl_commenters")

            st.markdown("#### Most common words in video titles")
            df, err = cached_query(f"SELECT video_title FROM video {where}", params)
            if not df.empty:
                stopwords = {
                    "the", "a", "an", "of", "to", "in", "on", "and", "for", "with", "is",
                    "this", "how", "your", "you", "i", "my", "vs", "at", "by", "from", "it",
                    "de", "el", "la", "que", "en",
                }
                counter = Counter()
                for title in df["video_title"].dropna():
                    words = re.findall(r"[A-Za-z']+", str(title).lower())
                    counter.update(w for w in words if len(w) > 2 and w not in stopwords)
                word_df = pd.DataFrame(counter.most_common(20), columns=["word", "count"])
                st.altair_chart(bar_chart(word_df, "word", "count", "Most common words in titles",
                                           "Word", "Occurrences"), width="stretch")

            st.markdown("#### Channels active in a given year")
            years_df, _ = cached_query(
                "SELECT DISTINCT YEAR(release_date) AS yr FROM video WHERE release_date IS NOT NULL ORDER BY yr DESC"
            )
            years = years_df["yr"].dropna().astype(int).tolist() if not years_df.empty else []
            if years:
                year = st.selectbox("Year", years)
                df, err = cached_query(
                    f"SELECT DISTINCT channel_name AS Channel FROM video {where} "
                    f"{'AND' if where else 'WHERE'} YEAR(release_date) = %s",
                    params + (year,),
                )
                st.dataframe(df, width="stretch", hide_index=True)

# ------------------------------------------------------------- Standard Reports
with tab_reports:
    st.subheader("The original 10 report questions — enhanced with charts, sortable tables, and CSV export")
    if mysql_conn is None:
        st.info("Connect MySQL (see sidebar) to run these reports.")
    else:
        report_tabs = st.tabs([f"{i}" for i in range(1, 11)])

        with report_tabs[0]:
            st.write("**1. What are the names of all the videos and their corresponding channels?**")
            df, err = cached_query(
                "SELECT channel_name AS Channel, video_title AS Video "
                "FROM video WHERE channel_name IS NOT NULL"
            )
            st.dataframe(df, width="stretch", hide_index=True)
            download_button(df, "report1_video_channel_names.csv", key="dl_r1")

        with report_tabs[1]:
            st.write("**2. Which channels have the most videos, and how many?**")
            df, err = cached_query(
                "SELECT channel_name AS Channel, channel_uploads AS Videos "
                "FROM channel ORDER BY channel_uploads DESC"
            )
            st.altair_chart(bar_chart(df, "Channel", "Videos", "Videos per channel"), width="stretch")
            st.dataframe(df, width="stretch", hide_index=True)
            download_button(df, "report2_channel_video_count.csv", key="dl_r2")

        with report_tabs[2]:
            st.write("**3. What are the top 10 most-viewed videos and their channels?**")
            df, err = cached_query(
                "SELECT channel_name AS Channel, video_title AS Video, views AS Views "
                "FROM video ORDER BY views DESC LIMIT 10"
            )
            st.altair_chart(bar_chart(df, "Video", "Views", "Top 10 most-viewed videos"), width="stretch")
            st.dataframe(df, width="stretch", hide_index=True)
            download_button(df, "report3_top10_viewed.csv", key="dl_r3")

        with report_tabs[3]:
            st.write("**4. How many comments were made on each video?**")
            df, err = cached_query(
                "SELECT channel_name AS Channel, video_title AS Video, comment_count AS Comments "
                "FROM video ORDER BY comment_count DESC"
            )
            st.dataframe(df, width="stretch", hide_index=True)
            download_button(df, "report4_comment_counts.csv", key="dl_r4")

        with report_tabs[4]:
            st.write("**5. Which videos have the highest number of likes per channel?**")
            df, err = cached_query(
                """SELECT v.channel_name AS Channel, v.video_title AS Video, v.likes AS Likes
                   FROM video v
                   JOIN (SELECT channel_id, MAX(likes) AS max_likes FROM video GROUP BY channel_id) m
                     ON v.channel_id = m.channel_id AND v.likes = m.max_likes
                   ORDER BY v.likes DESC"""
            )
            st.dataframe(df, width="stretch", hide_index=True)
            download_button(df, "report5_max_likes_per_channel.csv", key="dl_r5")

        with report_tabs[5]:
            st.write("**6. What are the top 10 videos by likes?**")
            df, err = cached_query(
                "SELECT channel_name AS Channel, video_title AS Video, likes AS Likes "
                "FROM video ORDER BY likes DESC LIMIT 10"
            )
            st.altair_chart(bar_chart(df, "Video", "Likes", "Top 10 videos by likes"), width="stretch")
            st.dataframe(df, width="stretch", hide_index=True)
            download_button(df, "report6_top10_likes.csv", key="dl_r6")

        with report_tabs[6]:
            st.write("**7. What is the total number of views for each channel?**")
            df, err = cached_query(
                "SELECT channel_name AS Channel, channel_views AS Views FROM channel ORDER BY channel_views DESC"
            )
            st.altair_chart(bar_chart(df, "Channel", "Views", "Total views per channel"), width="stretch")
            st.dataframe(df, width="stretch", hide_index=True)
            download_button(df, "report7_views_per_channel.csv", key="dl_r7")

        with report_tabs[7]:
            st.write("**8. Which channels published videos in a given year?**")
            years_df, _ = cached_query(
                "SELECT DISTINCT YEAR(release_date) AS yr FROM video WHERE release_date IS NOT NULL ORDER BY yr DESC"
            )
            years = years_df["yr"].dropna().astype(int).tolist() if not years_df.empty else []
            if years:
                year = st.selectbox("Year", years, key="report8_year")
                df, err = cached_query(
                    "SELECT DISTINCT channel_name AS Channel FROM video WHERE YEAR(release_date) = %s", (year,)
                )
                st.dataframe(df, width="stretch", hide_index=True)
                download_button(df, f"report8_channels_active_{year}.csv", key="dl_r8")
            else:
                st.info("No dated videos yet.")

        with report_tabs[8]:
            st.write("**9. What is the average video duration per channel?**")
            df, err = cached_query(
                "SELECT channel_name AS Channel, AVG(duration) AS AverageDurationSeconds "
                "FROM video GROUP BY channel_name ORDER BY AverageDurationSeconds DESC"
            )
            st.altair_chart(bar_chart(df, "Channel", "AverageDurationSeconds", "Average duration per channel (s)"),
                             width="stretch")
            st.dataframe(df, width="stretch", hide_index=True)
            download_button(df, "report9_avg_duration.csv", key="dl_r9")

        with report_tabs[9]:
            st.write("**10. Which videos have the highest number of comments?**")
            df, err = cached_query(
                "SELECT channel_name AS Channel, video_title AS Video, comment_count AS Comments "
                "FROM video ORDER BY comment_count DESC LIMIT 10"
            )
            st.altair_chart(bar_chart(df, "Video", "Comments", "Top 10 most-commented videos"),
                             width="stretch")
            st.dataframe(df, width="stretch", hide_index=True)
            download_button(df, "report10_top10_commented.csv", key="dl_r10")
