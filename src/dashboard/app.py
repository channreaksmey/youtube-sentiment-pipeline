import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine

st.set_page_config(page_title="YouTube Sentiment Analytics", page_icon="", layout="wide")

@st.cache_resource
def get_engine():
    return create_engine("postgresql://de_user:de_pass@localhost:5432/youtube_dw")

engine = get_engine()

@st.cache_data(ttl=30)
def load_data():
    fact = pd.read_sql("SELECT * FROM fact_sentiment", engine)
    dim_time = pd.read_sql("SELECT * FROM dim_time", engine)
    dim_video = pd.read_sql("SELECT * FROM dim_video", engine)
    dim_author = pd.read_sql("SELECT * FROM dim_author", engine)
    # Load silver comments to access the original comment text
    try:
        silver_comments = pd.read_sql("SELECT comment_id, text_cleaned FROM silver_comments", engine)
    except Exception:
        # If the table doesn't exist or can't be loaded, provide an empty DataFrame
        silver_comments = pd.DataFrame(columns=["comment_id", "text_cleaned"])
    return fact, dim_time, dim_video, dim_author, silver_comments

fact, dim_time, dim_video, dim_author, silver_comments = load_data()

# Merge
df = fact.merge(dim_time, on="time_sk", how="left")
df = df.merge(dim_video, on="video_sk", how="left")
df = df.merge(dim_author, on="author_sk", how="left")

# Merge in the original cleaned text if available
df = df.merge(silver_comments, on="comment_id", how="left")

# Handle column name differences between HF and Rule-based Gold layers
if "author_name" in df.columns and "author" not in df.columns:
    df = df.rename(columns={"author_name": "author"})

st.title("YouTube Sentiment & Emotion Analytics")
st.markdown("Powered by Advanced Transformer Models (Emotion Detection)")

# KPIs
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Comments", len(df))
with col2:
    pos = (df["sentiment_label"] == "positive").sum()
    st.metric("Positive Sentiment", pos, f"{pos/len(df)*100:.1f}%")
with col3:
    if "emotion_label" in df.columns:
        top_emotion = df["emotion_label"].mode()[0] if not df["emotion_label"].empty else "N/A"
        st.metric("Dominant Emotion", top_emotion.capitalize())
    else:
        st.metric("Dominant Emotion", "N/A (Run Pipeline)")
with col4:
    neg = (df["sentiment_label"] == "negative").sum()
    st.metric("Negative Sentiment", neg, f"{neg/len(df)*100:.1f}%")

# Row 1
left, right = st.columns(2)

with left:
    st.subheader("Emotion Distribution")
    if "emotion_label" in df.columns:
        emotion_counts = df["emotion_label"].value_counts().reset_index()
        emotion_counts.columns = ["emotion", "count"]
        fig = px.pie(emotion_counts, names="emotion", values="count", hole=0.3, 
                     color="emotion", color_discrete_sequence=px.colors.qualitative.Pastel)
        st.plotly_chart(fig, width='stretch')
    else:
        st.warning("Emotion data not found. Please run the updated pipeline.")

with right:
    st.subheader("Sentiment Over Time")
    time_sent = df.groupby(["hour", "sentiment_label"]).size().reset_index(name="count")
    fig = px.line(time_sent, x="hour", y="count", color="sentiment_label")
    st.plotly_chart(fig, width='stretch')

# Row 2
left, right = st.columns(2)

with left:
    st.subheader("Emotion by Video")
    if "emotion_label" in df.columns:
        vid_emo = df.groupby(["video_id", "emotion_label"]).size().reset_index(name="count")
        fig = px.bar(vid_emo, x="video_id", y="count", color="emotion_label", barmode="group")
        st.plotly_chart(fig, width='stretch')
    else:
        st.info("Run the pipeline to see emotion breakdown by video.")

with right:
    st.subheader("Confidence Distribution")
    fig = px.histogram(df, x="confidence", color="sentiment_label", nbins=20)
    st.plotly_chart(fig, width='stretch')

# Row 3: Confidence analysis
st.subheader("Model Confidence Analysis")

conf_by_sent = df.groupby("sentiment_label")["confidence"].agg(["mean", "min", "max"]).reset_index()
fig = go.Figure(data=[
    go.Bar(name="Mean", x=conf_by_sent["sentiment_label"], y=conf_by_sent["mean"]),
    go.Bar(name="Min", x=conf_by_sent["sentiment_label"], y=conf_by_sent["min"]),
    go.Bar(name="Max", x=conf_by_sent["sentiment_label"], y=conf_by_sent["max"])
])
fig.update_layout(barmode="group", title="Confidence Stats by Sentiment")
st.plotly_chart(fig, width='stretch')

# Sample high-confidence predictions
st.subheader("High-Confidence Predictions")
high_conf = df[df["confidence"] > 0.95][["text_cleaned", "sentiment_label", "confidence", "like_count"]].head(10)
st.dataframe(high_conf)

st.markdown("---")
st.markdown("Built with PySpark + Kafka + PostgreSQL + Streamlit")