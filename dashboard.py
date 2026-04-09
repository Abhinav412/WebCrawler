#!/usr/bin/env python3
"""
Live Enrichment Dashboard — monitors the incubator enrichment pipeline in real time.

Run alongside crawl_incubators.py:
    Terminal 1:  uv run python crawl_incubators.py --mode enrich --input ./datasets/…
    Terminal 2:  uv run streamlit run dashboard.py
"""

import json
import os
import time
from pathlib import Path

import pandas as pd
import streamlit as st

# ── Page Config ──────────────────────────────────────────────
st.set_page_config(
    page_title="Incubator Pipeline Dashboard",
    page_icon="🚀",
    layout="wide",
)

DATASETS_DIR = "./datasets"
PROGRESS_FILE = os.path.join(DATASETS_DIR, "enrichment_progress.json")
LIVE_FILE = os.path.join(DATASETS_DIR, "enrichment_live.json")
CHROMA_DIR = "./chroma_db"

# ── Auto-refresh ─────────────────────────────────────────────
REFRESH_SECS = 5
st_autorefresh = st.empty()


def load_progress() -> dict:
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {"stage": "waiting", "completed": 0, "total": 0, "pct": 0.0, "detail": ""}


def load_live_entities() -> list[dict]:
    if os.path.exists(LIVE_FILE):
        try:
            with open(LIVE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return []


def load_live_discovery() -> list[dict]:
    disc_file = os.path.join(DATASETS_DIR, "discovery_live.json")
    if os.path.exists(disc_file):
        try:
            with open(disc_file, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return []


def estimate_chroma_chunks() -> int:
    """Rough estimate of vector chunks by counting parquet/bin files."""
    count = 0
    if os.path.exists(CHROMA_DIR):
        for root, _, files in os.walk(CHROMA_DIR):
            for f in files:
                if f.endswith((".parquet", ".bin")):
                    count += 1
    return count


# ── Header ───────────────────────────────────────────────────
st.title("🚀 Incubator Discovery & Enrichment Dashboard")
st.caption("Live monitoring of the crawl pipeline. Auto-refreshes every 5 seconds.")
st.divider()

# ── Progress Section ─────────────────────────────────────────
progress = load_progress()
entities = load_live_entities()

stage_emoji = {
    "waiting": "⏳",
    "harvesting": "🌐",
    "synthesis": "🧠",
    "done": "✅",
}

col_stage, col_count, col_pct = st.columns(3)
with col_stage:
    emoji = stage_emoji.get(progress["stage"], "❓")
    st.metric("Current Stage", f"{emoji} {progress['stage'].upper()}")
with col_count:
    st.metric("Entities Enriched", f"{progress['completed']} / {progress['total']}")
with col_pct:
    st.metric("Completion", f"{progress['pct']}%")

st.progress(min(progress["pct"] / 100, 1.0))

if progress["detail"]:
    st.info(f"**Latest:** {progress['detail']}")

st.divider()

# ── Metrics Row ──────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Entities in Live File", len(entities))

with col2:
    if entities:
        avg_comp = sum(e.get("data_completeness", 0) for e in entities) / len(entities)
        st.metric("Avg Data Completeness", f"{avg_comp:.0%}")
    else:
        st.metric("Avg Data Completeness", "—")

with col3:
    chroma_files = estimate_chroma_chunks()
    st.metric("ChromaDB Files", chroma_files)

with col4:
    live_disc = load_live_discovery()
    if live_disc:
        st.metric("Discovery Count (Live)", len(live_disc))
    else:
        # Fall back to checking written final files
        discovery_files = list(Path(DATASETS_DIR).glob("incubators_discovery_*.json"))
        if discovery_files:
            latest = max(discovery_files, key=lambda p: p.stat().st_mtime)
            try:
                with open(latest, "r") as f:
                    disc_data = json.load(f)
                disc_count = len(disc_data.get("entities", []))
            except Exception:
                disc_count = 0
            st.metric("Discovery Count", disc_count)
        else:
            st.metric("Discovery Count", "—")

st.divider()

# ── Charts ───────────────────────────────────────────────────
if entities:
    df = pd.DataFrame(entities)

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("📍 Entities by State")
        if "state" in df.columns:
            state_counts = df["state"].dropna().replace("", pd.NA).dropna().value_counts().head(15)
            if not state_counts.empty:
                st.bar_chart(state_counts)
            else:
                st.caption("No state data yet.")

    with col_right:
        st.subheader("📊 Data Completeness Distribution")
        if "data_completeness" in df.columns:
            bins = pd.cut(
                df["data_completeness"],
                bins=[0, 0.3, 0.6, 0.9, 1.01],
                labels=["< 30%", "30-60%", "60-90%", "90-100%"],
            )
            bin_counts = bins.value_counts().sort_index()
            st.bar_chart(bin_counts)

    st.divider()

    # ── Live Feed ────────────────────────────────────────────
    st.subheader("📋 Recently Enriched Incubators")
    recent = entities[-20:][::-1]  # Last 20, newest first
    display_df = pd.DataFrame(recent)[
        [c for c in ["name", "city", "state", "website", "data_completeness", "focus_sectors"]
         if c in pd.DataFrame(recent).columns]
    ]
    st.dataframe(display_df, use_container_width=True, height=400)
else:
    st.warning("No enriched entities yet. Start the enrichment pipeline to see live data here.")

# ── Auto-refresh via rerun ───────────────────────────────────
time.sleep(REFRESH_SECS)
st.rerun()
