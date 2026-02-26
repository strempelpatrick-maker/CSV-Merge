from __future__ import annotations

import streamlit as st
import pandas as pd

from merge_lib import (
    DEFAULT_ENCODING,
    MergeOptions,
    read_csv_bytes,
    merge_frames,
    to_csv_bytes,
)

st.set_page_config(page_title="CSV Merge Tool", layout="wide")
st.title("CSV Merge Tool (Online)")

with st.sidebar:
    st.header("Einstellungen")

    mode = st.selectbox(
        "Modus",
        ["fast", "smart"],
        index=0,
        help="fast: identische Header/Spaltenreihenfolge (schnell). smart: flexible Spalten.",
    )

    how = st.selectbox(
        "SMART-Strategie",
        ["union", "intersection", "strict"],
        index=0,
        disabled=(mode != "smart"),
        help="union: alle Spalten (Default), intersection: nur gemeinsame, strict: identisch",
    )

    delimiter_choice = st.selectbox("Delimiter", ["auto", ";", ",", "\\t", "|"], index=0)
    delimiter = None
    if delimiter_choice == "\\t":
        delimiter = "\t"
    elif delimiter_choice != "auto":
        delimiter = delimiter_choice

    encoding = st.selectbox("Encoding", [DEFAULT_ENCODING, "utf-8", "cp1252", "latin1"], index=0)

    add_source = st.checkbox("Spalte _source_file hinzufügen", value=True)
    dedupe = st.checkbox("Duplikate entfernen (smart)", value=False, disabled=(mode != "smart"))

opt = MergeOptions(
    mode=mode,
    how=how,
    delimiter=delimiter,
    encoding=encoding,
    add_source=add_source,
    dedupe=dedupe,
)

st.write("### CSV-Dateien hochladen")
uploads = st.file_uploader(
    "Mehrere CSVs auswählen",
    type=["csv", "txt"],
    accept_multiple_files=True,
)

if not uploads:
    st.caption("Install: pip install streamlit pandas  |  Start: streamlit run app.py")
    st.stop()

total_size = sum(u.size for u in uploads)
if total_size > 200 * 1024 * 1024:
    st.error("Zu groß: Bitte insgesamt < 200 MB hochladen.")
    st.stop()

frames: list[pd.DataFrame] = []
names: list[str] = []
delims: list[str] = []

for u in uploads:
    b = u.getvalue()
    df, used = read_csv_bytes(b, delimiter=opt.delimiter, encoding=opt.encoding)
    frames.append(df)
    names.append(u.name)
    delims.append(used)

st.info(f"Erkannte/benutzte Delimiter: {', '.join(sorted(set(repr(d) for d in delims)))}")

try:
    merged = merge_frames(frames, names, opt)
except Exception as e:
    st.error(str(e))
    st.stop()

st.success(
    f"Gemerged: {len(uploads)} Datei(en) → {len(merged):,} Zeilen, {len(merged.columns)} Spalten"
)

st.write("### Vorschau")
st.dataframe(merged.head(200), use_container_width=True)

out_delim = opt.delimiter or delims[0] or ";"
csv_bytes = to_csv_bytes(merged, delimiter=out_delim, encoding=opt.encoding)

st.download_button(
    label="Merged CSV herunterladen",
    data=csv_bytes,
    file_name="merged.csv",
    mime="text/csv",
)
