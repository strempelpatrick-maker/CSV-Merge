from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from typing import List, Optional, Tuple

import pandas as pd


DEFAULT_ENCODING = "utf-8-sig"


@dataclass(frozen=True)
class MergeOptions:
    mode: str                # "fast" | "smart"
    how: str                 # "union" | "intersection" | "strict" (only for smart)
    delimiter: Optional[str] # None => auto per file/sample
    encoding: str            # e.g. "utf-8-sig"
    add_source: bool         # add _source_file column
    dedupe: bool             # drop duplicates (only smart)


def guess_delimiter(sample_text: str) -> str:
    candidates = [",", ";", "\t", "|"]
    try:
        dialect = csv.Sniffer().sniff(sample_text, delimiters=candidates)
        return dialect.delimiter
    except Exception:
        counts = {c: sample_text.count(c) for c in candidates}
        return max(counts, key=counts.get) if any(counts.values()) else ";"


def read_csv_bytes(
    file_bytes: bytes,
    *,
    delimiter: Optional[str],
    encoding: str,
) -> Tuple[pd.DataFrame, str]:
    sample = file_bytes[:8192].decode(encoding, errors="replace")
    used_delim = delimiter or guess_delimiter(sample)

    df = pd.read_csv(
        io.BytesIO(file_bytes),
        sep=used_delim,
        dtype=str,
        encoding=encoding,
        keep_default_na=False,
        na_filter=False,
        engine="python",
    )
    return df, used_delim


def merge_frames(frames: List[pd.DataFrame], names: List[str], opt: MergeOptions) -> pd.DataFrame:
    if not frames:
        raise ValueError("Keine DataFrames zum Mergen übergeben.")
    if len(frames) != len(names):
        raise ValueError("frames/names Längen stimmen nicht überein.")

    if opt.add_source:
        frames = [df.assign(_source_file=name) for df, name in zip(frames, names)]

    if opt.mode == "fast":
        cols0 = list(frames[0].columns)
        for i, df in enumerate(frames[1:], start=2):
            if list(df.columns) != cols0:
                raise ValueError(
                    "FAST-Modus erfordert identische Spaltenreihenfolge.\n"
                    f"Abweichung in Datei #{i}: {names[i-1]}\n"
                    f"Erwartet: {cols0}\n"
                    f"Gefunden: {list(df.columns)}\n"
                    "Tipp: Nutze SMART-Modus."
                )
        merged = pd.concat(frames, ignore_index=True)

    elif opt.mode == "smart":
        merged = pd.concat(frames, ignore_index=True, sort=False)

        if opt.how == "intersection":
            common = set(frames[0].columns)
            for df in frames[1:]:
                common &= set(df.columns)
            merged = merged[[c for c in merged.columns if c in common]]

        elif opt.how == "strict":
            cols0 = list(frames[0].columns)
            for i, df in enumerate(frames[1:], start=2):
                if list(df.columns) != cols0:
                    raise ValueError(
                        "STRICT erwartet identische Spaltenreihenfolge.\n"
                        f"Abweichung in Datei #{i}: {names[i-1]}"
                    )

        elif opt.how != "union":
            raise ValueError(f"Unbekanntes how='{opt.how}' (erwartet union/intersection/strict).")

        if opt.dedupe:
            merged = merged.drop_duplicates()

    else:
        raise ValueError("mode muss 'fast' oder 'smart' sein.")

    return merged


def to_csv_bytes(df: pd.DataFrame, *, delimiter: str, encoding: str) -> bytes:
    return df.to_csv(index=False, sep=delimiter, encoding=encoding).encode(encoding)
