from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from typing import List, Optional, Tuple

import pandas as pd


DEFAULT_ENCODING = "utf-8-sig"
COMMON_ENCODINGS = ["utf-8-sig", "utf-8", "cp1252", "latin1"]  # pragmatic EU defaults


@dataclass(frozen=True)
class MergeOptions:
    mode: str                # "fast" | "smart"
    how: str                 # "union" | "intersection" | "strict" (only for smart)
    delimiter: Optional[str] # None => auto per file/sample
    encoding: str            # e.g. "auto" | "utf-8-sig" | "cp1252" ...
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


def _encoding_try_order(requested: Optional[str]) -> List[str]:
    if not requested or requested.lower() == "auto":
        return COMMON_ENCODINGS[:]
    req = requested
    order = [req] + [e for e in COMMON_ENCODINGS if e.lower() != req.lower()]
    # de-dup, keep order
    seen = set()
    out = []
    for e in order:
        k = e.lower()
        if k not in seen:
            seen.add(k)
            out.append(e)
    return out


def read_csv_bytes(
    file_bytes: bytes,
    *,
    delimiter: Optional[str],
    encoding: str,
) -> Tuple[pd.DataFrame, str, str]:
    # Delimiter detection: decode a small sample with replacement to avoid crashes
    sample_text = file_bytes[:8192].decode("utf-8", errors="replace")
    used_delim = delimiter or guess_delimiter(sample_text)

    enc_order = _encoding_try_order(encoding)
    last_err: Optional[Exception] = None

    # First: strict decoding attempts
    for enc in enc_order:
        try:
            df = pd.read_csv(
                io.BytesIO(file_bytes),
                sep=used_delim,
                dtype=str,
                encoding=enc,
                keep_default_na=False,
                na_filter=False,
                engine="python",
            )
            return df, used_delim, enc
        except UnicodeDecodeError as e:
            last_err = e
            continue

    # Last resort: replace bad bytes (prevents Streamlit crash)
    fallback_enc = enc_order[-1] if enc_order else "latin1"
    try:
        df = pd.read_csv(
            io.BytesIO(file_bytes),
            sep=used_delim,
            dtype=str,
            encoding=fallback_enc,
            encoding_errors="replace",  # pandas >= 1.5/2.x
            keep_default_na=False,
            na_filter=False,
            engine="python",
        )
        return df, used_delim, fallback_enc
    except TypeError:
        # Older pandas: decode ourselves then parse from text buffer
        text = file_bytes.decode(fallback_enc, errors="replace")
        df = pd.read_csv(
            io.StringIO(text),
            sep=used_delim,
            dtype=str,
            keep_default_na=False,
            na_filter=False,
            engine="python",
        )
        return df, used_delim, fallback_enc
    except Exception as e:
        raise last_err or e


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
    # When exporting, utf-8-sig is usually Excel-friendly
    enc = encoding if encoding and encoding.lower() != "auto" else DEFAULT_ENCODING
    return df.to_csv(index=False, sep=delimiter, encoding=enc).encode(enc)
