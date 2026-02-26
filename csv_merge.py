from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

from merge_lib import (
    DEFAULT_ENCODING,
    MergeOptions,
    read_csv_bytes,
    merge_frames,
    to_csv_bytes,
)


def discover_files(inputs: List[str], pattern: str) -> List[Path]:
    files: List[Path] = []
    for item in inputs:
        p = Path(item)
        if p.is_dir():
            files.extend(sorted(p.glob(pattern)))
        elif p.is_file():
            files.append(p)
        else:
            files.extend(sorted(Path().glob(item)))

    seen = set()
    out: List[Path] = []
    for f in files:
        fp = f.resolve()
        if fp not in seen:
            seen.add(fp)
            out.append(fp)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="CSV-Dateien mergen (CLI).")
    ap.add_argument("-i", "--input", nargs="+", required=True, help="Dateien/Ordner/Globs")
    ap.add_argument("--pattern", default="*.csv", help="Pattern für Ordner-Input (default: *.csv)")
    ap.add_argument("-o", "--output", required=True, help="Output-Datei (z.B. merged.csv)")
    ap.add_argument("-d", "--delimiter", default=None, help="Delimiter (z.B. ';' oder ','); default=auto")
    ap.add_argument("--encoding", default=DEFAULT_ENCODING, help=f"Encoding (default: {DEFAULT_ENCODING})")
    ap.add_argument("--mode", choices=["fast", "smart"], default="fast")
    ap.add_argument("--how", choices=["union", "intersection", "strict"], default="union")
    ap.add_argument("--add-source", action="store_true", help="Spalte _source_file hinzufügen")
    ap.add_argument("--dedupe", action="store_true", help="Nur smart: Duplikate entfernen")

    args = ap.parse_args()

    files = discover_files(args.input, args.pattern)
    if not files:
        raise SystemExit("Keine passenden Dateien gefunden. Prüfe --input/--pattern.")

    opt = MergeOptions(
        mode=args.mode,
        how=args.how,
        delimiter=args.delimiter,
        encoding=args.encoding,
        add_source=bool(args.add_source),
        dedupe=bool(args.dedupe),
    )

    frames = []
    names = []
    delims = []

    for f in files:
        b = f.read_bytes()
        df, used = read_csv_bytes(b, delimiter=opt.delimiter, encoding=opt.encoding)
        frames.append(df)
        names.append(f.name)
        delims.append(used)

    merged = merge_frames(frames, names, opt)

    out_delim = opt.delimiter or delims[0] or ";"
    out_bytes = to_csv_bytes(merged, delimiter=out_delim, encoding=opt.encoding)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(out_bytes)

    print(
        f"OK: {len(files)} Datei(en) -> {out_path} | rows={len(merged)} cols={len(merged.columns)} "
        f"| delim={repr(out_delim)} mode={opt.mode}"
    )


if __name__ == "__main__":
    main()
