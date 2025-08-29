#!/usr/bin/env python3
import csv
import datetime as dt
import base64
import hashlib
import json
import uuid
import os

def iso_time_ms(total_ms: int) -> str:
    total_ms %= 24 * 3600 * 1000
    h = total_ms // 3600000
    m = (total_ms % 3600000) // 60000
    s = (total_ms % 60000) // 1000
    ms = total_ms % 1000
    return f"{h:02d}:{m:02d}:{s:02d}.{ms:03d}"

def iso_time_us(total_us: int) -> str:
    total_us %= 24 * 3600 * 1_000_000
    h = total_us // 3_600_000_000
    m = (total_us % 3_600_000_000) // 60_000_000
    s = (total_us % 60_000_000) // 1_000_000
    us = total_us % 1_000_000
    return f"{h:02d}:{m:02d}:{s:02d}.{us:06d}"

def iso_duration(days: int, hours: int, minutes: int, seconds: int) -> str:
    return f"P{days}DT{hours}H{minutes}M{seconds}S"

def main():
    rows = 200_000
    out_path = os.path.join(os.getcwd(), "parquet_types.csv")

    headers = [
        "bool_col",
        "int32_col",
        "int64_col",
        "float_col",
        "double_col",
        "date_col",
        "time_millis_col",
        "time_micros_col",
        "ts_millis_col",
        "ts_micros_col",
        "string_utf8_col",
        "binary_base64_col",
        "fixed_len_16_hex_col",
        "decimal_9_2_col",
        "decimal_18_6_col",
        "decimal_38_10_col",
        "uuid_col",
        "json_col",
        "list_int_col",
        "map_str_int_col",
        "interval_iso8601_col",
    ]

    epoch = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
    date0 = dt.date(1970, 1, 1)

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)

        for i in range(rows):
            is_null = (i % 53 == 0)

            bool_val = "" if is_null else ("true" if i % 2 == 0 else "false")
            int32_val = "" if is_null else (i % 100000 - 50000)
            int64_val = "" if is_null else (i * 1000003 - 500_000_000_000)
            float_val = "" if is_null else (i % 1000) * 0.5
            double_val = "" if is_null else (i % 10000) * 0.000123
            date_val = "" if is_null else (date0 + dt.timedelta(days=(i % 20000))).isoformat()
            time_ms_val = "" if is_null else iso_time_ms((i * 137) % (24*3600*1000))
            time_us_val = "" if is_null else iso_time_us((i * 1009) % (24*3600*1_000_000))
            ts_millis = "" if is_null else (epoch + dt.timedelta(milliseconds=i*1337)).isoformat().replace("+00:00","Z")
            ts_micros = "" if is_null else (epoch + dt.timedelta(microseconds=i*977)).isoformat().replace("+00:00","Z")

            utf8_samples = ["naïve", "café", "mañana", "über", "façade", "smile🙂"]
            string_utf8 = "" if is_null else f"{utf8_samples[i % len(utf8_samples)]}-{i}"

            bin_bytes = f"row-{i}".encode()
            binary_b64 = "" if is_null else base64.b64encode(bin_bytes).decode()
            fixed16_hex = "" if is_null else hashlib.sha256(str(i).encode()).digest()[:16].hex()

            d_9_2 = "" if is_null else f"{((i % 1_000_000) - 500_000)/100:.2f}"
            d_18_6 = "" if is_null else f"{((i * 97003) % 1_000_000_000_000)/1_000_000:.6f}"
            big = (i * 1_000_000_000_003)
            d_38_10 = "" if is_null else f"{big/10_000_000_000:.10f}"

            uid = "" if is_null else str(uuid.uuid5(uuid.NAMESPACE_DNS, f"row-{i}"))

            json_str = "" if is_null else json.dumps({"row": i, "flag": i%2==0, "group": i%7}, separators=(",",":"))
            list_str = "" if is_null else json.dumps([i, i+1, i+2], separators=(",",":"))
            map_str = "" if is_null else json.dumps({"k1": i, "k2": i%10}, separators=(",",":"))
            dur = "" if is_null else iso_duration(i%30, (i//7)%24, (i//13)%60, (i//29)%60)

            w.writerow([
                bool_val, int32_val, int64_val, float_val, double_val,
                date_val, time_ms_val, time_us_val, ts_millis, ts_micros,
                string_utf8, binary_b64, fixed16_hex,
                d_9_2, d_18_6, d_38_10,
                uid, json_str, list_str, map_str, dur
            ])

    print(f"Wrote {rows:,} rows to {out_path}")

if __name__ == "__main__":
    main()