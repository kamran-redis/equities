import sys
import random
import os
import struct

# -----------------------
# Configurable parameters (env override)
# -----------------------
# Allow overriding dataset size and batch size via env vars.
num_symbols = int(os.getenv("NUM_SYMBOLS", 5000))
num_risks = int(os.getenv("NUM_RISKS", 1))
batch_points_per_append = int(os.getenv("BATCH_POINTS_PER_APPEND", 5000))

# Generate synthetic symbol universe like SYM0001, SYM0002, ...
symbols = [f"SYM{i:04d}" for i in range(1, num_symbols + 1)]

tenors = [
    # Limit to a single tenor by default; expand the list for more.
    "1D"
    # "1D", "2D", "3D", "4D",
    # "1W", "2W",
    # "1M", "2M", "3M", "6M", "9M",
    # "1Y", "2Y"
]

# Synthetic risk bucket labels like 0.01, 0.02, ...
risks = [f"0.{i:02d}" for i in range(1, num_risks + 1)]

# Fixed horizon and day length used for timestamps.
num_days = 5040
day_ms = 24 * 60 * 60 * 1000

# Write binary RESP to stdout (e.g., to pipe into redis-cli --pipe).
out = sys.stdout.buffer

def emit(*parts):
    # Encode a Redis RESP array for the given parts.
    out.write(f"*{len(parts)}\r\n".encode())
    for p in parts:
        if not isinstance(p, (bytes, bytearray)):
            p = str(p).encode()
        out.write(f"${len(p)}\r\n".encode())
        out.write(p + b"\r\n")

# -----------------------
# Data ingestion (binary append per key)
# -----------------------
for symbol in symbols:
    for tenor in tenors:
        for risk in risks:
            # Use hash tags so all keys for a symbol map to the same slot.
            key = f"{{{symbol}}}:{tenor}:{risk}"
            buf = []

            for d in range(num_days):
                ts = d * day_ms
                val = random.uniform(0.1, 0.9)
                # Pack as little-endian (int64 timestamp, float32 value).
                buf.append(struct.pack("<qf", ts, val))

                if len(buf) >= batch_points_per_append:
                    # Flush batched points as a single APPEND.
                    emit("APPEND", key, b"".join(buf))
                    buf.clear()

            if buf:
                # Emit any remaining buffered points.
                emit("APPEND", key, b"".join(buf))
