python3 << 'EOF' | redis-cli --pipe
import sys
import random
import os

# -----------------------
# Configurable parameters (env override)
# -----------------------
num_symbols = int(os.getenv("NUM_SYMBOLS", 5000))
num_risks = int(os.getenv("NUM_RISKS", 1))

symbols = [f"SYM{i:04d}" for i in range(1, num_symbols + 1)]

tenors = [
"1D"
#    "1D", "2D", "3D", "4D",
#    "1W", "2W",
#    "1M", "2M", "3M", "6M", "9M",
#    "1Y", "2Y"
]

risks = [f"0.{i:02d}" for i in range(1, num_risks + 1)]

num_days = 5040
day_ms = 24 * 60 * 60 * 1000
points_per_madd = 5000

out = sys.stdout.buffer

def emit(*parts):
    out.write(f"*{len(parts)}\r\n".encode())
    for p in parts:
        if not isinstance(p, (bytes, bytearray)):
            p = str(p).encode()
        out.write(f"${len(p)}\r\n".encode())
        out.write(p + b"\r\n")

# -----------------------
# Create series with labels
# -----------------------
for symbol in symbols:
    for tenor in tenors:
        for risk in risks:
            key = f"{{{symbol}}}:{tenor}:{risk}"
            emit(
                "TS.CREATE", key,
                "LABELS",
                "symbol", symbol,
                "tenor", tenor,
                "risk", risk
            )

# -----------------------
# Data ingestion
# -----------------------
batch = []

for symbol in symbols:
    for tenor in tenors:
        for risk in risks:
            key = f"{{{symbol}}}:{tenor}:{risk}"

            for d in range(num_days):
                ts = d * day_ms
                val = f"{random.uniform(0.1, 0.9):.6f}"

                batch.extend([key, str(ts), val])

                if len(batch) >= points_per_madd * 3:
                    emit("TS.MADD", *batch)
                    batch.clear()

if batch:
    emit("TS.MADD", *batch)

EOF