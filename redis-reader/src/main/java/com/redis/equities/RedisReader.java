package com.redis.equities;

import redis.clients.jedis.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Reads all keys written by str.py from Redis as fast as possible.
 *
 * Key format : {SYMxxxx}:TENOR:RISK  (e.g. {SYM0001}:1D:0.01)
 * Value format: N × 12-byte records, each = little-endian int64 timestamp + float32 value
 *
 * Keys are generated deterministically from the same parameters as str.py
 * (no SCAN needed – zero round-trips to discover keys).
 *
 * Strategy:
 *   1. Build the full key list locally, mirroring str.py's loops.
 *   2. Partition the key list across CPU threads.
 *   3. Each thread issues pipelined GET batches for its slice.
 *   4. Each thread decodes the binary payload in-place.
 */
public class RedisReader {

    // --- mirror str.py defaults (override with -D or env) -------------------
    private static final int     NUM_SYMBOLS    = intEnv("NUM_SYMBOLS",    5000);
    private static final int     NUM_RISKS      = intEnv("NUM_RISKS",      1);

    // Fixed tenor list – matches the active list in str.py
    private static final List<String> TENORS = List.of("1D");

    // --- tunables -----------------------------------------------------------
    private static final String  HOST           = System.getProperty("redis.host", "localhost");
    private static final int     PORT           = Integer.getInteger("redis.port", 6379);
    private static final int     THREADS        = Integer.getInteger("reader.threads",
                                                      Runtime.getRuntime().availableProcessors());
    private static final int     PIPELINE_BATCH = Integer.getInteger("reader.pipelineBatch", 500);
    // 0 = read all keys; any positive value caps how many keys are read
    private static final int     MAX_KEYS       = intEnv("MAX_KEYS", 0);
    // set to true to print every decoded point
    private static final boolean PRINT_POINTS   = Boolean.getBoolean("reader.printPoints");

    // Shared buffered writer: 256 KB buffer avoids per-point syscalls;
    // threads write one StringBuilder per pipeline batch (one lock acquisition per batch).
    private static final BufferedWriter STDOUT = new BufferedWriter(
            new OutputStreamWriter(System.out), 256 * 1024);
    // -------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {

        // ---- 1. Build key list locally (same triple-loop as str.py) ---------
        List<String> keys = buildKeys();
        if (MAX_KEYS > 0 && MAX_KEYS < keys.size()) {
            keys = keys.subList(0, MAX_KEYS);
        }
        System.out.printf("Keys to read: %d  (symbols=%d tenors=%d risks=%d%s)%n",
                keys.size(), NUM_SYMBOLS, TENORS.size(), NUM_RISKS,
                MAX_KEYS > 0 ? "  MAX_KEYS=" + MAX_KEYS : "");

        ConnectionPoolConfig cfg = new ConnectionPoolConfig();
        cfg.setMaxTotal(THREADS + 2);
        cfg.setMaxIdle(THREADS + 2);
        cfg.setMinIdle(THREADS);
        cfg.setTestOnBorrow(false);
        cfg.setTestOnReturn(false);

        System.out.printf("Connecting to Redis %s:%d  threads=%d  pipelineBatch=%d%n",
                HOST, PORT, THREADS, PIPELINE_BATCH);

        try (JedisPooled jedis = new JedisPooled(cfg, HOST, PORT)) {

            // ---- 2. Read all values in parallel --------------------------------
            AtomicLong totalPoints = new AtomicLong();
            ExecutorService exec = Executors.newFixedThreadPool(THREADS);
            List<Future<?>> futures = new ArrayList<>(THREADS);

            int chunkSize = (keys.size() + THREADS - 1) / THREADS;
            long t1 = System.nanoTime();

            for (int i = 0; i < keys.size(); i += chunkSize) {
                List<String> slice = keys.subList(i, Math.min(i + chunkSize, keys.size()));
                futures.add(exec.submit(() -> totalPoints.addAndGet(readSlice(jedis, slice))));
            }

            for (Future<?> f : futures) f.get();
            exec.shutdown();
            if (PRINT_POINTS) STDOUT.flush();

            long readMs = (System.nanoTime() - t1) / 1_000_000;
            double keysPerSec = keys.size() / Math.max(readMs / 1000.0, 0.001);

            System.out.printf("%nDone: %d keys | %d data points | %d ms | %.0f keys/s%n",
                    keys.size(), totalPoints.get(), readMs, keysPerSec);
        }
    }

    // -------------------------------------------------------------------------
    // Mirror str.py's key generation:
    //   symbols  = [f"SYM{i:04d}" for i in range(1, num_symbols + 1)]
    //   risks    = [f"0.{i:02d}"  for i in range(1, num_risks  + 1)]
    //   key      = f"{{{symbol}}}:{tenor}:{risk}"
    // -------------------------------------------------------------------------
    private static List<String> buildKeys() {
        List<String> keys = new ArrayList<>(NUM_SYMBOLS * TENORS.size() * NUM_RISKS);
        for (int s = 1; s <= NUM_SYMBOLS; s++) {
            String symbol = String.format("SYM%04d", s);
            for (String tenor : TENORS) {
                for (int r = 1; r <= NUM_RISKS; r++) {
                    String risk = String.format("0.%02d", r);
                    keys.add("{" + symbol + "}:" + tenor + ":" + risk);
                }
            }
        }
        return keys;
    }

    // -------------------------------------------------------------------------
    // Worker: pipelined GETs + inline binary decode for a slice of keys
    // -------------------------------------------------------------------------
    private static long readSlice(JedisPooled jedis, List<String> keys) {
        long points = 0;
        // Reused per-thread buffer – avoids allocating a new SB per batch
        StringBuilder sb = PRINT_POINTS ? new StringBuilder(PIPELINE_BATCH * 64) : null;
        for (int i = 0; i < keys.size(); i += PIPELINE_BATCH) {
            int end = Math.min(i + PIPELINE_BATCH, keys.size());
            List<String> batch = keys.subList(i, end);

            // Fire all GETs in one pipeline round-trip
            List<Response<byte[]>> responses = new ArrayList<>(batch.size());
            try (Pipeline pipe = jedis.pipelined()) {
                for (String key : batch) {
                    responses.add(pipe.get(key.getBytes()));
                }
                pipe.sync();
            }

            // Decode binary payload; accumulate output into sb
            if (sb != null) sb.setLength(0);
            for (int j = 0; j < batch.size(); j++) {
                byte[] data = responses.get(j).get();
                if (data != null) {
                    points += decode(batch.get(j), data, sb);
                }
            }

            // One write + one lock acquisition per pipeline batch, not per point
            if (sb != null && sb.length() > 0) {
                try {
                    synchronized (STDOUT) { STDOUT.write(sb.toString()); }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
        return points;
    }

    // -------------------------------------------------------------------------
    // Read NUM_SYMBOLS / NUM_RISKS from env (str.py uses os.getenv) or -D JVM property
    // -------------------------------------------------------------------------
    private static int intEnv(String name, int def) {
        String v = System.getenv(name);
        if (v != null) return Integer.parseInt(v);
        return Integer.getInteger(name, def);
    }

    // -------------------------------------------------------------------------
    // Decode: N × (int64 LE timestamp, float32 LE value) packed by struct.pack("<qf", ts, val)
    // Each record is exactly 12 bytes.
    // -------------------------------------------------------------------------
    private static long decode(String key, byte[] data, StringBuilder sb) {
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        long count = 0;
        while (buf.remaining() >= 12) {
            long  ts  = buf.getLong();   // 8 bytes – int64 timestamp (ms)
            float val = buf.getFloat();  // 4 bytes – float32 value
            if (sb != null) {
                sb.append(String.format("key=%-32s  ts=%d  val=%.6f%n", key, ts, val));
            }
            count++;
        }
        return count;
    }
}
