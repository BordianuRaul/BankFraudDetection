package org.example.fraud.detection;

import java.util.ArrayList;
import java.util.List;

/** Recent transaction timestamps (epoch millis) for one account — the sliding window for velocity. */
public record RecentActivity(List<Long> timestamps) {

    public static RecentActivity empty() {
        return new RecentActivity(List.of());
    }

    /** Append {@code nowMs} and drop anything older than {@code windowMs}. */
    public RecentActivity record(long nowMs, long windowMs) {
        long cutoff = nowMs - windowMs;
        List<Long> kept = new ArrayList<>(timestamps.size() + 1);
        for (long t : timestamps) {
            if (t >= cutoff) kept.add(t);
        }
        kept.add(nowMs);
        return new RecentActivity(kept);
    }

    public int count() {
        return timestamps.size();
    }
}
