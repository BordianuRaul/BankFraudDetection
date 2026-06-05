package org.example.fraud.detection;

import java.util.ArrayList;
import java.util.List;

public record RecentActivity(List<Long> timestamps) {

    public static RecentActivity empty() {
        return new RecentActivity(List.of());
    }

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
