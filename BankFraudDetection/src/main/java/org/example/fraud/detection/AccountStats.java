package org.example.fraud.detection;

/** Running per-account amount statistics via Welford's online mean/variance algorithm. */
public record AccountStats(long count, double mean, double m2) {

    public static AccountStats empty() {
        return new AccountStats(0, 0.0, 0.0);
    }

    /** Fold one amount in, returning updated stats. */
    public AccountStats add(double value) {
        long newCount = count + 1;
        double delta = value - mean;
        double newMean = mean + delta / newCount;
        double newM2 = m2 + delta * (value - newMean);
        return new AccountStats(newCount, newMean, newM2);
    }

    /** Sample standard deviation, or 0 with fewer than 2 samples. */
    public double stddev() {
        if (count < 2) return 0.0;
        return Math.sqrt(m2 / (count - 1));
    }
}
