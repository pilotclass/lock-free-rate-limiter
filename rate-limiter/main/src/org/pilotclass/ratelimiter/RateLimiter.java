package org.pilotclass.ratelimiter;

import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class RateLimiter {
    private final long windowNs;
    private final long nsPerItem;
    private final AtomicLong nextAvailableTimestamp;
    private final Acquisition nothingAcquired = new Acquisition(0);

    public RateLimiter(final long maxItems, final long window, final TimeUnit timeUnit, final long preFill, final Instant now) {
        this(maxItems, timeUnit.toNanos(window), preFill, now);
    }

    private RateLimiter(final long maxItems, final long windowNs, final long preFill, final Instant now) {
        this.windowNs = windowNs;
        this.nsPerItem = windowNs / maxItems;
        nextAvailableTimestamp = new AtomicLong(0);
        tryAcquire(Math.min(maxItems, preFill), now);
    }

    public static RateLimiter createPreFilled(final long maxItems, final long window, final TimeUnit timeUnit) {
        return new RateLimiter(maxItems, window, timeUnit, maxItems, Instant.now());
    }

    public Acquisition tryAcquireNow(final int items) {
        return tryAcquire(items, Instant.now());
    }

    public Acquisition tryAcquire(final long items, final Instant instant) {
        return tryAcquire(items, nanosFor(instant));
    }

    public void rollback(long items) {
        nextAvailableTimestamp.addAndGet(-items * nsPerItem);
    }

    private Acquisition tryAcquire(final long items, final long timestampOfAcquisition) {
        final long localNextAvailable = nextAvailableTimestamp.get();
        final long newNextAvailableTimestamp = nextAvailableTimestampAfterAcquiring(items, timestampOfAcquisition, localNextAvailable);
        if (newNextAvailableTimestamp <= timestampOfAcquisition && nextAvailableTimestamp.compareAndSet(localNextAvailable, newNextAvailableTimestamp)) {
            return new Acquisition(items);
        }
        return nothingAcquired;
    }

    private long nextAvailableTimestampAfterAcquiring(final long items, final long timestampOfAcquisition, final long previousNextAvailableTimestamp) {
        return Long.max(previousNextAvailableTimestamp, timestampOfAcquisition - windowNs) + items * nsPerItem;
    }

    private static long nanosFor(final Instant instant) {
        return TimeUnit.SECONDS.toNanos(instant.getEpochSecond()) + instant.getNano();
    }

    public class Acquisition {
        private long numAcquired;

        private Acquisition(long numAcquired) {
            this.numAcquired = numAcquired;
        }

        public boolean acquired() {
            return numAcquired > 0;
        }

        public void rollback() {
            RateLimiter.this.rollback(numAcquired);
            numAcquired = 0;
        }
    }
}
