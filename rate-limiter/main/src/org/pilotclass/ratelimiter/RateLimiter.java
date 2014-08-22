package org.pilotclass.ratelimiter;

import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class RateLimiter {
    private final long maxItems;
    private final long windowNs;
    private final long nsPerItem;
    private final AtomicLong nextAvailable;

    public RateLimiter(long maxItems, long window, TimeUnit timeUnit, long prefill, Instant now) {
        this(maxItems, timeUnit.toNanos(window), prefill, now);
    }

    public RateLimiter(long maxItems, long windowNs, long prefill, Instant now) {
        this.maxItems = maxItems;
        this.windowNs = windowNs;
        this.nsPerItem = windowNs / maxItems;
        nextAvailable = new AtomicLong(nextAvailableGiven(now, maxItems - prefill));
    }

    public static RateLimiter createPreFilled(long maxItems, long window, TimeUnit timeUnit) {
        return new RateLimiter(maxItems, window, timeUnit, maxItems, Instant.now());
    }

    public Acquisition tryAcquireNow() {
        return tryAcquire(1, Instant.now());
    }

    public void rollback(long items) {
        nextAvailable.addAndGet(-items * nsPerItem);
    }

    public void setAvailable(final Instant instant, final long available) {
        this.nextAvailable.set(nextAvailableGiven(instant, available));
    }

    private long nextAvailableGiven(Instant instant, long available) {
        return asNanos(instant) - available * nsPerItem;
    }

    private static long asNanos(Instant instant) {
        return TimeUnit.SECONDS.toNanos(instant.getEpochSecond()) + instant.getNano();
    }

    public Acquisition tryAcquire(long items, Instant instant) {
        return tryAcquire(items, asNanos(instant));
    }

    private Acquisition tryAcquire(long items, long instantNs) {
        long localNextAvailable = nextAvailable.get();
        long newNextAvailable = Long.max(localNextAvailable, instantNs - windowNs) + items * nsPerItem;
        if (newNextAvailable <= instantNs && nextAvailable.compareAndSet(localNextAvailable, newNextAvailable)) {
            return new Acquisition(items);
        }
        return new Acquisition(0);
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
