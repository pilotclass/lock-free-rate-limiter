package org.pilotclass.ratelimiter;

import org.junit.Test;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class RateLimiterTest {

    private final Instant now;

    public RateLimiterTest() {
        now = Instant.now();
    }

    @Test
    public void testPreFill() {
        RateLimiter rl = new RateLimiter(10, 100, TimeUnit.NANOSECONDS, 2, now);
        assertFalse(rl.tryAcquire(9, now).acquired());
        assertTrue(rl.tryAcquire(8, now).acquired());
        rl = new RateLimiter(10, 1, TimeUnit.SECONDS, 20, now);
        assertFalse(rl.tryAcquire(1, now).acquired());
    }

    @Test
    public void testRollback() {
        RateLimiter rl = new RateLimiter(10, 100, TimeUnit.NANOSECONDS, 0, now);
        RateLimiter.Acquisition acquisition = rl.tryAcquire(10, now);
        assertTrue(acquisition.acquired());
        assertFalse(rl.tryAcquire(1, now).acquired());
        acquisition.rollback();
        assertFalse(acquisition.acquired());
        assertTrue(rl.tryAcquire(1, now).acquired());
    }

    @Test
    public void testThatAcquireTakesAway() {
        RateLimiter rl = new RateLimiter(10, 100, TimeUnit.NANOSECONDS, 0, now);
        assertTrue(rl.tryAcquire(10, now).acquired());
        assertFalse(rl.tryAcquire(0, now).acquired());
    }

    @Test
    public void testConstructedEmptyAllowsStuffing() {
        RateLimiter rl = new RateLimiter(10, 100, TimeUnit.NANOSECONDS, 0, now);
        assertTrue(rl.tryAcquire(10, now).acquired());
    }

    @Test
    public void testRateOverTime() {
        RateLimiter rl = new RateLimiter(4, 100, TimeUnit.MICROSECONDS, 4, now);
        assertFalse(rl.tryAcquire(1,now.plusNanos(24_000)).acquired());
        assertFalse(rl.tryAcquire(2,now.plusNanos(49_000)).acquired());
        assertFalse(rl.tryAcquire(3,now.plusNanos(74_000)).acquired());
        assertFalse(rl.tryAcquire(4,now.plusNanos(99_000)).acquired());

        RateLimiter.Acquisition aq;
        assertTrue((aq = rl.tryAcquire(1, now.plusNanos( 25_000))).acquired()); aq.rollback();
        assertTrue((aq = rl.tryAcquire(2, now.plusNanos( 50_000))).acquired()); aq.rollback();
        assertTrue((aq = rl.tryAcquire(3, now.plusNanos( 75_000))).acquired()); aq.rollback();
        assertTrue((aq = rl.tryAcquire(4, now.plusNanos(100_000))).acquired()); aq.rollback();
    }

    @Test
    public void testOverAcquire() {
        RateLimiter rl = new RateLimiter(4, 100, TimeUnit.NANOSECONDS, 4, now);
        assertFalse(rl.tryAcquire(5,now.plusNanos(0)).acquired());
    }
    @Test
    public void testUsage() throws InterruptedException {
        final int EXPECTED_ACCURACY_ON_WINDOWS_XP = 15;
        final int cycles = 10;
        final int numPerTestTimeUnit = 1_234_567;
        final TimeUnit testTimeUnit = TimeUnit.SECONDS;
        final RateLimiter rl = RateLimiter.createPreFilled(numPerTestTimeUnit, 1, testTimeUnit);
        int count = cycles * numPerTestTimeUnit;
        final long startNs = System.nanoTime();
        while (count > 0) {
            final RateLimiter.Acquisition acquisition = rl.tryAcquireNow(1);
            if (acquisition.acquired()) {
                count--;
            } else {
                acquisition.waitBeforeTryingAgain();
            }
        }
        final long endNs = System.nanoTime();
        final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(endNs - startNs);
        final long expectedMs = testTimeUnit.toMillis(cycles);
        // Enable to see actual vs elapsed time
//        assertEquals(expectedMs, elapsedMs);
        assertTrue(Math.abs(elapsedMs - expectedMs) < EXPECTED_ACCURACY_ON_WINDOWS_XP);
    }

}