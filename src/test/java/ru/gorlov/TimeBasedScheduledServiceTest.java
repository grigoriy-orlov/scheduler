package ru.gorlov;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TimeBasedScheduledServiceTest {

    @Test
    public void testSchedule_delay() throws Exception {
        final int callTimes = 10;
        final int queueSize = 12;
        final TimeBasedScheduledService<Long> scheduledService =
                new TimeBasedScheduledService<>(queueSize);
        try {
            scheduledService.start();
            final List<Long> results = new ArrayList<>(callTimes);
            final CountDownLatch latch = new CountDownLatch(callTimes);

            final long start = System.nanoTime();
            for (int i = 0; i < callTimes; i++) {
                final long number = i;
                scheduledService
                        .schedule(new Date(System.currentTimeMillis() + 3_000 + 100 * i), () -> {
                            results.add(number);
                            latch.countDown();
                            return number;
                        });
            }

            latch.await(10, TimeUnit.SECONDS);
            final long stop = System.nanoTime();

            assertTrue((stop - start) / 1_000_000 >= 3);
            assertEquals(callTimes, results.size());
            assertOrder(results, callTimes);
        } finally {
            scheduledService.stop();
        }
    }

    @Test
    public void testSchedule_differentTime() throws Exception {
        final int callTimes = 10;
        final int queueSize = 10;
        final TimeBasedScheduledService<Long> scheduledService =
                new TimeBasedScheduledService<>(queueSize);
        try {
            scheduledService.start();
            final List<Long> results = new ArrayList<>(callTimes);
            final CountDownLatch latch = new CountDownLatch(callTimes);

            for (int i = 0; i < callTimes; i++) {
                final long number = i;
                scheduledService.schedule(new Date(System.currentTimeMillis() + 100 * i), () -> {
                    results.add(number);
                    latch.countDown();
                    return number;
                });
            }

            latch.await(5, TimeUnit.SECONDS);

            assertEquals(callTimes, results.size());
            assertOrder(results, callTimes);
        } finally {
            scheduledService.stop();
        }
    }

    @Test
    public void testSchedule_sameTime() throws Exception {
        final int callTimes = 10;
        final int queueSize = 10;
        final TimeBasedScheduledService<Long> scheduledService =
                new TimeBasedScheduledService<>(queueSize);
        try {
            scheduledService.start();
            final List<Long> results = new ArrayList<>(callTimes);
            final CountDownLatch latch = new CountDownLatch(callTimes);

            final long time = System.currentTimeMillis() + 1_000;
            for (int i = 0; i < callTimes; i++) {
                final long number = i;
                scheduledService.schedule(new Date(time), () -> {
                    results.add(number);
                    latch.countDown();
                    return number;
                });
            }

            latch.await(5, TimeUnit.SECONDS);

            assertEquals(callTimes, results.size());
            assertOrder(results, callTimes);
        } finally {
            scheduledService.stop();
        }
    }

    @Test
    public void testSchedule_overload() throws Exception {
        final int callTimes = 10;
        final int queueSize = 5;
        final TimeBasedScheduledService<Long> scheduledService =
                new TimeBasedScheduledService<>(queueSize);
        try {
            scheduledService.start();
            final List<Long> results = new ArrayList<>(callTimes);
            final CountDownLatch latch = new CountDownLatch(callTimes);

            final long time = System.currentTimeMillis() + 5_000;
            for (int i = 0; i < callTimes; i++) {
                final long number = i;
                scheduledService.schedule(new Date(time), () -> {
                    results.add(number);
                    latch.countDown();
                    return number;
                });
            }

            latch.await(10, TimeUnit.SECONDS);

            assertEquals(callTimes, results.size());
            assertOrder(results, callTimes);
        } finally {
            scheduledService.stop();
        }
    }

    @Test
    public void testSchedule_concurrent() throws Exception {
        final int callTimes = 1000;
        final int queueSize = 10;
        final int threads = 50;
        final AtomicLong callCounter = new AtomicLong();
        final TimeBasedScheduledService<Long> scheduledService =
                new TimeBasedScheduledService<>(queueSize);
        final ExecutorService executorService =
                Executors.newFixedThreadPool(threads);

        try {
            scheduledService.start();
            final List<Long> results = new ArrayList<>(callTimes);
            final CountDownLatch latch = new CountDownLatch(threads);

            for (int i = 0; i < threads; i++) {
                executorService.submit((Runnable) () -> {
                    while (callCounter.get() < callTimes) {
                        scheduledService.schedule(new Date(System.currentTimeMillis()),
                                () -> {
                                    results.add(callCounter.getAndIncrement());
                                    return 1L;
                                });
                    }
                    latch.countDown();
                });
            }

            latch.await(10, TimeUnit.SECONDS);

            assertOrder(results, callTimes);
        } finally {
            scheduledService.stop();
        }
    }

    private static void assertOrder(List<Long> results, int callTimes) {
        for (int i = 0; i < callTimes - 1; i++) {
            assertEquals(i, (long) results.get(i));
        }
    }
}