package ru.gorlov;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SchedulerServiceTest {

    @Test
    public void testScheduleExactOk() throws Exception {
        int callTimes = 3;
        SchedulerService<Integer> schedulerService = new UnorderedSchedulerService<>();
        final List<Optional<ScheduledFuture<Integer>>> results = new ArrayList<>();

        for (int i = 0; i < callTimes; i++) {
            final int number = i;
            results.add(schedulerService.scheduleAfterNow(
                    () -> number, new Date(System.currentTimeMillis() + 1000)));
        }

        assertEquals(3, results.size());
        assertEquals(0, (long) results.get(0).get().get());
        assertEquals(1, (long) results.get(1).get().get());
        assertEquals(2, (long) results.get(2).get().get());
    }

    @Test
    public void testScheduleExactOneTime() throws Exception {
        int callTimes = 3;
        SchedulerService<Integer> schedulerService = new UnorderedSchedulerService<>();
        final List<Optional<ScheduledFuture<Integer>>> results = new ArrayList<>();
        Date dateTime = new Date(System.currentTimeMillis() + 1000);

        for (int i = 0; i < callTimes; i++) {
            final int number = i;
            results.add(schedulerService.scheduleAfterNow(() -> number, dateTime));
        }

        assertEquals(3, results.size());
        assertEquals(0, (long) results.get(0).get().get());
        assertEquals(1, (long) results.get(1).get().get());
        assertEquals(2, (long) results.get(2).get().get());
    }

    @Test
    public void testScheduleExactNoTasks() throws Exception {
        int callTimes = 3;
        SchedulerService<Integer> schedulerService = new UnorderedSchedulerService<>();
        final List<Optional<ScheduledFuture<Integer>>> results = new ArrayList<>();

        for (int i = 0; i < callTimes; i++) {
            final int number = i;
            results.add(schedulerService.scheduleAfterNow(
                    () -> number, new Date(System.currentTimeMillis() - 1000)));
        }

        assertFalse(results.get(0).isPresent());
        assertFalse(results.get(1).isPresent());
        assertFalse(results.get(2).isPresent());
    }

    @Test
    public void testScheduleOk() throws Exception {
        int callTimes = 3;
        SchedulerService<Integer> schedulerService = new UnorderedSchedulerService<>();
        final List<ScheduledFuture<Integer>> results = new ArrayList<>();

        for (int i = 0; i < callTimes; i++) {
            final int number = i;
            results.add(schedulerService.schedule(
                    () -> number, new Date(System.currentTimeMillis() + 1000)));
        }

        assertEquals(3, results.size());
        assertEquals(0, (long) results.get(0).get());
        assertEquals(1, (long) results.get(1).get());
        assertEquals(2, (long) results.get(2).get());
    }

    @Test
    public void testScheduleOneTime() throws Exception {
        int callTimes = 3;
        SchedulerService<Integer> schedulerService = new UnorderedSchedulerService<>();
        final List<ScheduledFuture<Integer>> results = new ArrayList<>();
        Date dateTime = new Date(System.currentTimeMillis() + 1000);

        for (int i = 0; i < callTimes; i++) {
            final int number = i;
            results.add(schedulerService.schedule(() -> number, dateTime));
        }

        assertEquals(3, results.size());
        assertEquals(0, (long) results.get(0).get());
        assertEquals(1, (long) results.get(1).get());
        assertEquals(2, (long) results.get(2).get());
    }

    @Test
    public void testScheduleNoTasks() throws Exception {
        int callTimes = 3;
        SchedulerService<Integer> schedulerService = new UnorderedSchedulerService<>();
        final List<ScheduledFuture<Integer>> results = new ArrayList<>();

        for (int i = 0; i < callTimes; i++) {
            final int number = i;
            results.add(schedulerService.schedule(
                    () -> number, new Date(System.currentTimeMillis() - 1000)));
        }

        assertEquals(3, results.size());
        assertEquals(0, (long) results.get(0).get());
        assertEquals(1, (long) results.get(1).get());
        assertEquals(2, (long) results.get(2).get());
    }

    @Test
    public void testScheduleOkConcurrent() throws Exception {
        final int callTimes = 100000;
        int threads = 100;
        ExecutorService executorService =
                Executors.newFixedThreadPool(threads);
        final AtomicLong integer = new AtomicLong();

        final SchedulerService<Long> schedulerService = new UnorderedSchedulerService<>();
        final List<ScheduledFuture<Long>> futures = new ArrayList<>(callTimes);
        final CountDownLatch latch = new CountDownLatch(threads);
        final Lock lock = new ReentrantLock(true);

        for (int i = 0; i < threads; i++) {
            executorService.submit((Runnable) () -> {
                while (true) {
                    try {
                        lock.lock();
                        if (integer.get() > callTimes) {
                            latch.countDown();
                            return;
                        }
                        futures.add(schedulerService.schedule(integer::incrementAndGet,
                                new Date(System.currentTimeMillis())));
                    } finally {
                        lock.unlock();
                    }
                }
            });
        }

        latch.await(10, TimeUnit.SECONDS);

        List<Long> results = futures.stream().limit(callTimes).map(f -> {
            try {
                return f.get(1, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());

        int errors = 0;
        for (int i = 0; i < results.size() - 1; i++) {
            if (results.get(i + 1) <= results.get(i)) {
                errors++;
            }
        }
        assertEquals(0, errors);
    }

}