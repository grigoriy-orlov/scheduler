package ru.gorlov;

import java.util.Date;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class UnorderedSchedulerService<V> implements SchedulerService<V> {

    private final ScheduledExecutorService schedulerService =
            Executors.newScheduledThreadPool(1);

    @Override
    public Optional<ScheduledFuture<V>> scheduleAfterNow(Callable<V> callable, Date dateTime)
            throws RejectedExecutionException {
        Objects.requireNonNull(callable, "callable must be not null");
        Objects.requireNonNull(dateTime, "dateTime must be not null");

        long now = System.currentTimeMillis();
        long scheduledTime = dateTime.getTime();
        if (scheduledTime < now) {
            return Optional.empty();
        }
        long delay = scheduledTime - now;
        return Optional.of(schedulerService.schedule(callable, delay, TimeUnit.MILLISECONDS));
    }

    @Override
    public ScheduledFuture<V> schedule(Callable<V> callable, Date dateTime)
            throws RejectedExecutionException {
        Objects.requireNonNull(callable, "callable must be not null");
        Objects.requireNonNull(dateTime, "dateTime must be not null");

        long now = System.currentTimeMillis();
        long scheduledTime = dateTime.getTime();
        long delay = (scheduledTime > now) ? (scheduledTime - now) : 0;
        return schedulerService.schedule(callable, delay, TimeUnit.MILLISECONDS);
    }
}
