package ru.gorlov;

import java.util.Date;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;

public interface SchedulerService<V> {
    Optional<ScheduledFuture<V>> scheduleAfterNow(Callable<V> callable, Date dateTime)
            throws RejectedExecutionException;

    ScheduledFuture<V> schedule(Callable<V> callable, Date dateTime)
                    throws RejectedExecutionException;
}
