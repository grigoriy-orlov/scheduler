package ru.gorlov;

import java.util.Date;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

//TODO add scheduler state handling (created, running, stopped)
//TODO add logging
public class TimeBasedScheduledService<V> {
    private static final Logger log = Logger.getLogger(TimeBasedScheduledService.class.getName());

    private final PriorityQueue<CallableDecorator<V>> queue = new PriorityQueue<>();
    private final AtomicLong sequenceNumber = new AtomicLong();
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private final int nowaitQueueCapacity;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public TimeBasedScheduledService(int nowaitQueueCapacity) {
        if (nowaitQueueCapacity < 0) {
            throw new IllegalStateException("nowaitQueueCapacity must be more than -1");
        }
        this.nowaitQueueCapacity = nowaitQueueCapacity;
    }

    @SuppressWarnings("InfiniteLoopStatement")
    public void start() {
        executor.execute(() -> {
            while (true) {
                try {
                    lock.lockInterruptibly();
                    while (queue.isEmpty()) {
                        condition.await();
                    }
                    while (queue.size() < nowaitQueueCapacity
                            && queue.peek().dateTime.getTime() > System.currentTimeMillis()) {
                        condition.await(System.currentTimeMillis() -
                                        queue.peek().dateTime.getTime(),
                                TimeUnit.MILLISECONDS);
                    }
                    CallableDecorator<V> task = queue.poll();
                    task.call();
                } catch (InterruptedException e) {
                    return;
                } catch (Exception e) {
                    log.severe("task execution error: " + e);
                } finally {
                    lock.unlock();
                }
            }
        });
    }

    public void stop() {
        executor.shutdown();
    }

    public void schedule(Date dateTime, Callable<V> callable) {
        try {
            lock.lock();
            queue.add(new CallableDecorator<>(callable, dateTime, sequenceNumber.getAndIncrement()));
            condition.signal();
        } finally {
            lock.unlock();
        }
    }

    private static class CallableDecorator<V1> implements Callable<V1>, Comparable<V1> {

        private final Callable<V1> callable;
        private final Date dateTime;
        private final long sequenceNumber;

        private CallableDecorator(Callable<V1> callable,
                                  Date dateTime,
                                  long sequenceNumber) {
            this.callable = callable;
            this.dateTime = dateTime;
            this.sequenceNumber = sequenceNumber;
        }

        @Override
        public V1 call() throws Exception {
            return callable.call();
        }

        @SuppressWarnings("unchecked")
        @Override
        public int compareTo(V1 other) {
            if (other == this) {
                return 0;
            }
            CallableDecorator<V1> otherDecorator = (CallableDecorator<V1>) other;
            if (dateTime.compareTo(otherDecorator.dateTime) != 0) {
                return dateTime.compareTo(otherDecorator.dateTime);
            }
            if (sequenceNumber < otherDecorator.sequenceNumber) {
                return -1;
            } else if (sequenceNumber > otherDecorator.sequenceNumber) {
                return 1;
            } else {
                log.severe("two task have same time and sequenceNumber (continue handling)");
                return -1;
            }
        }

        @Override
        public String toString() {
            return "CallableDecorator{" +
                    "dateTime=" + dateTime +
                    ", sequenceNumber=" + sequenceNumber +
                    '}';
        }
    }

}
