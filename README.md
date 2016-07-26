# Scheduler library #

* Class executes Callable at Datetime or after Datetime as soon as possible if it is busy.
* schedule() differs from scheduleAfterNow() by Callable with Datetime less than now resolution. schedule() executes it immediately. scheduleAfterNow() just doesn't execute it.
* If tasks execution start order must be consistent with task submitting order you need to synchronize before schedule() or scheduleAfterNow() call (see ru.gorlov.SchedulerServiceTest.testScheduleOkConcurrent()) 