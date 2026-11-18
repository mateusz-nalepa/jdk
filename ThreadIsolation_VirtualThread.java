import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class ThreadIsolation_VirtualThread {
    private static final long STARTED_AT = System.nanoTime();

    static {
        System.setProperty("jdk.virtualThreadScheduler.parallelism", "1");
        System.setProperty("jdk.virtualThreadScheduler.maxPoolSize", "1");
    }

    // horizontal
    private static final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private static final ExecutorService executorForCriticalTask = Executors.newSchedulerForVirtualThread(1, "critical");
    private static final ExecutorService executorForNonCriticalTask = Executors.newSchedulerForVirtualThread(1, "non-critical");

    // vertical
    private static final ExecutorService executorForTasks = Executors.newVirtualThreadPerTaskExecutor();
    private static final ExecutorService executorForIo = Executors.newSchedulerForVirtualThread(1, "io");
    private static final ExecutorService executorForCpu = Executors.newSchedulerForVirtualThread(1, "cpu");

    public static void main(String[] args) throws Exception {
        horizontalIsolation();
        log("");
        log("");
        verticalIsolation();
    }

    private static void horizontalIsolation() throws Exception {
        log("### HORIZONTAL");

        var nonCritical = executor.submit(() -> someNonCriticalTask());

        sleep(1_000);
        log("[main] After 1 s submitting critical: CPU only, 100 ms.");
        var critical = executor.submit(() -> criticalTask());

        critical.get();
        nonCritical.get();

        executor.close();
        executorForCriticalTask.close();
        executorForNonCriticalTask.close();
    }

    private static void criticalTask() {
        log("[critical] Started...");
        Thread.whenVirtualContinueOn(executorForCriticalTask);
        cpuPart("critical", 100);
        log("[critical] End");
    }

    private static void someNonCriticalTask() {
        log("[non-critical] Started...");
        Thread.whenVirtualContinueOn(executorForNonCriticalTask);
        cpuPart("non-critical", 5_000);
        log("[non-critical] End");
    }

    private static void verticalIsolation() throws Exception {
        log("### VERTICAL");
//        log("Expected: task-2 I/O executed during task-1 CPU, total around 16 s");
        long startedAt = System.nanoTime();

        var task1 = executorForTasks.submit(() -> ioAndCpuTask("task-1"));

        sleep(6_000);
        log("[main] After 6 s submitting task-2: I/O 5 s + CPU 5 s.");
        var task2 = executorForTasks.submit(() -> ioAndCpuTask("task-2"));

        task1.get();
        log("[main] task-1 done.");
        task2.get();
        log("[main] task-2 done.");
        log("[main] Vertical scenario time: "
                + (System.nanoTime() - startedAt) / 1_000_000L + " ms.");

        executorForTasks.close();
        executorForIo.close();
        executorForCpu.close();
    }

    private static void ioAndCpuTask(String id) {
        // io
        throwingExecutionScopeRun(executorForIo, id, () -> ioPart(id, 5_000));

        // cpu
        throwingExecutionScopeRun(executorForCpu, id, () -> cpuPart(id, 5_000));
    }

    private static void throwingExecutionScopeRun(ExecutorService executor, String taskId, Runnable originalTask) {
        try {
            // todo: use monitored executor instead of monitored runnable
            var monitoredTask = new MonitoredRunnable(taskId, originalTask);
            ExecutionScope.run(executor, monitoredTask);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void ioPart(String id, long millis) {
        sleep(millis);
    }

    private static void cpuPart(String id, long millis) {
        long startedAt = System.nanoTime();
        long iterations = 0;
        while (System.nanoTime() - startedAt < millis * 1_000_000L) {
            iterations++;
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void log(String message) {
        long elapsedMillis = (System.nanoTime() - STARTED_AT) / 1_000_000L;
        System.out.printf("%6d ms | %s%n", elapsedMillis, message);
    }

    static class MonitoredRunnable implements Runnable {
        private final String taskId;
        private final Runnable delegate;
        private final long createdAt;
        private final String submitThread;

        MonitoredRunnable(String taskId, Runnable delegate) {
            this.taskId = taskId;
            this.delegate = delegate;
            this.createdAt = System.nanoTime();
            this.submitThread = Thread.currentThread().toString();
            log(String.format(
                    "[%s] TASK SUBMITTED | submittedOn=%s",
                    taskId, submitThread));
        }

        @Override
        public void run() {
            long startExecution = System.nanoTime();
            long queueWaitMs = Duration.ofNanos(startExecution - createdAt).toMillis();
            String startThread = Thread.currentThread().toString();

            try {
                delegate.run();
            } finally {
                long end = System.nanoTime();
                long executionMs = Duration.ofNanos(end - startExecution).toMillis();
                long totalMs = Duration.ofNanos(end - createdAt).toMillis();

                log(String.format(
                        "[%s] TASK DONE | queueWait=%d ms | execution=%d ms | total=%d ms"
                                + " | \n\t\t\t\t submittedOn=%s | startedOn=%s | finishedOn=%s",
                        taskId, queueWaitMs, executionMs, totalMs,
                        submitThread, startThread, Thread.currentThread()));
            }
        }
    }
}
