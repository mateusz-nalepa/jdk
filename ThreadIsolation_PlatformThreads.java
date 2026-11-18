import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadIsolation_PlatformThreads {
    private static final long STARTED_AT = System.nanoTime();

    // horizontal
    private static final ExecutorService executor = pool("horizontal-");
    private static final ExecutorService executorForCriticalTask = pool("critical-");
    private static final ExecutorService executorForNonCriticalTask = pool("non-critical-");

    // vertical
    private static final ExecutorService executorForTasks = pool("vertical-");
    private static final ExecutorService executorForIo = pool("io-");
    private static final ExecutorService executorForCpu = pool("cpu-");

    public static void main(String[] args) throws Exception {
        horizontalIsolation();
        log("");
        log("");
        verticalIsolation();
    }

    private static void horizontalIsolation() throws Exception {
        log("### HORIZONTAL");
//        log("Expected: critical task executed during non-critical task");

        var nonCritical = executor.submit(() -> someNonCriticalTask());

        sleep(1_000);
        log("[main] After 1 s submitting critical: CPU only, 100 ms.");
        var critical = executor.submit(() -> criticalTask());

        critical.get().get();
        nonCritical.get().get();

        executor.close();
        executorForCriticalTask.close();
        executorForNonCriticalTask.close();
    }

    private static CompletableFuture<Void> criticalTask() {
        return utilityCall(executorForCriticalTask, "critical", () -> cpuPart("critical", 100));
    }

    private static CompletableFuture<Void> someNonCriticalTask() {
        return utilityCall(executorForNonCriticalTask, "non-critical", () -> cpuPart("non-critical", 5_000));
    }

    private static void verticalIsolation() throws Exception {
        log("### VERTICAL");
        long startedAt = System.nanoTime();

        var task1 = executorForTasks.submit(() -> ioAndCpuTask("task-1"));

        sleep(6_000);
        log("[main] After 6 s submitting task-2: I/O 5 s + CPU 5 s.");
        var task2 = executorForTasks.submit(() -> ioAndCpuTask("task-2"));

        task1.get().get();
        task2.get().get();
        log("[main] Vertical scenario time: "
                + (System.nanoTime() - startedAt) / 1_000_000L + " ms.");

        executorForTasks.close();
        executorForIo.close();
        executorForCpu.close();
    }

    private static CompletableFuture<Void> ioAndCpuTask(String id) {
        return utilityCall(executorForIo, id + "/io", () -> {
            ioPart(id, 5_000);
        }).thenCompose(ignored -> utilityCall(executorForCpu, id + "/cpu", () -> {
            cpuPart(id, 5_000);
        }));
    }

    private static CompletableFuture<Void> utilityCall(ExecutorService executor, String id, Runnable task) {
        return CompletableFuture.runAsync(new MonitoredRunnable(id, task), executor);
    }

    private static ExecutorService pool(String name) {
        return Executors.newFixedThreadPool(1, Thread.ofPlatform().name(name, 0).factory());
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
