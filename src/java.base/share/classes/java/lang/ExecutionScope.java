package java.lang;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Executor;

/**
 * Experimental scoped execution of tasks on another scheduler in the current
 * virtual thread. The same virtual thread executes the task synchronously,
 * preserving its thread-local values and scoped-value bindings.
 *
 * <p> This proof-of-concept API delegates migration and restoration of the previous
 * scheduler to the virtual-thread implementation.
 */
public final class ExecutionScope {
    /**
     * needs stay private
     */
    private ExecutionScope() {
    }

    /**
     * Executes the task using the given executor in the current virtual thread,
     * then attempts to restore the previous scheduler before returning.
     * The executor must run submitted tasks on platform threads and remain
     * available while used by the virtual thread. This method does not close it.
     * A suitable executor can be created with
     * {@link Executors#newSchedulerForVirtualThread(int, String)}.
     *
     * @param executor the executor to use as the scheduler
     * @param task the task
     * @param <T> the result type
     * @return the result of the task
     * @throws NullPointerException if executor or task is null
     * @throws IllegalStateException if the current thread is not a virtual thread
     *         or migration cannot be performed
     * @throws Exception if thrown by the task
     */
    public static <T> T call(ExecutorService executor, Callable<T> task) throws Exception {
        Objects.requireNonNull(executor);
        Objects.requireNonNull(task);
        if (Thread.currentThread() instanceof VirtualThread vthread) {
            return vthread.runWithContext(executor, task);
        } else {
            throw new IllegalStateException(
                    "ExecutionScope.run must be invoked by a virtual thread");
        }
    }

    /**
     * Executes the task using the given executor in the current virtual thread,
     * then attempts to restore the previous scheduler before returning.
     * The executor must run submitted tasks on platform threads and remain
     * available while used by the virtual thread. This method does not close it.
     * A suitable executor can be created with
     * {@link Executors#newSchedulerForVirtualThread(int, String)}.
     *
     * @param executor the executor to use as the scheduler
     * @param task the task
     * @throws NullPointerException if executor or task is null
     * @throws IllegalStateException if the current thread is not a virtual thread
     *         or migration cannot be performed
     * @throws Exception if thrown by the task
     */
    public static void run(ExecutorService executor, Runnable task) throws Exception {
        Objects.requireNonNull(executor);
        Objects.requireNonNull(task);
        if (Thread.currentThread() instanceof VirtualThread vthread) {
            vthread.runWithContext(executor, Executors.callable(task, (Void) null));
        } else {
            throw new IllegalStateException(
                    "ExecutionScope.run must be invoked by a virtual thread");
        }
    }
}
