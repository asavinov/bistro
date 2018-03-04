package org.conceptoriented.bistro.server;

import org.conceptoriented.bistro.core.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Server {

    Schema schema;

    ReentrantLock schemaLock = new ReentrantLock();

    //
    // Executor service
    //

    BlockingQueue<Runnable> queue;
    ExecutorService executor;

    public void start() {

        //
        // Start executor service
        //

        int n = 1;
        queue = new ArrayBlockingQueue<>(1000);
        this.executor = new ThreadPoolExecutor(n, n, 0L, TimeUnit.MILLISECONDS, queue);
        //this.executor = Executors.newSingleThreadExecutor();

        //
        // Start all registered actions
        //

    }

    public void stop() {
        if(this.executor != null) {
            try {
                System.out.println("Attempt to shutdown executor");
                this.executor.shutdown();
                this.executor.awaitTermination(5, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                System.err.println("tasks interrupted");
            }
            finally {
                if (!executor.isTerminated()) {
                    System.err.println("Cancel non-finished tasks");
                    this.executor.shutdownNow();
                }
                this.executor = null;
            }
        }

        this.queue = null;
    }

    public void submit(ActionSequence sequence) {

        // Store in the action time when it was added to the queue
        long submitTime = System.currentTimeMillis();

        // When the action really starts executing (in another thread), we need to store time (of retrieving from the queue)

        this.executor.submit(sequence);


    }

    public void submit(Action action) { // Convenience method
        this.submit(new ActionSequence(action));
    }

    //
    // Registration
    //

    public void addAction() {
    }

    public void removeAction() {
    }

    @Override
    public String toString() {
        return "Server";
    }

    public Server(Schema schema) {
        this.schema = schema;
    }
}

