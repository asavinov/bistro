package org.conceptoriented.bistro.server;

import org.conceptoriented.bistro.core.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public class Server {

    Schema schema;
    ReentrantLock schemaLock = new ReentrantLock();

    protected List<BistroError> errors = new ArrayList<>();
    public List<BistroError> getErrors() {
        return this.errors;
    }
    public void addError(BistroError error) {
        this.errors.add(error);
    }

    //
    // Executor service
    //

    BlockingQueue<Runnable> queue;
    ExecutorService executor;

    public void start() throws BistroError {

        this.errors.clear();

        //
        // Start executor service
        //

        int n = 1;
        queue = new ArrayBlockingQueue<>(1000);
        this.executor = new ThreadPoolExecutor(n, n, 0L, TimeUnit.MILLISECONDS, queue);
        //this.executor = Executors.newSingleThreadExecutor();
    }

    public void stop() throws BistroError {
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

    public void submit(Task task) {

        long submitTime = System.currentTimeMillis(); // The time when the connector was added to the queue

        this.executor.submit(task); // Add to the queue where it will wait for the next free worker thread

    }

    public void submit(Action action, Context context) {
        this.submit(new Task(action, context));
    }

    @Override
    public String toString() {
        return "Server";
    }

    public Server(Schema schema) {
        this.schema = schema;
    }
}

