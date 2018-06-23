package org.conceptoriented.bistro.server;

import org.conceptoriented.bistro.core.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public class Server {

    final Logger logger = LoggerFactory.getLogger(this.getClass());

    Schema schema;
    public Schema getSchema() {
        return this.schema;
    }
    public void setSchema(Schema schema) {
        this.schema = schema;
    }

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

        this.logger.info("Bistro Server started.");
    }

    public void stop() throws BistroError {
        if(this.executor != null) {
            try {
                this.executor.shutdown();
                this.executor.awaitTermination(5, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                this.logger.error("Tasks interrupted during shutdown of Bistro Server executors.");
            }
            finally {
                if (!executor.isTerminated()) {
                    this.logger.error("Cancel non-finished tasks during stopping Bistro Server.");
                    this.executor.shutdownNow();
                }
                this.executor = null;
            }
        }

        this.queue = null;

        this.logger.info("Bistro Server stopped.");
    }

    public void submit(Task task) {

        long submitTime = System.currentTimeMillis(); // The time when the connector was added to the queue

        this.executor.submit(task); // Add to the queue where it will wait for the next free worker thread

    }

    public void submit(Action action, Context context) {
        this.submit(new Task(action, context));
    }

    public void submit(Action action) {
        this.submit(new Task(action, null));
    }

    @Override
    public String toString() {
        return "Bistro Server";
    }

    public Server(Schema schema) {
        this.schema = schema;

        this.logger.info("Bistro Server created.");
    }
}

