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

    public void start() throws BistroError {

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
        for(Action a : this.actions) {
            a.start();
        }

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

    public void submit(Action action) {

        long submitTime = System.currentTimeMillis(); // The time when the action was added to the queue

        this.executor.submit(new ActionSequence(this,action)); // Add to the queue where it will wait for the next free worker thread

    }

    //
    // Registration
    //

    List<Action> actions = new ArrayList<>();
    public void addAction(Action action) {
        this.actions.add(action);
    }

    public void removeAction(Action action) {
        this.actions.remove(action);
    }

    @Override
    public String toString() {
        return "Server";
    }

    public Server(Schema schema) {
        this.schema = schema;
    }
}

