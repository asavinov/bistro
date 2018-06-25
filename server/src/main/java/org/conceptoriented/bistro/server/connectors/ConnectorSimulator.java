package org.conceptoriented.bistro.server.connectors;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.server.*;
import org.conceptoriented.bistro.server.actions.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The class simulates an asynchronous data source by feeding the data into the specified table.
 * The input data is provided as a list of records.
 * The data is appended to the table using the specified list of delays.
 */
public class ConnectorSimulator extends ConnectorBase implements Runnable {

    final Logger logger = LoggerFactory.getLogger(this.getClass());

    Table table;
    Duration delay;
    List<Duration> delays;
    List<Object[]> data;

    Thread thread;

    @Override
    public void run() {

        long delay_millis = 100;
        int start = 0;
        int end = 0;
        long submissionTime = System.nanoTime();

        List<Column> columns = this.table.getColumns();

        while(start < this.data.size()) {

            //
            // Find next data
            //
            for( ; end < this.data.size(); end++) {
                if(this.delays == null) {
                    delay_millis = this.delay.toMillis();
                }
                else {
                    delay_millis = this.delays.get(end).toMillis();
                }
                if(delay_millis > 0) { end++; break; }
            }

            //
            // Create records to be added
            //
            List<Action> toSubmit = new ArrayList<>();
            for( ; start < end; start++) {

                Map<Column, Object> record = new HashMap<>();

                // First, insert timestamp
                record.put(columns.get(0), Instant.now());

                // Then, insert all data fields
                Object[] rec = this.data.get(start);
                for(int i=0; i<rec.length; i++) {

                    if(i >= columns.size()) break; // More values than columns

                    record.put(columns.get(i+1), rec[i]);
                }

                toSubmit.add(new ActionAdd(this.table, record));
            }

            toSubmit.addAll(this.getActions());
            Task task = new Task(toSubmit, null);

            //
            // Sleep some time by waking up when the event has to be submitted
            //
            long restNanos = delay_millis * 1000000 - (System.nanoTime() - submissionTime);
            if(restNanos > 10) {
                try {
                    Thread.sleep(restNanos / 1000000, (int)(restNanos % 1000000));
                } catch (InterruptedException e) {
                    if (Thread.currentThread().isInterrupted()) {
                        break;
                    }
                }
            }

            submissionTime = System.nanoTime();
            this.server.submit(task);
        }

    }

    @Override
    public void start() {
        this.thread = new Thread(this, "Bistro ConnectorSimulator Thread");
        this.thread.start();

        this.logger.info("Bistro ConnectorSimulator started.");
    }

    @Override
    public void stop() {
        // Stop streaming
        if(this.thread != null) {
            this.thread.interrupt();
            this.thread = null;
        }

        this.logger.info("Bistro ConnectorSimulator stopped.");
    }

    public ConnectorSimulator(Server server, Table table, Duration delay, List<Object[]> data) {
        super(server);

        this.table = table;
        this.delay = delay;
        this.delays = null;
        this.data = data;
    }

    public ConnectorSimulator(Server server, Table table, List<Duration> delays, List<Object[]> data) {
        super(server);

        this.table = table;
        this.delay = null;
        this.delays = delays;
        this.data = data;
    }
}
