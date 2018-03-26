package org.conceptoriented.bistro.server.connectors;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.server.*;
import org.conceptoriented.bistro.server.actions.*;

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

        List<Column> columns = this.table.getColumns();

        while(start < this.data.size()) {

            // Find next data
            for( ; end < this.data.size(); end++) {
                if(this.delays == null) {
                    delay_millis = this.delay.toMillis();
                }
                else {
                    delay_millis = this.delays.get(end).toMillis();
                }
                if(delay_millis > 0) { end++; break; }
            }

            // Sleep some time (duration between this and next)
            try { Thread.sleep(delay_millis); }
            catch (InterruptedException e) {
                if(Thread.currentThread().isInterrupted()) {
                    break;
                }
            }

            // Timestamp to be inserted
            Instant now = Instant.now();

            // Create records to be added and create one action for each of them
            List<Action> actions = new ArrayList<>();
            for( ; start < end; start++) {

                Map<Column, Object> record = new HashMap<>();

                // First, insert timestamp
                record.put(columns.get(0), now);

                // Then, insert all data fields
                Object[] rec = this.data.get(start);
                for(int i=0; i<rec.length; i++) {

                    if(i >= columns.size()) break; // More values than columns

                    record.put(columns.get(i+1), rec[i]);
                }

                actions.add(new ActionAdd(this.table, record));
            }

            // Add follow up actions at the end
            actions.addAll(this.getActions());
            Task task = new Task(actions, null);
            this.server.submit(task);
        }

    }

    @Override
    public void start() throws BistroError {
        this.thread = new Thread(this, "Bistro ConnectorSimulator Thread");
        this.thread.start();
    }

    @Override
    public void stop() throws BistroError {
        // Stop streaming
        if(this.thread != null) {
            this.thread.interrupt();
            this.thread = null;
        }
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
