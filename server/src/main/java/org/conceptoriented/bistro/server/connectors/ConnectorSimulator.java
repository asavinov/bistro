package org.conceptoriented.bistro.server.connectors;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.server.*;
import org.conceptoriented.bistro.server.actions.TaskAdd;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Use it to simulate an asynchronous data source.
 */
public class ConnectorSimulator extends Connector implements Runnable {

    /* Move to file reader
    protected int timeColumn;
    public void setTimeColumn(int timeColumn) {
        this.timeColumn = timeColumn;
    }

    protected Function<String,Instant> converter;
    public void setTimeConverter(Function<String,Instant> converter) {
        this.converter = converter;
    }

    protected Instant convert(String time) {
        return this.converter.apply(time);
    }
    */

    Table table;
    List<Instant> timestamps;
    List<Object[]> data;

    Thread thread;

    @Override
    public void run() {

        long delay = 100;
        int start = 0;
        int end = 0;

        List<Column> columns = this.table.getColumns();

        while(start < this.data.size()) {

            // Find next data
            while(end < this.data.size()) {
                delay = Duration.between(this.timestamps.get(start), this.timestamps.get(end)).toMillis();
                if(delay > 0) break;
                end++;
            }

            // Sleep some time (duration between this and next)
            try { Thread.sleep(delay); }
            catch (InterruptedException e) {
                if(Thread.currentThread().isInterrupted()) {
                    break;
                }
            }

            // Send records to the server
            for( ; start < end; start++) {

                Map<Column, Object> record = new HashMap<>();

                for(int i=0; i<columns.size(); i++) {
                    Column col = columns.get(i);
                    Object val = null;
                    if(i == 0) {
                        val = this.timestamps.get(start);
                    }
                    else {
                        Object[] rec = this.data.get(start);
                        if(i-1 >= rec.length) break; // More columns than data fields
                        val = rec[i-1];
                    }

                    record.put(col, val);
                }

                this.server.submit(new TaskAdd(this.table, record));
            }
        }

    }

    @Override
    public void start() throws BistroError {
        this.thread = new Thread(this, "Bistro Server: ConnectorSimulator Thread");
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

    public ConnectorSimulator(Server server, Table table, List<Instant> timestamps, List<Object[]> data) {
        super(server);

        this.table = table;
        this.timestamps = timestamps;
        this.data = data;
    }
}
