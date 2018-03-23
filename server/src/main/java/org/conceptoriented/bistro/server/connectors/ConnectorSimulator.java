package org.conceptoriented.bistro.server.connectors;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.server.*;
import org.conceptoriented.bistro.server.actions.TaskAdd;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Use it to simulate an asynchronous data source using data provided as an array in the constructor.
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

    delay = Duration.between(this.delays.get(start), this.delays.get(end)).toMillis();
    */

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

            // Send records to the server
            for( ; start < end; start++) {

                Map<Column, Object> record = new HashMap<>();
                Object[] rec = this.data.get(start);

                for(int i=0; i<columns.size(); i++) {

                    if(i >= rec.length) break; // More columns than data fields

                    record.put(columns.get(i), rec[i]);
                }

                this.server.submit(new TaskAdd(this.table, record));
            }
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
