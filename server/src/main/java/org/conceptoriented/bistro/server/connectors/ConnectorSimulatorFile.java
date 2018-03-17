package org.conceptoriented.bistro.server.connectors;

import org.conceptoriented.bistro.core.BistroError;
import org.conceptoriented.bistro.core.Column;
import org.conceptoriented.bistro.core.Table;
import org.conceptoriented.bistro.server.Connector;
import org.conceptoriented.bistro.server.Server;
import org.conceptoriented.bistro.server.actions.TaskAdd;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Use data from CSV file to feed into the table.
 */
public class ConnectorSimulatorFile extends ConnectorSimulator {

    protected double accelerate = 1.0;

    protected Function<String,Instant> converter;
    public void setTimeConverter(Function<String,Instant> converter) {
        this.converter = converter;
    }
    protected Instant convert(String time) {
        return this.converter.apply(time);
    }

    protected void load(String file) {

        //
        // Load timestamp column and parse dates
        //

        //
        // If there is timestamp column, then load it and use it to compute delays
        // Delays are differences between rows
        // Use acceleration factor
        //
        Duration del = Duration.between(Instant.now(), Instant.now());
        List<Duration> delays;

        Column timestampColumn; // We might want to store timestamp in the data (e.g., to do rolling aggregation using time windows)

        // Load data columns and convert them to in-memory data
        List<Object[]> data;

    }

    public ConnectorSimulatorFile(Server server, Table table, String path) {
        super(server, table, (Duration)null, null);

        // Load data from CSV file into the base class data structures
        this.load(path);
    }
}
