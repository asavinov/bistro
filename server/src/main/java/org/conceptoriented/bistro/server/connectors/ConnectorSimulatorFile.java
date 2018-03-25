package org.conceptoriented.bistro.server.connectors;

import org.conceptoriented.bistro.core.BistroError;
import org.conceptoriented.bistro.core.BistroErrorCode;
import org.conceptoriented.bistro.core.Table;
import org.conceptoriented.bistro.server.Server;

import java.io.File;
import java.io.FileNotFoundException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;

/**
 * Use data from CSV file to feed into the table.
 */
public class ConnectorSimulatorFile extends ConnectorSimulator {

    protected String path;
    protected String timestampColumn;
    protected double accelerate = 1.0;

    protected Function<String,Instant> converter = x -> Instant.parse(x);
    public void setConverter(Function<String,Instant> converter) {
        // For example to convert seconds: this.setConverter( x -> Instant.ofEpochSecond(Long.valueOf(x)) );
        this.converter = converter;
    }
    protected Instant convert(String time) {
        return this.converter.apply(time);
    }

    @Override
    public void start() throws BistroError {
        // Load data from CSV file into the base class data structures
        try {
            this.load(this.path, this.timestampColumn);
        } catch (FileNotFoundException e) {
            throw new BistroError(BistroErrorCode.CONNECTOR_ERROR, "Error loading file.", e.getMessage());
        }

        super.start();
    }

    @Override
    public void stop() throws BistroError {
        super.stop();

        this.data = null;
        this.delays = null;
    }

    protected void load(String path, String timestampColumn) throws FileNotFoundException {

        //
        // Load data
        //
        List<String[]> lines = readLinesFromCsvFile(path);

        //
        // Load timestamp column and parse dates
        //
        String[] columnNames = lines.get(0);
        int tsColumnIndex = Arrays.asList(columnNames).indexOf(timestampColumn);

        List<Object[]> data = new ArrayList<>();
        List<Instant> timestamps = new ArrayList<>();
        for(int i=1; i<lines.size(); i++) {
            String[] line = lines.get(i);
            Instant ts = null;
            List<Object> dataLine = new ArrayList<>();

            for(int f=0; f<line.length; f++) {
                String val = line[f];
                if(f == tsColumnIndex) {
                    ts = this.convert(val);
                }
                else {
                    dataLine.add(val);
                }
            }

            timestamps.add(ts);
            data.add(dataLine.toArray());
        }

        //
        // Compute delays using accelerator factor
        //
        List<Duration> delays = new ArrayList<>();
        for(int i=1; i<timestamps.size(); i++) {
            Duration del = Duration.between(timestamps.get(i-1), timestamps.get(i));
            long nanos = Math.round( (double)del.toNanos() * this.accelerate );
            delays.add(Duration.ofNanos(nanos));
        }
        delays.add(Duration.ofMillis(0));

        //
        // Set fields of the parent object
        //
        this.delays = delays;
        this.data = data;
    }

    private static List<String[]> readLinesFromCsvFile(String path) throws FileNotFoundException {
        List<String[]> lines = new ArrayList<>();
        Scanner scanner = new Scanner(new File(path));
        while (scanner.hasNextLine())
        {
            String line = scanner.nextLine();
            String[] fields = line.split(",");
            Arrays.parallelSetAll(fields, i -> fields[i].trim());
            lines.add(fields);
        }
        scanner.close();
        return lines;
    }

    public ConnectorSimulatorFile(Server server, Table table, String path, String timestampColumn, double accelerate) throws FileNotFoundException {
        super(server, table, (Duration)null, null);

        this.path = path;
        this.timestampColumn = timestampColumn;
        this.accelerate = accelerate;
    }
}
