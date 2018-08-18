package org.conceptoriented.bistro.examples.core;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.examples.*;

public class Example7 {

    public static String location = "src/main/resources/ds4";

    public static Schema schema;

    public static void main(String[] args) throws IOException {

        //
        // Create schema
        //

        schema = new Schema("Example 7");

        //
        // Create tables and columns by loading data from CSV files
        //

        Table quotes = ExUtils.readFromCsv(schema, location, ".krakenEUR.csv");

        //
        // Convert time (in seconds) from string to long
        //

        Column timestamp = schema.createColumn("Timestamp", quotes);
        timestamp.calculate(
                p -> Instant.ofEpochSecond(Long.valueOf((String)p[0]).longValue()),
                quotes.getColumn("Time")
        );

        //
        // Range table with 1 hour intervals
        //

        Table hourlyQuotes = schema.createTable("Hourly Quotes");

        Column hourColumn = schema.createColumn("Hour", hourlyQuotes);
        hourColumn.noop(true);

        Column intervalColumn = schema.createColumn("Interval", hourlyQuotes);
        intervalColumn.noop(true);

        hourlyQuotes.range(
                Instant.parse("2015-12-01T00:00:10.00Z"),
                Duration.ofSeconds(3600),
                10000L
        );

        //
        // Link to range
        //

        Column timestamp2hour = schema.createColumn("Link To Hour", quotes, hourlyQuotes);
        timestamp2hour.project(
                timestamp // Timestamp will be mapped to hourly intervals
        );

        //
        // Accumulate for each hour
        //

        Column priceVolumeSum = schema.createColumn("Price Volume Sum", hourlyQuotes);
        priceVolumeSum.getData().setDefaultValue(0.0); // It will be used as an initial value
        priceVolumeSum.accumulate(
                timestamp2hour,
                (a,p) -> Double.valueOf((String)p[0]) * Double.valueOf((String)p[1]) + (double)a, // [Price] * [Amount] + [out]
                null,
                quotes.getColumn("Price"), quotes.getColumn("Amount")
        );

        Column volumeSum = schema.createColumn("Volumne Sum", hourlyQuotes);
        volumeSum.getData().setDefaultValue(0.0); // It will be used as an initial value
        volumeSum.accumulate(
                timestamp2hour, // Time stamp
                (a,p) -> Double.valueOf((String)p[0]) + (double)a, // [Amount] + [out]
                null,
                quotes.getColumn("Amount")
        );

        Column VWAP = schema.createColumn("VWAP", hourlyQuotes);
        VWAP.calculate(
                p -> (double)p[0] / (double)p[1],
                priceVolumeSum, volumeSum
        );

        //
        // Evaluate and read values
        //

        schema.evaluate();

        Object value;

        value = hourlyQuotes.getLength();
        if(((Number)value).longValue() != 288) System.out.println(">>> UNEXPECTED RESULT.");

        value = timestamp2hour.getData().getValue(20);
        if(((Number)value).longValue() != 0) System.out.println(">>> UNEXPECTED RESULT.");
        value = timestamp2hour.getData().getValue(21);
        if(((Number)value).longValue() != 1) System.out.println(">>> UNEXPECTED RESULT.");

        value = volumeSum.getData().getValue(0);
        if(Math.abs((double)value - 7.0947694) > 1e-10) System.out.println(">>> UNEXPECTED RESULT.");

        value = priceVolumeSum.getData().getValue(3);
        if(Math.abs((double)value - 48473.09601907268) > 1e-10) System.out.println(">>> UNEXPECTED RESULT.");

        value = VWAP.getData().getValue(5);
        if(Math.abs((double)value - 400.61719044972426) > 1e-10) System.out.println(">>> UNEXPECTED RESULT.");
    }
}
