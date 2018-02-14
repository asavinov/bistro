package org.conceptoriented.bistro.examples;

import org.conceptoriented.bistro.core.Column;
import org.conceptoriented.bistro.core.Schema;
import org.conceptoriented.bistro.core.Table;

import java.io.IOException;
import java.util.ArrayList;

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

        Table columnType = schema.getTable("Object");

        Table quotes = ExUtils.readFromCsv(schema, location, ".krakenEUR.csv");

        //
        // Convert time (in seconds) from string to long
        //

        Column time_seconds = schema.createColumn("Time Seconds", quotes, columnType);
        time_seconds.calc(
                p -> Long.valueOf((String)p[0]).longValue(),
                quotes.getColumn("Time")
        );

        //
        // Volume Weighted Average Price - VWAP = SUM(Price*Volume) / SUM(Volume)
        //

        Column volumeSum = schema.createColumn("VolumneSum", quotes, columnType);
        volumeSum.setDefaultValue(0.0); // It will be used as an initial value
        volumeSum.roll(
                time_seconds, // Time stamp
                60, 0, // 3600 seconds moving average
                (a,d,p) -> (double)a + Double.valueOf((String)p[0]), // [out] + [Amount]
                quotes.getColumn("Amount")
        );

        Column priceSum = schema.createColumn("PriceSum", quotes, columnType);
        priceSum.setDefaultValue(0.0); // It will be used as an initial value
        priceSum.roll(
                time_seconds, // Time stamp
                60, 0, // 3600 seconds moving average
                (a,d,p) -> (double)a + (Double.valueOf((String)p[0]) * Double.valueOf((String)p[1])), // [out] + [Price] * [Amount]
                quotes.getColumn("Price"), quotes.getColumn("Amount")
        );

        Column VWAP = schema.createColumn("VWAP", quotes, columnType);
        VWAP.calc(
                p -> (double)p[0] / (double)p[1],
                priceSum, volumeSum
        );

        //
        // Evaluate and read values
        //

        schema.eval();

        Object value;

        value = volumeSum.getValue(3); // value = 0,54029262 (3 elements including this one)
        if(Math.abs((double)value - 0.54029262) > 1e-10) System.out.println("UNEXPECTED RESULT.");

        value = priceSum.getValue(3); // value = 214,430001602
        if(Math.abs((double)value - 214.430001602) > 1e-10) System.out.println("UNEXPECTED RESULT.");

        value = VWAP.getValue(3); // value = 396,87753203440017374288769667074
        if(Math.abs((double)value - 396.87753203440017374288769667074) > 1e-10) System.out.println("UNEXPECTED RESULT.");
    }

}
