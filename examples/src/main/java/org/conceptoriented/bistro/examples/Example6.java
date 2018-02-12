package org.conceptoriented.bistro.examples;

import org.conceptoriented.bistro.core.Column;
import org.conceptoriented.bistro.core.Schema;
import org.conceptoriented.bistro.core.Table;

import java.io.IOException;

public class Example6 {

    public static String location = "src/main/resources/ds3";

    public static Schema schema;

    public static void main(String[] args) throws IOException {

        //
        // Create schema
        //

        schema = new Schema("Example 6");

        //
        // Create tables and columns by loading data from CSV files
        //

        Table columnType = schema.getTable("Object");

        Table quotes = ExUtils.readFromCsv(schema, location, "BTC-EUR.csv");

        //
        // Calculate daily (relative) price span (in percent)
        //

        // [Quotes].[Span] = ([High] - [Low]) / [Close]
        Column span = schema.createColumn("Span", quotes, columnType);
        span.calc(
                p -> 100.0 * (Double.valueOf((String)p[0]) - Double.valueOf((String)p[1])) / Double.valueOf((String)p[2]),
                quotes.getColumn("High"), quotes.getColumn("Low"), quotes.getColumn("Close")
        );

        //
        // Weekly aggregated price span
        //

        // [Quotes].[Span7] = ROLL_SUM_(7 days) [Quotes].[Span]
        Column span7 = schema.createColumn("Span7", quotes, columnType);
        span7.setDefaultValue(0.0); // It will be used as an initial value
        span7.roll(
                7, 0,
                (a,d,p) -> (double)a + (double)p[0], // [out] + [Span]
                span
        );

        // [Quotes].[Span7] = ROLL_SUM_(7 days exp) [Quotes].[Span]
        Column span7exp = schema.createColumn("Span7exp", quotes, columnType);
        span7exp.setDefaultValue(0.0); // It will be used as an initial value
        span7exp.roll(
                7, 0,
                (a,d,p) -> (double)a + ((double)p[0] / Math.exp(d)), // [out] + ([Span] / e^d)
                span
        );

        //
        // Evaluate and read values
        //

        schema.eval();

        Object value;

        value = span.getValue(0); // value = 1.9550342130987395
        value = span.getValue(1); // value = 1.8682399213372616
        value = span.getValue(6); // value = 1.860920666013707
        value = span.getValue(7); // value = 2.514506769825934
        value = span.getValue(99); // value = 24.29761718053468
        if(Math.abs((double)value - 24.29761718053468) > 1e-10) System.out.println("UNEXPECTED RESULT.");

        value = span7.getValue(0); // value = 1.9550342130987395
        value = span7.getValue(1); // value = 3.823274134436001
        value = span7.getValue(6); // value = 11.75773858746599
        value = span7.getValue(7); // value = 14.272245357291924
        value = span7.getValue(99); // value = 129.26354771247534
        if(Math.abs((double)value - 129.26354771247534) > 1e-10) System.out.println("UNEXPECTED RESULT.");

        value = span7exp.getValue(0); // value = 1.9550342130987395
        value = span7exp.getValue(1); // value = 2.587456815123076
        value = span7exp.getValue(6); // value = 2.576851527984925
        value = span7exp.getValue(7); // value = 3.4624774699228054
        value = span7exp.getValue(99); // value = 32.69604034898669
        if(Math.abs((double)value - 32.69604034898669) > 1e-10) System.out.println("UNEXPECTED RESULT.");
    }

}
