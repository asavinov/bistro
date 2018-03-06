package org.conceptoriented.bistro.examples.core;

import java.io.IOException;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.examples.*;

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

        // We need it for exponential moving average (it is a sum of all coefficients used as weights for computing average in a window)
        double sumExpWeights = 0.0;
        for(int i=0; i<7; i++) sumExpWeights += (1 / Math.exp(i));
        final double sumExpWeightsFinal = sumExpWeights; // To use in lambda we need final

        //
        // Calculate daily (relative) price span (in percent)
        //

        // [Quotes].[Span] = ([High] - [Low]) / [Close]
        Column spanDaily = schema.createColumn("Span", quotes, columnType);
        spanDaily.calc(
                p -> 100.0 * (Double.valueOf((String)p[0]) - Double.valueOf((String)p[1])) / Double.valueOf((String)p[2]),
                quotes.getColumn("High"), quotes.getColumn("Low"), quotes.getColumn("Close")
        );

        //
        // Weekly aggregated price span
        //

        // [Quotes].[SpanWeekly] = ROLL_SUM_(7 days) [Quotes].[Span]
        Column spanWeekly = schema.createColumn("SpanWeekly", quotes, columnType);
        spanWeekly.setDefaultValue(0.0); // It will be used as an initial value
        spanWeekly.roll(
                7, 0,
                (a,d,p) -> (double)a + ((double)p[0] / 7.0), // [out] + [Span]. Equal weights for all 7 constituents
                spanDaily
        );

        // [Quotes].[spanWeeklyExp] = ROLL_SUM_(7 days exp) [Quotes].[Span]
        Column spanWeeklyExp = schema.createColumn("SpanWeeklyExp", quotes, columnType);
        spanWeeklyExp.setDefaultValue(0.0); // It will be used as an initial value
        spanWeeklyExp.roll(
                7, 0,
                (a,d,p) -> (double)a + ((double)p[0] / Math.exp(d)) / sumExpWeightsFinal, // [out] + ([Span] / e^d).
                spanDaily
        );

        //
        // Evaluate and read values
        //

        schema.eval();

        Object value;

        value = spanDaily.getValue(0); // value = 1.9550342130987395
        value = spanDaily.getValue(1); // value = 1.8682399213372616
        value = spanDaily.getValue(6); // value = 1.860920666013707
        value = spanDaily.getValue(7); // value = 2.514506769825934
        value = spanDaily.getValue(99); // value = 24.29761718053468
        if(Math.abs((double)value - 24.29761718053468) > 1e-10) System.out.println(">>> UNEXPECTED RESULT.");

        value = spanWeekly.getValue(0); // value = 0.2792906018712485
        value = spanWeekly.getValue(1); // value = 0.546182019205143
        value = spanWeekly.getValue(6); // value = 1.6796769410665702
        value = spanWeekly.getValue(7); // value = 1.7596015920275978
        value = spanWeekly.getValue(99); // value = 16.36032122029923
        if(Math.abs((double)value - 16.36032122029923) > 1e-10) System.out.println(">>> UNEXPECTED RESULT.");

        value = spanWeeklyExp.getValue(0); // value = 1.23694526739464
        value = spanWeeklyExp.getValue(1); // value = 1.6370774693408667
        value = spanWeeklyExp.getValue(6); // value = 1.6303675306364669
        value = spanWeeklyExp.getValue(7); // value = 2.1895729057377022
        value = spanWeeklyExp.getValue(99); // value = 20.678198203000097
        if(Math.abs((double)value - 20.678198203000097) > 1e-10) System.out.println(">>> UNEXPECTED RESULT.");
    }

}
