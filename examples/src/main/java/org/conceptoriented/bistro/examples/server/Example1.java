package org.conceptoriented.bistro.examples.server;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.server.*;
import org.conceptoriented.bistro.server.actions.ActionEval;
import org.conceptoriented.bistro.server.actions.ActionRemove;
import org.conceptoriented.bistro.server.connectors.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;

/**
 * Feed events to the engine by computing several moving averages and firing alerts when they have a certain relationship.
 */
public class Example1
{

    public static String location = "src/main/resources/ds4";

    public static Schema schema;

    public static void main(String[] args) throws IOException {

        //
        // Create schema
        //
        schema = new Schema("Example 1");

        //
        // Create tables
        //
        Table quotes = schema.createTable("T");

        //
        // Create columns
        //

        //Column time = schema.createColumn("Time", quotes);
        Column price = schema.createColumn("Price", quotes);
        Column amount = schema.createColumn("Amount", quotes);

        // Weighted price: Price * Amount
        Column weigtedPrice = schema.createColumn("WeightedPrice", quotes);
        weigtedPrice.calc(
                p -> Double.valueOf((String)p[0]),
                price
        );

        // Moving average of the weighted price
        Column avg10 = schema.createColumn("Avg10", quotes);
        avg10.setDefaultValue(0.0); // It will be used as an initial value
        avg10.roll(
                10, 0,
                (a,d,p) -> (double)a + ((double)p[0] / 10.0),
                weigtedPrice
        );

        // Moving average of the weighted price
        Column avg50 = schema.createColumn("Avg50", quotes);
        avg50.setDefaultValue(0.0); // It will be used as an initial value
        avg50.roll(
                50, 0,
                (a,d,p) -> (double)a + ((double)p[0] / 50.0),
                weigtedPrice
        );

        //
        // Create server and connectors
        //
        Server server = new Server(schema);

        // Feed data into the schema
        ConnectorSimulatorFile inSimulator = null;
        double acceleration = 1.0/(31*24*60); // 46549 for 1 month which we want to play for 1 minute, whic is approximately 775 events per second
        try {
            inSimulator = new ConnectorSimulatorFile(server, quotes, location +"/.krakenEUR.csv", "Time", acceleration);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        inSimulator.setConverter( x -> Instant.ofEpochSecond(Long.valueOf(x)) );

        inSimulator.addAction(new ActionRemove(quotes, 100));

        inSimulator.addAction(new ActionEval(schema));

        // Detect some condition and print
        Action myAction = new MyAction(quotes, avg10, avg50);
        inSimulator.addAction(myAction);

        // Periodically print current state
        ConnectorTimer outTimer = new ConnectorTimer(server,1000);
        outTimer.addAction(
                x -> {
                    long len = quotes.getIdRange().end;
                    System.out.print("." + len);
                }
        );

        //
        // Start the server
        //

        try {
            server.start();
            inSimulator.start();
            outTimer.start();
        } catch (BistroError bistroError) {
            bistroError.printStackTrace();
        }
        System.out.println("Server started.");

        try {
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // All connectors have to be stopped.
        try {
            outTimer.stop();
            inSimulator.stop();
            server.stop();
        } catch (BistroError bistroError) {
            bistroError.printStackTrace();
        }
        System.out.println("");
        System.out.println("Server stopped.");
    }

}

// TODO: Alternative: in general, when should we use a custom action and when a custom connector (for outputs)?

class MyAction implements Action {

    long lastEnd = 0;

    Table table;
    Column column1;
    Column column2;

    @Override
    public void eval(Context context) throws BistroError {

        long last = table.getIdRange().end - 1;

        for( ; lastEnd<=last; lastEnd++) {

            if((double)this.column1.getValue(last) < 0.9965*(double)this.column2.getValue(last)) {
                System.out.print("x");
            }

        }
    }

    public MyAction(Table table, Column column1, Column column2) {
        this.table = table;
        this.column1 = column1;
        this.column2 = column2;
    }
}
