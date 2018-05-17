package org.conceptoriented.bistro.examples.server;

import org.conceptoriented.bistro.core.BistroError;
import org.conceptoriented.bistro.core.Column;
import org.conceptoriented.bistro.core.Schema;
import org.conceptoriented.bistro.core.Table;
import org.conceptoriented.bistro.server.Action;
import org.conceptoriented.bistro.server.ConnectorBase;
import org.conceptoriented.bistro.server.Server;
import org.conceptoriented.bistro.server.Task;
import org.conceptoriented.bistro.server.actions.ActionAdd;
import org.conceptoriented.bistro.server.actions.ActionEval;
import org.conceptoriented.bistro.server.actions.ActionRemove;
import org.conceptoriented.bistro.server.connectors.ConnectorTimer;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Hello Bistro Streams.
 * Create server and feed it with constant events.
 */
public class Example4 {

    public static Schema schema;

    public static void main(String[] args) throws IOException, BistroError {

        // Time while the server will process events (after that it will stop so it has to be enough for all events).
        long serverProcessingTime = 10000;

        //
        // Create schema
        //
        Schema schema = new Schema("My Schema");

        Table table = schema.createTable("EVENTS");
        Column column1 = schema.createColumn("Message", table);
        Column column2 = schema.createColumn("Temperature", table);

        //
        // Create and start server
        //
        Server server = new Server(schema);
        server.start();
        System.out.println("Server started.");

        //
        // Create and start connector
        //

        ConnectorTimer outTimer = new ConnectorTimer(server,500);

        outTimer.addAction(new ActionEval(schema)); // Evaluate

        //
        // Start the server
        //

        try {
            server.start();
            outTimer.start();
            System.out.println("Server started.");

            Thread.sleep(serverProcessingTime);

            outTimer.stop();
            server.stop();
            System.out.println("Server stopped.");

        } catch (BistroError bistroError) {
            bistroError.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}


// Based on the source code from this project: https://github.com/ashokc/Kafka-Streams-Catching-Data-in-the-Act
class ProducerProcess extends ConnectorBase implements Runnable {

//	Y = A * sin (w*t).	w: angular velocity. radinans/second
//	angularV = 2.0 * Math.PI / 60.0 ;	It will take 60secs i.e. 1 minute to trace the full circle. => period = 1 min

    //private static final Logger logger = LogManager.getLogger(ProducerProcess.class);
    String[] topics ;
    boolean sync ;
    String clientId; //Properties producerProps ;
    long sleepTimeMillis ;

    private double amplitude, angularV, error, xReference, yReference ;

    private Random random = new Random() ;
    //private Producer<String, RawVertex> producer ;
    //private int partitionNumber ;
    //Elastic elastic = new Elastic() ;

    ProducerProcess (Server server, String[] topics, /*boolean sync,*/ String clientId, /*Properties producerProps,*/ long sleepTimeMillis, double amplitude, double angularV, double error, double xReference, double yReference) {
        super(server);

        this.topics = topics ; // args[6]
        //this.sync = sync; // args[5]
        this.clientId = clientId; //this.producerProps = producerProps ;
        this.sleepTimeMillis = sleepTimeMillis ; // args[7]

        this.amplitude = amplitude ; // args[8]
        // args[9]
        this.angularV = angularV * 2.0 * Math.PI / 60.0 ;			// one revolution a minute
        this.error = error ; // args[10]
        this.xReference = xReference ; // args[11]
        this.yReference = yReference ;

        //producer = new KafkaProducer<String, RawVertex>(producerProps); // args[12]
    }

    @Override
    public void run() {
        //String clientId = producerProps.getProperty("client.id") ;
        //if (clientId.equals("A")) {
        //    partitionNumber = 0 ;
        //}
        //else if (clientId.equals("B")) {
        //    partitionNumber = 1 ;
        //}
        //else if (clientId.equals("C")) {
        //    partitionNumber = 2 ;
        //}
        long timeStart = System.currentTimeMillis() ; // current time in milliseconds
        long timePrev = timeStart ;
        double valX = xReference ;
        double valY = yReference ;
        try {
            //logger.info ("-------------- Kafka Producer Start:" + producerProps.getProperty("client.id") + " ----------------") ;
            while ( !(Thread.currentThread().isInterrupted()) ) {
                for (String topic: topics) {
                    //String key = clientId ;
                    long currentTime = System.currentTimeMillis() ;
                    double rand = -error + random.nextDouble() * 2.0 * error ;
                    valX = valX + amplitude * Math.sin(angularV * (currentTime - timePrev) * 0.001) * rand ;
                    valY = valY + amplitude * Math.cos(angularV * (currentTime - timePrev) * 0.001) * rand ;
                    //RawVertex rawVertex = new RawVertex (clientId, currentTime, valX, valY) ;
                    //ProducerRecord<String, RawVertex> record = new ProducerRecord<>(topic, partitionNumber, key, rawVertex) ;
                    //if (sync) {
                    //    RecordMetadata metadata = producer.send(record).get();
                    //}
                    //else {
                    //    producer.send(record, new KafkaCallback(producerProps.getProperty("acks"))) ;
                    //}

                    this.submit(valX, valY); // Create and submit a record with coordinates

                    timePrev = currentTime ;
                }
                Thread.sleep(sleepTimeMillis) ;
            }
        }
        catch (InterruptedException e) {
            //logger.error ("Producer " + producerProps.getProperty("client.id") + " Was Interrupted", e) ;
        }
        catch (Exception e) {
            //logger.error ("Producer " + producerProps.getProperty("client.id") + " ran into some errors", e) ;
        }
        finally {
            //producer.close();
        }
    }

    @Override
    public void start() throws BistroError {
        this.columns = this.table.getColumns();
        this.thread = new Thread(this, "ProducerProcess Thread");
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

    Thread thread;
    Table table;
    List<Column> columns;

    private void submit(double valX, double valY) {
        Map<Column, Object> record = new HashMap<>();
        // First, insert timestamp
        record.put(this.columns.get(0), Instant.now());
        record.put(this.columns.get(1), valX);
        record.put(this.columns.get(2), valY);

        Action action = new ActionAdd(this.table, record);
        Task task = new Task(action, null);
        this.server.submit(task);
    }
}
