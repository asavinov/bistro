package org.conceptoriented.bistro.core;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RollTests {

    @BeforeClass
    public static void setUpClass() {
    }

    @Before
    public void setUp() {
    }

    @Test
    public void rollRowsTest() {
        Schema s = this.createSchema();
        Table t = s.getTable("F");

        // Measure to be accumulate
        Column t_m = t.getColumn("M");

        // Create roll column
        Column t_r = s.createColumn("R", t);
        t_r.getData().setDefaultValue(0.0);

        // Lambda for rolling accumulation " [out] + [M] / (distance + 1) "
        t_r.roll(
                2, 0, // distance in (2,0]
                (a,d,p) -> (Double)a + ((Double)p[0] / (d + 1)),
                t_m
        );
        t_r.evaluate();

        assertEquals(1.0, t_r.getData().getValue(0));
        assertEquals(2.5, t_r.getData().getValue(1));
        assertEquals(4.0, t_r.getData().getValue(2));
        assertEquals(3.5, t_r.getData().getValue(3));
        assertEquals(2.0, t_r.getData().getValue(4));
    }

    @Test
    public void rollColumnTest() {
        Schema s = this.createSchema();
        Table t = s.getTable("F");

        // Measure to be accumulate
        Column t_m = t.getColumn("M");

        // Distance column
        Column t_d = t.getColumn("D");

        // Create roll column
        Column t_r = s.createColumn("R", t);
        t_r.getData().setDefaultValue(0.0);

        // Lambda for rolling accumulation " [out] + [M] / (distance + 1) "
        t_r.roll(
                t_d,
                1000, 0, // (1000,0] 1000 milliseconds back in the past (exclusive)
                (a,d,p) -> (Double)a + (Double)p[0],
                t_m
        );
        t_r.evaluate();

        assertEquals(1.0, t_r.getData().getValue(0));
        assertEquals(2.0, t_r.getData().getValue(1));
        assertEquals(5.0, t_r.getData().getValue(2));
        assertEquals(7.0, t_r.getData().getValue(3));
        assertEquals(6.0, t_r.getData().getValue(4));
    }

    Schema createSchema() {

        Schema schema = new Schema("My Schema");

        //
        // Table (fact table)
        //
        Table tf = schema.createTable("F");
        tf.getData().add();
        tf.getData().add();
        tf.getData().add();
        tf.getData().add();
        tf.getData().add();

        // Create measure to be aggregated
        Column tf_m = schema.createColumn("M", tf);
        tf_m.getData().setValue(0, 1.0);
        tf_m.getData().setValue(1, 2.0);
        tf_m.getData().setValue(2, 3.0);
        tf_m.getData().setValue(3, 2.0);
        tf_m.getData().setValue(4, 1.0);

        // Create column to be used as a distance between rows, e.g., storing timestamp as milliseconds
        Column tf_d = schema.createColumn("D", tf);
        tf_d.getData().setValue(0, 1514761200000L); // 2018-01-01 00:00:00
        tf_d.getData().setValue(1, 1514761200000L + 1000L); // 1 second later
        tf_d.getData().setValue(2, 1514761200000L + 1100L);
        tf_d.getData().setValue(3, 1514761200000L + 1200L);
        tf_d.getData().setValue(4, 1514761200000L + 2000L); // 2 seconds later

        return schema;
    }

}
