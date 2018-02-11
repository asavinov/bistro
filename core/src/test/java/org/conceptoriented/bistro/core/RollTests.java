package org.conceptoriented.bistro.core;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

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

        // Accu (group) formula
        Column t_m = t.getColumn("M");

        // Create roll column
        Column t_r = s.createColumn("R", t);
        t_r.setDefaultValue(0.0);

        // Lambda for rolling accumulation " [out] + [M] / (distance + 1) "
        t_r.roll(
                1, 1,
                (a,d,p) -> (Double)a + ((Double)p[0] / (d + 1)),
                t_m
        );
        t_r.eval();

        assertEquals(2.0, t_r.getValue(0));
        assertEquals(4.0, t_r.getValue(1));
        assertEquals(5.0, t_r.getValue(2));
        assertEquals(4.0, t_r.getValue(3));
        assertEquals(2.0, t_r.getValue(4));
    }

    @Test
    public void rollColumnTest() {

    }

    Schema createSchema() {

        Schema schema = new Schema("My Schema");

        //
        // Table (fact table)
        //
        Table tf = schema.createTable("F");
        tf.add();
        tf.add();
        tf.add();
        tf.add();
        tf.add();

        // Create measure to be aggregated
        Column tf_m = schema.createColumn("M", tf);
        tf_m.setValue(0, 1.0);
        tf_m.setValue(1, 2.0);
        tf_m.setValue(2, 3.0);
        tf_m.setValue(3, 2.0);
        tf_m.setValue(4, 1.0);

        return schema;
    }

}
