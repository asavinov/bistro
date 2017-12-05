package org.conceptoriented.bistro.core;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ProdTests {

    @BeforeClass
    public static void setUpClass() {
    }

    @Before
    public void setUp() {
    }

    @Test
    public void prodTest() {
        Schema s = createSchema();
        Table t3 = s.getTable("T3");
        Column c31 = t3.getColumn("C31");
        Column c32 = t3.getColumn("C32");

        c31.key(); // First key (2 records)
        c32.key(); // Second key (3 records)

        t3.prod();

        s.eval();

        // Check correctness of dependencies
        List<Element> t3_deps = t3.getDependencies();
        assertTrue(t3_deps.contains(c31.getOutput()));
        assertTrue(t3_deps.contains(c32.getOutput()));

        assertEquals(6, t3.getLength());

        // c31 = {111222}
        assertEquals(1L, c31.getValue(0));
        assertEquals(2L, c31.getValue(4));

        // c32 = {121212}
        assertEquals(1L, c32.getValue(0));
        assertEquals(1L, c32.getValue(3));
    }

    Schema createSchema() {
        // Create and configure: schema, tables, keyColumns
        Schema s = new Schema("My Schema");

        //
        // Table 1 (first domain)
        //
        Table t1 = s.createTable("T1");

        Column c11 = s.createColumn("C11", t1);

        // Add two arbitrary records
        t1.add(3);
        t1.remove();
        c11.setValue(1, "v1");
        c11.setValue(2, "v2");

        //
        // Table 2 (second domain)
        //
        Table t2 = s.createTable("T2");

        Column c21 = s.createColumn("C21", t2);

        // Add three arbitrary records
        t2.add(4);
        t2.remove();
        c21.setValue(1, "v1");
        c21.setValue(2, "v2");
        c21.setValue(3, "v3");

        //
        // Table 3 (product of the two domain tables)
        //
        Table t3 = s.createTable("T3");

        Column t31 = s.createColumn("C31", t3, t1);
        Column t32 = s.createColumn("C32", t3, t2);

        return s;
    }

}
