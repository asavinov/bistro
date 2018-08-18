package org.conceptoriented.bistro.core;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ProductTests {

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

        s.evaluate();

        // Check correctness of dependencies
        List<Element> t3_deps = t3.getDependencies();
        assertTrue(t3_deps.contains(c31.getOutput()));
        assertTrue(t3_deps.contains(c32.getOutput()));

        // Result size
        assertEquals(6, t3.getData().getLength());

        // c31 = {111222}
        assertEquals(1L, c31.getData().getValue(0));
        assertEquals(2L, c31.getData().getValue(4));

        // c32 = {123123}
        assertEquals(1L, c32.getData().getValue(0));
        assertEquals(1L, c32.getData().getValue(3));
    }

    @Test
    public void whereTest() {
        Schema s = createSchema();
        Table t3 = s.getTable("T3");
        Column c31 = t3.getColumn("C31");
        Column c32 = t3.getColumn("C32");

        t3.product(
                p -> p[0] == "v1" || p[1] == "v1",
                new ColumnPath(c31, s.getTable("T1").getColumn("C11")), new ColumnPath(c32, s.getTable("T2").getColumn("C21"))
        );

        s.evaluate();

        // Result size
        assertEquals(4, t3.getData().getLength());

        // c31 = {1112}
        assertEquals(1L, c31.getData().getValue(0));
        assertEquals(2L, c31.getData().getValue(3));

        // c32 = {1231}
        assertEquals(2L, c32.getData().getValue(1));
        assertEquals(3L, c32.getData().getValue(2));
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
        t1.getData().add(3);
        t1.getData().remove();
        c11.getData().setValue(1, "v1");
        c11.getData().setValue(2, "v2");

        //
        // Table 2 (second domain)
        //
        Table t2 = s.createTable("T2");

        Column c21 = s.createColumn("C21", t2);

        // Add three arbitrary records
        t2.getData().add(4);
        t2.getData().remove();
        c21.getData().setValue(1, "v1");
        c21.getData().setValue(2, "v2");
        c21.getData().setValue(3, "v3");

        //
        // Table 3 (product of the two domain tables)
        //
        Table t3 = s.createTable("T3");
        t3.product();

        Column c31 = s.createColumn("C31", t3, t1);
        c31.noop(true); // First key (2 records)
        Column c32 = s.createColumn("C32", t3, t2);
        c32.noop(true); // Second key (3 records)

        return s;
    }

    @Test
    public void whereProjTest() {
        Schema s = createSchema2();

        Table t3 = s.getTable("T3");
        Column c31 = t3.getColumn("C31");
        Column c32 = t3.getColumn("C32");

        Table t4 = s.getTable("T4");

        // Projection without product
        s.evaluate();

        // Result size
        assertEquals(3, t3.getData().getLength());

        // c31 = {112}
        assertEquals(1L, c31.getData().getValue(0));
        assertEquals(2L, c31.getData().getValue(2));

        // c32 = {122}
        assertEquals(1L, c32.getData().getValue(0));
        assertEquals(2L, c32.getData().getValue(2));

        // Add filter
        t3.product(
                p -> p[0] == "v1" || p[1] == "v1",
                new ColumnPath(c31, s.getTable("T1").getColumn("C11")),
                new ColumnPath(c32, s.getTable("T2").getColumn("C21"))
        );

        s.evaluate();

        // Result size
        assertEquals(2, t3.getData().getLength());
        Range t3range = t3.getData().getIdRange();

        // c31 = {11}
        assertEquals(1L, c31.getData().getValue(t3range.start));
        assertEquals(1L, c31.getData().getValue(t3range.start+1));

        // c32 = {12}
        assertEquals(1L, c32.getData().getValue(t3range.start));
        assertEquals(2L, c32.getData().getValue(t3range.start+1));
    }

    Schema createSchema2() {
        Schema s = createSchema();
        Table t3 = s.getTable("T3");
        Column c31 = t3.getColumn("C31");
        Column c32 = t3.getColumn("C32");

        //
        // Table 4 (fact table referencing and populating T3)
        //
        Table t4 = s.createTable("T4");
        Column c41 = s.createColumn("C41", t4, s.getTable("T1"));
        Column c42 = s.createColumn("C42", t4, s.getTable("T2"));

        t4.getData().add(4);
        c41.getData().setValue(0, 1L); c42.getData().setValue(0, 1L); // v1, v1
        c41.getData().setValue(1, 1L); c42.getData().setValue(1, 2L); // v1, v2
        c41.getData().setValue(2, 2L); c42.getData().setValue(2, 2L); // v2, v2
        c41.getData().setValue(3, 2L); c42.getData().setValue(3, 2L); // v2, v2

        Column c43 = s.createColumn("C43", t4, t3); // Project column
        c43.project(
                new Column[] {c41, c42},
                c31, c32
        );

        return s;
    }
}
