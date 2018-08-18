package org.conceptoriented.bistro.examples.core;

import java.io.IOException;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.examples.*;

public class Example3 {

    public static String location = "src/main/resources/ds1";

    public static Schema schema;

    public static void main(String[] args) throws IOException {

        //
        // Create schema
        //

        schema = new Schema("Example 3");

        //
        // Create tables and columns by loading data from CSV files
        //

        Table items = ExUtils.readFromCsv(schema, location, "OrderItems.csv");

        Table orders = schema.createTable("Orders");
        orders.product(); // This table will be populated by using data from other tables

        Column ordersId = schema.createColumn("ID", orders);
        ordersId.noop(true); // Key columns specify where the data for this table comes from

        //
        // Calculate amount
        //

        // [OrderItems].[Amount] = [Quantity] * [Unit Price]
        Column itemsAmount = schema.createColumn("Amount", items);
        itemsAmount.calculate(
                p -> Double.valueOf((String)p[0]) * Double.valueOf((String)p[1]),
                items.getColumn("Quantity"), items.getColumn("Unit Price")
        );

        //
        // Link from OrderItems to Orders
        //

        // [OrderItems].[Order]: OrderItems -> Orders
        Column itemsOrder = schema.createColumn("Order", items, orders);
        itemsOrder.project(
                new Column[] { items.getColumn("Order ID") },
                orders.getColumn("ID") // Only key columns can be specified here
        );

        //
        // Accumulate item characteristics
        //

        // [Order].[Total Amount] = SUM [OrderItems].[Amount]
        Column ordersAmount = schema.createColumn("Total Amount", orders);
        ordersAmount.getData().setDefaultValue(0.0); // It will be used as an initial value
        ordersAmount.accumulate(
                itemsOrder,
                (a,p) -> (double)p[0] + (double)a, // [Amount] + [out]
                null,
                items.getColumn("Amount")
        );

        schema.evaluate();

        //
        // Evaluate and read values
        //

        schema.evaluate();

        Object value;

        value = ordersAmount.getData().getValue(0); // value = 1505.0
        if(Math.abs((double)value - 1505.0) > 1e-10) System.out.println(">>> UNEXPECTED RESULT.");
    }

}
