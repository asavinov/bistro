package org.conceptoriented.bistro.examples;

import org.conceptoriented.bistro.core.Column;
import org.conceptoriented.bistro.core.Schema;
import org.conceptoriented.bistro.core.Table;

import java.io.IOException;

public class Example3 {

    public static String location = "src/main/resources/ex1";

    public static Schema schema;

	public static void main(String[] args) throws IOException {

        //
        // Create schema
        //

        schema = new Schema("Example 3");

        //
        // Create tables and columns by loading data from CSV files
        //

        Table columnType = schema.getTable("Object");

        Table items = ExUtils.readFromCsv(schema, location, "OrderItems.csv");

        Table orders = schema.createTable("Orders");
        orders.prod(); // This table will be populated by using data from other tables

        Column ordersId = schema.createColumn("ID", orders, columnType);
        ordersId.key(); // Key columns specify where the data for this table comes from

        //
        // Calculate amount
        //

        // [OrderItems].[Amount] = [Quantity] * [Unit Price]
        Column itemsAmount = schema.createColumn("Amount", items, columnType);
        itemsAmount.calc(
                p -> Double.valueOf((String)p[0]) * Double.valueOf((String)p[1]),
                items.getColumn("Quantity"), items.getColumn("Unit Price")
        );

        //
        // Link from OrderItems to Orders
        //

        // [OrderItems].[Order]: OrderItems -> Orders
        Column itemsOrder = schema.createColumn("Order", items, orders);
        itemsOrder.proj(
                new Column[] { items.getColumn("Order ID") },
                orders.getColumn("ID") // Only key columns can be specified here
        );

        //
        // Accumulate item characteristics
        //

        // [Order].[Total Amount] = SUM [OrderItems].[Amount]
        Column ordersAmount = schema.createColumn("Total Amount", orders, columnType);
        ordersAmount.setDefaultValue(0.0); // It will be used as an initial value
        ordersAmount.accu(
                itemsOrder,
                p -> (double)p[0] + (double)p[1], // [Amount] + [out]
                items.getColumn("Amount")
        );

        schema.eval();

        //
        // Evaluate and read values
        //

        schema.eval();

        Object value;

        value = ordersAmount.getValue(0); // value = 1505.0
    }

}
