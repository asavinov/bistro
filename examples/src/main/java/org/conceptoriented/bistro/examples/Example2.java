package org.conceptoriented.bistro.examples;

import org.conceptoriented.bistro.core.*;

import java.io.*;

public class Example2 {

    public static String location = "src/main/resources/ex2";

    public static Schema schema;

	public static void main(String[] args) throws IOException {

        //
        // Create schema
        //

        schema = new Schema("Example 2");

        //
        // Create tables and columns by loading data from CSV files
        //

        Table columnType = schema.getTable("Object");

        Table items = ExUtils.readFromCsv(schema, location, "OrderItems.csv");

        Table products = ExUtils.readFromCsv(schema, location, "Products.csv");

        Table orders = schema.createTable("Orders");
        schema.createColumn("ID", orders, columnType);

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
        // Links from OrderItems to Products and Orders
        //

        // [OrderItems].[Product]: OrderItems -> Products
        Column itemsProduct = schema.createColumn("Product", items, products);
        itemsProduct.link(
                new Column[] { products.getColumn("ID") },
                items.getColumn("Product ID")
        );

        // [OrderItems].[Order]: OrderItems -> Orders
        Column itemsOrder = schema.createColumn("Order", items, orders);
        itemsOrder.link(
                new Column[] { orders.getColumn("ID") },
                items.getColumn("Order ID")
        );

        //
        // Accumulate item characteristics
        //

        // [Products].[Total Amount] = SUM [OrderItems].[Amount]
        Column productsAmount = schema.createColumn("Total Amount", products, columnType);
        productsAmount.setDefaultValue(0.0); // It will be used as an initial value
        productsAmount.accu(
                itemsProduct,
                p -> (double)p[0] + (double)p[1], // [Amount] + [out]
                items.getColumn("Amount")
        );

        // [Order].[Total Amount] = SUM [OrderItems].[Amount]
        Column ordersAmount = schema.createColumn("Total Amount", orders, columnType);
        ordersAmount.setDefaultValue(0.0); // It will be used as an initial value
        ordersAmount.accu(
                itemsOrder,
                p -> (double)p[0] + (double)p[1], // [Amount] + [out]
                items.getColumn("Amount")
        );

        schema.eval();
    }

}
