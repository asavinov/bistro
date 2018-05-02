package org.conceptoriented.bistro.examples.core;

import java.io.IOException;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.examples.*;

public class Example4 {

    public static String location = "src/main/resources/ds1";

    public static Schema schema;

    public static void main(String[] args) throws IOException {

        //
        // Create schema
        //

        schema = new Schema("Example 4");

        //
        // Create tables and columns by loading data from CSV files
        //

        Table items = ExUtils.readFromCsv(schema, location, "OrderItems.csv");

        Table products = ExUtils.readFromCsv(schema, location, "Products.csv");



        Table categories = schema.createTable("Categories");
        categories.product(); // This table will be populated by using data from other tables

        Column categoriesName = schema.createColumn("Name", categories);
        categoriesName.noop(true); // Key columns specify where the data for this table comes from

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
        // Link from OrderItems to Products
        //

        // [OrderItems].[Product]: OrderItems -> Products
        Column itemsProduct = schema.createColumn("Product", items, products);
        itemsProduct.link(
                new Column[] { items.getColumn("Product ID") },
                products.getColumn("ID")
        );

        // [Products].[Cat]: Products -> Categories
        Column productsCategory = schema.createColumn("Cat", products, categories);
        productsCategory.project(
                new Column[] { products.getColumn("Category") },
                categories.getColumn("Name") // Only key columns can be specified here
        );

        //
        // Accumulate item characteristics
        //

        // [Products].[Total Amount] = SUM [OrderItems].[Amount]
        Column productsAmount = schema.createColumn("Total Amount", products);
        productsAmount.setDefaultValue(0.0); // It will be used as an initial value
        productsAmount.accumulate(
                itemsProduct,
                (a,p) -> (double)p[0] + (double)a, // [Amount] + [out]
                null,
                items.getColumn("Amount")
        );

        // [Categories].[Total Amount] = SUM [Products].[Total Amount]
        Column categoriesAmount = schema.createColumn("Total Amount", categories);
        categoriesAmount.setDefaultValue(0.0); // It will be used as an initial value
        categoriesAmount.accumulate(
                productsCategory,
                (a,p) -> (double)p[0] + (double)a, // [Amount] + [out]
                null,
                products.getColumn("Total Amount")
        );

        //
        // Evaluate and read values
        //

        schema.evaluate();

        Object value;

        value = itemsAmount.getValue(32); // value = 533.75 = 25 * 21.35
        if(Math.abs((double)value - 533.75) > 1e-10) System.out.println(">>> UNEXPECTED RESULT.");
        value = itemsProduct.getValue(32); // value = 3
        if(((Number)value).longValue() != 3) System.out.println(">>> UNEXPECTED RESULT.");

        value = productsAmount.getValue(3); // value = 533.75 * 1 item
        if(Math.abs((double)value - 533.75) > 1e-10) System.out.println(">>> UNEXPECTED RESULT.");
        value = productsCategory.getValue(3); // value = 2
        if(((Number)value).longValue() != 2) System.out.println(">>> UNEXPECTED RESULT.");

        value = categoriesAmount.getValue(2); // value = 533.75 * 1 product
        if(Math.abs((double)value - 533.75) > 1e-10) System.out.println(">>> UNEXPECTED RESULT.");
        value = categoriesName.getValue(2); // value = "Oil"
        if(!((String)value).equals("Oil")) System.out.println(">>> UNEXPECTED RESULT.");
    }

}
