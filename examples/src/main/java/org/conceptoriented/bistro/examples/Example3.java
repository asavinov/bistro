package org.conceptoriented.bistro.examples;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.formula.*;

import java.io.*;

public class Example3
{

    public static String location = "src/main/resources/ex3";

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

        Table items = ExUtils.readFromCsv(schema, location, "OrderDetails.csv");

        Table products = ExUtils.readFromCsv(schema, location, "Products.csv");

        Table orders = ExUtils.readFromCsv(schema, location, "Orders.csv");

        Table categories = ExUtils.readFromCsv(schema, location, "Categories.csv");

        //
        // Calculate amount
        //

        // [OrderDetails].[Amount] = [Quantity] * [UnitPrice]
        Column itemsAmount = schema.createColumn("Amount", items, columnType);
        itemsAmount.calc(
                new FormulaExp4j("[Quantity] * [UnitPrice]", items)
        );

        //
        // Links from OrderItems to Products and Orders
        //

        // [OrderDetails].[ProductID]: OrderDetails -> Products
        Column itemsProduct = schema.createColumn("Product", items, products);
        itemsProduct.link(
                new Column[] { products.getColumn("ProductID") },
                new FormulaExp4j("[ProductID]", items)
        );

        // [OrderDetails].[OrderID]: OrderDetails -> Orders
        Column itemsOrder = schema.createColumn("Order", items, orders);
        itemsOrder.link(
                new Column[] { orders.getColumn("OrderID") },
                new FormulaExp4j("[OrderID]", items)
        );

        //
        // Accumulate item characteristics
        //

        // [Products].[Total Amount] = SUM [OrderDetails].[Amount]
        Column productsAmount = schema.createColumn("Total Amount", products, columnType);
        productsAmount.setDefaultValue(0.0); // It will be used as an initial value
        productsAmount.accu(
                itemsProduct,
                new FormulaExp4j("[out] + [Amount]", items)
        );

        // [Order].[Total Amount] = SUM [OrderDetails].[Amount]
        Column ordersAmount = schema.createColumn("Total Amount", orders, columnType);
        ordersAmount.setDefaultValue(0.0); // It will be used as an initial value
        ordersAmount.accu(
                itemsOrder,
                new FormulaExp4j("[out] + [Amount]", items)
        );

        schema.eval();
    }

}
