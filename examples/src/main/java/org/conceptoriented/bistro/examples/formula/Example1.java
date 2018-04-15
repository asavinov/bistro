package org.conceptoriented.bistro.examples.formula;

import java.io.*;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.formula.*;
import org.conceptoriented.bistro.examples.*;

public class Example1
{

    public static String location = "src/main/resources/ds2";

    public static Schema schema;

    public static void main(String[] args) throws IOException {

        //
        // Create schema
        //

        schema = new Schema("Example 1");

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
        Expression expr = new FormulaExp4j("[Quantity] * [UnitPrice]", items);
        itemsAmount.calc(
                expr.getEvaluator(), expr.getParameterPaths().toArray(new ColumnPath[]{})
        );

        //
        // Links from OrderItems to Products and Orders
        //

        // [OrderDetails].[ProductID]: OrderDetails -> Products
        Column itemsProduct = schema.createColumn("Product", items, products);
        /* Deprecated: link columns do not accept expressions - create explicit columns before
        itemsProduct.link(
                new Expression[] { new FormulaExp4j("[ProductID]", items) },
                new Column[] { products.getColumn("ProductID") }
        );
        */

        // [OrderDetails].[OrderID]: OrderDetails -> Orders
        Column itemsOrder = schema.createColumn("Order", items, orders);
        /* Deprecated: link columns do not accept expressions - create explicit columns before
        itemsOrder.link(
                new Expression[] { new FormulaExp4j("[OrderID]", items) },
                new Column[] { orders.getColumn("OrderID") }
        );
        */

        //
        // Accumulate item characteristics
        //

        // [Products].[Total Amount] = SUM [OrderDetails].[Amount]
        Column productsAmount = schema.createColumn("Total Amount", products, columnType);
        productsAmount.setDefaultValue(0.0); // It will be used as an initial value
        productsAmount.accu(
                itemsProduct,
                (a,p) -> (double)a + (double)p[0], // new FormulaExp4j("[out] + [Amount]", items)
                itemsAmount
        );

        // [Order].[Total Amount] = SUM [OrderDetails].[Amount]
        Column ordersAmount = schema.createColumn("Total Amount", orders, columnType);
        ordersAmount.setDefaultValue(0.0); // It will be used as an initial value
        ordersAmount.accu(
                itemsOrder,
                (a,p) -> (double)a + (double)p[0], // new FormulaExp4j("[out] + [Amount]", items),
                itemsAmount
        );

        schema.evaluate();
    }

}
