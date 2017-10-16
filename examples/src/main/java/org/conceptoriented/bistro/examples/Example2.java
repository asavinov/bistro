package org.conceptoriented.bistro.examples;

import org.conceptoriented.bistro.core.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Example2 {

    public static String location = "src/main/resources/ex2";

    public static Schema schema;

	public static void main(String[] args) throws IOException {

        //
        // Create schema
        //

        schema = new Schema("Ex1");

        //
        // Create tables and columns by loading data from CSV files
        //

        //Double doubleObject = new Double("aaa");
        //double number = Double.valueOf("nnn");
        double number2 = Double.parseDouble("ccc");

        Table products = ExUtils.readFromCsv(schema, location, "Products.csv");
        Table orderItems = ExUtils.readFromCsv(schema, location, "OrderItems.csv");

    }

}
