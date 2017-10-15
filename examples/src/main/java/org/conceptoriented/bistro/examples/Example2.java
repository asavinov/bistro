package org.conceptoriented.bistro.examples;

import org.conceptoriented.bistro.core.*;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

public class Example2 {

	public static void main(String[] args) {

        //
        // Load data from CSV files
        //

    }



    public Table createFromCsvFile(String fileName, boolean hasHeaderRecord) {
        String tableName = null;
        File file = new File(fileName);
        tableName = file.getName();
        tableName = Files.getNameWithoutExtension(tableName);

        // Read column names from CSV
        List<String> colNames = this.readColumnNamesFromCsvFile(fileName);

        // Read Records from CSV
        List<Record> records = Record.fromCsvFile(fileName, colNames, true);

        // Get column types
        List<String> colTypes = Utils.recommendTypes(colNames, records);

        // Create/append table with file name
        Table tab = this.createTable(tableName);

        // Append columns
        this.createColumns(tab.getName(), colNames, colTypes);

        // Append records to this table
        tab.append(records, null);

        // Auto-evaluation if needed
        this.autoEvaluate();

        return tab;
    }

    public static List<String> readColumnNamesFromCsvFile(String fileName) {
        List<String> columnNames = new ArrayList<String>();

        try {
            File file = new File(fileName);

            Reader in = new FileReader(file);
            Iterable<CSVRecord> csvRecs = CSVFormat.EXCEL.parse(in);

            for (CSVRecord csvRec : csvRecs) {
                for(int i=0; i<csvRec.size(); i++) {
                    columnNames.add(csvRec.get(i));
                }
                break; // Only one record is needed
            }

            in.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        return columnNames;
    }

    public static List<Record> fromCsvList(String csvLines, String params) {
        if(params == null || params.isEmpty()) params = "{}";

        JSONObject paramsObj = new JSONObject(params);
        List<String> lines = new ArrayList<String>(Arrays.asList(csvLines.split("\\r?\\n")));

        //
        // Extract a list of column names from the header (first line)
        //
        String headerLine = lines.get(0);
        List<String> colNames = Utils.csvLineToList(headerLine, paramsObj);

        //
        // Add records in the loop over all lines
        //
        List<Record> records = new ArrayList<Record>();
        for (int i=1; i < lines.size(); i++) {
            String line = lines.get(i);
            if(line == null || line.trim().isEmpty()) continue;

            List<String> vals = Utils.csvLineToList(line, paramsObj);

            Record record = new Record();
            for(int j=0; j<vals.size(); j++) {
                if(j >= colNames.size()) break; // More values than columns
                record.set(colNames.get(j), vals.get(j));
            }

            records.add(record);
        }

        return records;
    }
    public static List<Record> fromCsvFile(String fileName, List<String> columnNames, boolean skipFirst) {
        List<Record> records = new ArrayList<Record>();
        Record record = null;

        try {
            File file = new File(fileName);

            Reader in = new FileReader(file);
            Iterable<CSVRecord> csvRecs = CSVFormat.EXCEL.parse(in);

            int recordNumber = 0;
            for (CSVRecord csvRec : csvRecs) {

                if(skipFirst && recordNumber == 0) { // First record
                    recordNumber++;
                    continue;
                }

                record = new Record();
                for(int i=0; i<csvRec.size(); i++) {
                    Object value = csvRec.get(i);
                    record.set(columnNames.get(i), value);
                }
                records.add(record);

                recordNumber++;
            }

            in.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        return records;
    }

    public static List<String> csvLineToList(String line, JSONObject paramsObj) {
        List<String> ret = new ArrayList<String>();
        String[] fields = line.split(",");
        for(int j=0; j<fields.length; j++) {
            String val = fields[j].trim();
            val = CharMatcher.is('\"').trimFrom(val); // Remove quotes if any
            ret.add(val);
        }
        return ret;
    }

}
