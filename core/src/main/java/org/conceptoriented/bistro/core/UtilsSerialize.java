package org.conceptoriented.bistro.core;

import com.google.common.base.CharMatcher;
import com.google.common.io.Files;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.conceptoriented.bistro.core.expr.ColumnDefinitionCalc;
import org.conceptoriented.bistro.core.expr.ColumnDefinitionAccu;
import org.conceptoriented.bistro.core.expr.ColumnDefinitionLink;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.*;

public class UtilsSerialize {

    //
    // Serialization
    //

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

    //
    // Schema creation and serialization (migrated from Schema)
    //

    public static String schemaToJson() {
        Schema schema = null; // Previous this

        // Trick to avoid backslashing double quotes: use backticks and then replace it at the end
        String jid = "`id`: `" + schema.getId() + "`";
        String jname = "`name`: `" + schema.getName() + "`";

        String json = jid + ", " + jname;

        return ("{" + json + "}").replace('`', '"');
    }

    public static Schema fromJson(String json) throws BistroError {
        JSONObject obj = new JSONObject(json);

        // Extract all necessary parameters

        String id = obj.getString("id");

        String name = obj.getString("name");
        if(!UtilsNames.validElementName(name)) {
            throw new BistroError(BistroErrorCode.UPATE_ELEMENT, "Error updating schema. ", "Name contains invalid characters. ");
        }

        //
        // Create
        //

        Schema schema = new Schema(name);
        return schema;
    }

    public static void updateFromJson(String json) throws BistroError {
        Schema schema = null; // Previous this

        JSONObject obj = new JSONObject(json);

        //
        // Extract parameters and check validity
        //

        String id = obj.getString("id");

        if(obj.has("name")) {
            String name = obj.getString("name");
            if(!UtilsNames.validElementName(name)) {
                throw new BistroError(BistroErrorCode.UPATE_ELEMENT, "Error updating column. ", "Name contains invalid characters. ");
            }
        }

        long afterAppend = obj.has("afterAppend") && !obj.isNull("afterAppend") ? obj.getLong("afterAppend") : -1;

        //
        // Update the properties
        //

        if(obj.has("name")) schema.setName(obj.getString("name"));
    }

    //
    // Table creation and serialization (migrated from Schema)
    //

    public static Table createTableFromJson(String json) throws BistroError {
        Schema schema = null; // Previous this

        JSONObject obj = new JSONObject(json);

        //
        // Validate properties
        //
        String id = obj.getString("id");
        String name = obj.getString("name");

        Table tab = schema.getTable(name);
        if(tab != null) {
            throw new BistroError(BistroErrorCode.CREATE_ELEMENT, "Error creating table. ", "Name already exists. ");
        }
        if(!UtilsNames.validElementName(name)) {
            throw new BistroError(BistroErrorCode.CREATE_ELEMENT, "Error creating table. ", "Name contains invalid characters. ");
        }

        //
        // Create
        //

        tab = schema.createTable(name);

        return tab;
    }

    public static Table createFromCsvFile(String fileName, boolean hasHeaderRecord) {
        Schema schema = null; // Previous this

        String tableName = null;
        File file = new File(fileName);
        tableName = file.getName();
        tableName = Files.getNameWithoutExtension(tableName);

        // Read column names from CSV
        List<String> colNames = UtilsSerialize.readColumnNamesFromCsvFile(fileName);

        // Read Records from CSV
        List<Record> records = Record.fromCsvFile(fileName, colNames, true);

        // Get column types
        List<String> colTypes = UtilsData.recommendTypes(colNames, records);

        // Create/add table with file name
        Table tab = schema.createTable(tableName);

        // Append columns
        schema.createColumns(tab.getName(), colNames, colTypes);

        // Append records to this table
        Record.add(tab, records, null);

        return tab;
    }

    public static Table createFromCsvLines(String tableName, String csvLines, String params) {
        Schema schema = null; // Previous this

        if(params == null || params.isEmpty()) params = "{}";

        JSONObject paramsObj = new JSONObject(params);
        List<String> lines = new ArrayList<String>(Arrays.asList(csvLines.split("\\r?\\n")));

        // Read column names from CSV
        String headerLine = lines.get(0);
        List<String> colNames = UtilsSerialize.csvLineToList(headerLine, paramsObj);

        // Read Records from CSV
        List<Record> records = Record.fromCsvList(csvLines, params);

        // Create/add table with file name
        Table tab = schema.createTable(tableName);

        // Append columns if necessary
        if(paramsObj.optBoolean("createColumns")) {
            List<String> colTypes = UtilsData.recommendTypes(colNames, records);
            schema.createColumns(tab.getName(), colNames, colTypes);
        }

        // Append records to this table
        Record.add(tab, records, null);

        return tab;
    }

    public static void updateTableFromJson(String json) throws BistroError {
        Schema schema = null; // Previous this

        JSONObject obj = new JSONObject(json);

        // Find table

        String id = obj.getString("id");
        Table tab = schema.getTableById(id);
        if(tab == null) {
            throw new BistroError(BistroErrorCode.UPATE_ELEMENT, "Error updating table. ", "Table not found. ");
        }

        //
        // Validate properties
        //
        if(obj.has("name")) {
            String name = obj.getString("name");
            Table t = schema.getTable(name);
            if(t != null && t != tab) {
                throw new BistroError(BistroErrorCode.UPATE_ELEMENT, "Error updating table. ", "Name already exists. ");
            }
            if(!UtilsNames.validElementName(name)) {
                throw new BistroError(BistroErrorCode.UPATE_ELEMENT, "Error updating table. ", "Name contains invalid characters. ");
            }
        }

        //
        // Update only properties which are present
        //

        if(obj.has("name")) tab.setName(obj.getString("name"));
    }

    public static String tableToJson() {
        Table table = null;

        // Trick to avoid backslashing double quotes: use backticks and then replace it at the end
        String jid = "`id`: `" + table.getId() + "`";
        String jname = "`name`: `" + table.getName() + "`";

        String json = jid + ", " + jname + ", ";

        return ("{" + json + "}").replace('`', '"');
    }

    //
    // Column creation and serialization (migraged from Schema)
    //

    public static Column createColumnFromJson(String json) throws BistroError {
        Schema schema = null; // Previous this

        JSONObject obj = new JSONObject(json);

        // Extract all necessary parameters

        String id = obj.getString("id");

        JSONObject input_table = obj.getJSONObject("input");
        String input_id = input_table.getString("id");
        Table input = schema.getTableById(input_id);

        JSONObject output_table = obj.getJSONObject("output");
        String output_id = output_table.getString("id");
        Table output = schema.getTableById(output_id);

        String name = obj.getString("name");
        Column col = schema.getColumn(input.getName(), name);
        if(col != null) {
            throw new BistroError(BistroErrorCode.CREATE_ELEMENT, "Error creating column. ", "Name already exists. ");
        }
        if(!UtilsNames.validElementName(name)) {
            throw new BistroError(BistroErrorCode.CREATE_ELEMENT, "Error creating column. ", "Name contains invalid characters. ");
        }

        // We do not process status (it is always result of the backend)
        // We do not process dirty (it is always result of the backend)

        ColumnKind kind = obj.has("kind") ? ColumnKind.fromInt(obj.getInt("kind")) : ColumnKind.AUTO;

        String calcFormula = obj.has("calcFormula") && !obj.isNull("calcFormula") ? obj.getString("calcFormula") : "";

        String linkFormula = obj.has("linkFormula") && !obj.isNull("linkFormula") ? obj.getString("linkFormula") : "";

        String initFormula = obj.has("initFormula") && !obj.isNull("initFormula") ? obj.getString("initFormula") : "";
        String accuFormula = obj.has("accuFormula") && !obj.isNull("accuFormula") ? obj.getString("accuFormula") : "";
        String accuTable = obj.has("accuTable") && !obj.isNull("accuTable") ? obj.getString("accuTable") : "";
        String accuPath = obj.has("accuPath") && !obj.isNull("accuPath") ? obj.getString("accuPath") : "";

        //
        // Check validity
        //

        boolean isValid = true;
        if(name == null || name.isEmpty()) isValid = false;
        if(input == null) isValid = false;
        if(output == null) isValid = false;

        // Create

        if(isValid) {
            col = schema.createColumn(input.getName(), name, output.getName());

            col.setKind(kind);

            // Always create a new definition object
            col.setDefinitionCalc(new ColumnDefinitionCalc(calcFormula, col.expressionKind));
            col.setDefinitionLink(new ColumnDefinitionLink(linkFormula, col.expressionKind));
            col.setDefinitionAccu(new ColumnDefinitionAccu(initFormula, accuFormula, null, accuTable, accuPath, col.expressionKind));

            if(!col.isDerived()) { // Columns without formula (non-evalatable) are clean
                col.setFormulaChange(false);
            }

            return col;
        }
        else {
            return null;
        }
    }

    public static void updateColumnFromJson(String json) throws BistroError {
        Schema schema = null; // Previous this

        JSONObject obj = new JSONObject(json);

        // Extract all necessary parameters

        String id = obj.getString("id");
        Column column = schema.getColumnById(id);

        JSONObject input_table = obj.getJSONObject("input");
        String input_id = input_table.getString("id");
        Table input = schema.getTableById(input_id);

        JSONObject output_table = obj.getJSONObject("output");
        String output_id = output_table.getString("id");
        Table output = schema.getTableById(output_id);

        if(obj.has("name")) {
            String name = obj.getString("name");
            Column col = schema.getColumn(column.getInput().getName(), name);
            if(col != null && col != column) {
                throw new BistroError(BistroErrorCode.UPATE_ELEMENT, "Error updating column. ", "Name already exists. ");
            }
            if(!UtilsNames.validElementName(name)) {
                throw new BistroError(BistroErrorCode.UPATE_ELEMENT, "Error updating column. ", "Name contains invalid characters. ");
            }
        }

        // We do not process status (it is always result of the backend)
        // We do not process dirty (it is always result of the backend)

        ColumnKind kind = obj.has("kind") ? ColumnKind.fromInt(obj.getInt("kind")) : ColumnKind.AUTO;

        String calcFormula = obj.has("calcFormula") && !obj.isNull("calcFormula") ? obj.getString("calcFormula") : "";

        String linkFormula = obj.has("linkFormula") && !obj.isNull("linkFormula") ? obj.getString("linkFormula") : "";

        String initFormula = obj.has("initFormula") && !obj.isNull("initFormula") ? obj.getString("initFormula") : "";
        String accuFormula = obj.has("accuFormula") && !obj.isNull("accuFormula") ? obj.getString("accuFormula") : "";
        String accuTable = obj.has("accuTable") && !obj.isNull("accuTable") ? obj.getString("accuTable") : "";
        String accuPath = obj.has("accuPath") && !obj.isNull("accuPath") ? obj.getString("accuPath") : "";

        // Descriptor is either JSON object or JSON string with an object but we want to store a string
        String descr_string = null;
        if(obj.has("descriptor")) {
            Object jdescr = !obj.isNull("descriptor") ? obj.get("descriptor") : "";
            if(jdescr instanceof String) {
                descr_string = (String)jdescr;
            }
            else if(jdescr instanceof JSONObject) {
                descr_string = ((JSONObject) jdescr).toString();
            }
        }

        //
        // Update only the properties which have been provided
        //

        if(obj.has("input")) column.setInput(input);
        if(obj.has("output")) column.setOutput(output);
        if(obj.has("name")) column.setName(obj.getString("name"));

        if(obj.has("kind")) column.setKind(kind);

        // Always create a new definition object
        if(obj.has("calcFormula"))
            column.setDefinitionCalc(new ColumnDefinitionCalc(calcFormula, column.expressionKind));
        if(obj.has("linkFormula"))
            column.setDefinitionLink(new ColumnDefinitionLink(linkFormula, column.expressionKind));
        if(obj.has("initFormula") || obj.has("accuFormula") || obj.has("initTable") || obj.has("initPath"))
            column.setDefinitionAccu(new ColumnDefinitionAccu(initFormula, accuFormula, null, accuTable, accuPath, column.expressionKind));
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

    public static String columnToJson() {
        Column column = null;

        // Trick to avoid backslashing double quotes: use backticks and then replace it at the end
        String jid = "`id`: `" + column.getId() + "`";
        String jname = "`name`: `" + column.getName() + "`";

        String jinid = "`id`: `" + column.getInput().getId() + "`";
        String jin = "`input`: {" + jinid + "}";

        String joutid = "`id`: `" + column.getOutput().getId() + "`";
        String jout = "`output`: {" + joutid + "}";

        String jstatus = "`status`: " + (column.getThisOrDependenceError() != null ? column.getThisOrDependenceError().toJson() : "null");
        String jdirty = "`dirty`: " + (column.isThisOrDependenceDirty() ? "true" : "false"); // We transfer deep dirty including this column

        String jkind = "`kind`:" + column.kind.getValue() + "";

        String jcalc = "`calcFormula`: " + JSONObject.valueToString(column.getDefinitionCalc() == null ? "" : column.getDefinitionCalc().getFormula()) + "";

        String jlink = "`linkFormula`: " + JSONObject.valueToString(column.getDefinitionLink() == null ? "" : column.getDefinitionLink().getFormula()) + "";

        String jinit = "`initFormula`: " + JSONObject.valueToString(column.getDefinitionAccu() == null ? "" : column.getDefinitionAccu().getInitFormula()) + "";
        String jaccu = "`accuFormula`: " + JSONObject.valueToString(column.getDefinitionAccu() == null ? "" : column.getDefinitionAccu().getAccuFormula()) + "";
        String jatbl = "`accuTable`: " + JSONObject.valueToString(column.getDefinitionAccu() == null ? "" : column.getDefinitionAccu().getAccuTable()) + "";
        String japath = "`accuPath`: " + JSONObject.valueToString(column.getDefinitionAccu() == null ? "" : column.getDefinitionAccu().getAccuPath()) + "";

        String json = jid + ", " + jname + ", " + jin + ", " + jout + ", " + jdirty + ", " + jstatus + ", " + jkind + ", " + jcalc + ", " + jlink + ", " + jinit + ", " + jaccu + ", " + jatbl + ", " + japath;

        return ("{" + json + "}").replace('`', '"');
    }

}



class Record {

    // Alternative: Apache Commons CaseInsensitiveMap
    Map<String, Object> fields = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);

    public List<String> getNames() {
        return new ArrayList<String>(fields.keySet());
    }

    public Object get(String name) {
        return fields.get(name);
    }

    public void set(String name, Object value) {
        fields.put(name, value);
    }

    public String toJsonMap() { // Column name is a key. Column value is the key value.
        // Loop over all keys
        String data = "";
        for (Map.Entry<String, Object> entry : fields.entrySet())
        {
            String data_elem = "`" + entry.getKey() + "`:`" + entry.getValue() + "`, ";
            data += data_elem;
        }
        if(data.length() > 2) {
            data = data.substring(0, data.length()-2);
        }

        return ("{" + data + "}").replace('`', '"'); // Trick to avoid backslashing double quotes: use backticks and then replace it at the end
    }
    public String toJsonList(List<String> columns) { // First element in the pair is column name. Second element is column value.
        String data = "";
        // Loop over all columns (to retain their order in the list)
        for(String column : columns) {
            Object value = this.get(column);
            if(value == null) value = "";
            String data_elem = "[`" + column + "`,`" + value.toString() + "`], ";
            data += data_elem;
        }
        if(data.length() > 2) {
            data = data.substring(0, data.length()-2);
        }

        return ("[" + data + "]").replace('`', '"'); // Trick to avoid backslashing double quotes: use backticks and then replace it at the end
    }
    public String toCsv(List<String> columns) { // Comma separated column values.
        String data = "";
        // Loop over all columns (to retain their order in the list)
        for(String column : columns) {
            Object value = this.get(column);
            if(value == null) value = "";
            String data_elem = "`" + value.toString() + "`,";
            data += data_elem;
        }
        if(data.length() > 1) {
            data = data.substring(0, data.length()-1);
        }

        return data.replace('`', '"'); // Trick to avoid backslashing double quotes: use backticks and then replace it at the end
    }

    //
    // Creation methods
    //

    public static Record fromJson(String json) {
        JSONObject obj = new JSONObject(json);
        return fromJsonObject(obj);
    }

    public static Record fromJsonObject(JSONObject obj) {
        Record record = new Record();

        Iterator<?> keys = obj.keys();
        while(keys.hasNext()) {
            String key = (String)keys.next(); // Column name
            Object value = obj.get(key);
            record.set(key, value);
        }
        return record;
    }

    public static List<Record> fromJsonList(String jsonString) {
        List<Record> records = new ArrayList<Record>();

        Object token = new JSONTokener(jsonString).nextValue();
        JSONArray arr;
        if (token instanceof JSONArray) { // Array of records
            arr = (JSONArray) token;
        }
        else { // (token instanceof JSONObject)
            if(!((JSONObject)token).has("data")) { // token is Record object
                records.add(Record.fromJsonObject((JSONObject)token));
                return records;
            }
            else {
                Object content = ((JSONObject)token).get("data");
                if(content instanceof JSONObject) { // content is Record object
                    records.add(Record.fromJsonObject((JSONObject)content));
                    return records;
                }
                else if(content instanceof JSONArray) {
                    arr = (JSONArray) content;
                }
                else {
                    return null;
                }
            }

        }

        // Loop over all list
        for (int i = 0 ; i < arr.length(); i++) {
            JSONObject jrec = arr.getJSONObject(i);
            Record record = Record.fromJsonObject(jrec);
            records.add(record);
        }

        return records;
    }

    public static List<Record> fromCsvList(String csvLines, String params) {
        if(params == null || params.isEmpty()) params = "{}";

        JSONObject paramsObj = new JSONObject(params);
        List<String> lines = new ArrayList<String>(Arrays.asList(csvLines.split("\\r?\\n")));

        //
        // Extract a list of column names from the header (first line)
        //
        String headerLine = lines.get(0);
        List<String> colNames = UtilsSerialize.csvLineToList(headerLine, paramsObj);

        //
        // Add records in the loop over all lines
        //
        List<Record> records = new ArrayList<Record>();
        for (int i=1; i < lines.size(); i++) {
            String line = lines.get(i);
            if(line == null || line.trim().isEmpty()) continue;

            List<String> vals = UtilsSerialize.csvLineToList(line, paramsObj);

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

    public static List<Record> fromTable(Table table, Range range) {
        if(range == null) {
            range = table.getIdRange();
        }

        // Get all outgoing columns
        List<Column> columns = table.getColumns();

        List<Record> records = new ArrayList<Record>();
        for(long row = range.start; row < range.end; row++) {

            Record record = new Record();
            record.set("_id", row);

            for(Column column : columns) {
                // Get value from the record
                Object value = column.getValue(row);
                // Store the value in the record
                record.set(column.getName(), value);
            }

            records.add(record);
        }

        return records;
    }



    public static long addToTable(Table table, Record record) {

        // Get all outgoing columns
        List<Column> columns = table.getColumns();

        for(Column column : columns) { // We must add a new value to all columns even if it has not been provided (null)

            // Get value from the record
            Object value = record.get(column.getName());

            // Append the value to the column (even if it is null). This will also update the dirty status of data (range).
            column.add();
            column.setValue(table.getIdRange().end, value);
        }

        table.getIdRange().end++;
        return table.getIdRange().end - 1;
    }

    public static void add(Table table, List<Record> records, Map<String, String> columnMapping) {
        for(Record record : records) {
            Record.addToTable(table, record);
        }
    }


    public Record() {
    }

}
