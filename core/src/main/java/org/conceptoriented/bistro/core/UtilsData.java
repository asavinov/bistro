package org.conceptoriented.bistro.core;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class UtilsData {
    // Values:
    // We assume that input data has the correct type corresonding to the column declared data type. If not then it must be converted before use. Otherwise the behavior is not determined.
    // We can think of and provide in future two versions of methods: safe and unsafe. Safe method do all the necessary checks (and maybe conversions in the case of no ambiguity). Unsafe methods assume that all these schecks have been performed before.
    // For example, if column is not nullable then for unsafe methods, it is the task of the user to guarantee that, while safe methods will check values for null before use.

    //
    // Primitive data types
    //

    public static boolean isInt32(String[] values) {
        if(values == null) return false;

        for (String val : values)
        {
            if(val == null) continue; // assumption: null is supposed to be a valid number
            try {
                int intValue = Integer.parseInt((String) val);
            }
            catch(Exception e) {
                return false;
            }
        }
        return true;
    }

    public static boolean isDouble(String[] values) {
        if(values == null) return false;

        for (String val : values)
        {
            if(val == null) continue; // assumption: null is supposed to be a valid number
            try {
                double doubleValue = Double.parseDouble((String) val);
            }
            catch(Exception e) {
                return false;
            }
        }
        return true;
    }

    public static int toInt32(Object val) {
        if(val == null) {
            return 0;
        }
        else if (val instanceof Integer) {
            return ((Integer) val).intValue();
        }
        else if (val instanceof Double) {
            return ((Double) val).intValue();
        }
        else if (val instanceof Boolean) {
            return ((Boolean) val) == true ? 1 : 0;
        }
        else if (val instanceof String) {
            return Integer.parseInt((String) val);
        }
        else {
            String toString = val.toString();
            if (toString.matches("-?\\d+"))
            {
                return Integer.parseInt(toString);
            }
            throw new IllegalArgumentException("This Object doesn't represent an int");
        }
    }

    public static double toDouble(Object val) {
        if(val == null) {
            return 0.0;
        }
        else if (val instanceof Integer) {
            return ((Integer) val).doubleValue();
        }
        else if (val instanceof Double) {
            return ((Double) val).doubleValue();
        }
        else if (val instanceof Boolean) {
            return ((Boolean) val) == true ? 1.0 : 0.0;
        }
        else if (val instanceof String) {
            return Double.parseDouble((String) val);
        }
        else {
            String toString = val.toString();
            if (toString.matches("-?\\d+"))
            {
                return Double.parseDouble(toString);
            }
            throw new IllegalArgumentException("This Object doesn't represent a double");
        }
    }

    public static BigDecimal toDecimal(Object val) {
        if(val == null) {
            return null;
        }
        else if (val instanceof BigDecimal) {
            return (BigDecimal)val;
        }
        else {
            return new BigDecimal(val.toString());
        }
    }

    public static boolean toBoolean(Object val) {
        if(val == null) {
            return false;
        }
        if (val instanceof Integer) {
            return ((Integer) val) == 0 ? false : true;
        }
        else if (val instanceof Double) {
            return ((Double) val) == 0.0 ? false : true;
        }
        else if (val instanceof Boolean) {
            return ((Boolean) val).booleanValue();
        }
        else if (val instanceof String) {
            return ((String) val).equals("0") || ((String) val).equals("false") ? false : true;
        }
        else {
            throw new IllegalArgumentException("This Object doesn't represent a boolean");
        }
    }

    public static Instant toDateTime(Object val) {
        if(val == null) {
            return null;
        }
        else if (val instanceof Instant) {
            return ((Instant) val);
        }
        else {
            return Instant.parse(val.toString());
        }
    }


    //
    // Comparing numeric objects of different types
    //

    // Cast to some basic type and then create something common like double
    //if( ((Number) recordValue).doubleValue() != ((Number) columnValue).doubleValue() ) { found = false; break; }


    public static List<String> recommendTypes(List<String> columnNames, List<Record> records) {
        // Try to find most appropriate data type for each column
        // Data types are recommended by trying to convert their values and choosing conversion that works best
        // There might be other approaches, for example, by using schema mappings but they can be implemented in other methods

        List<String> types = new ArrayList<String>();

        // For each column, scan sample values and try to determine the best (working) type
        for(String name : columnNames) {
            // Collect sample values
            List<String> values = new ArrayList<String>();
            int recordNumber = 0;
            for(Record rec : records) {
                values.add(rec.get(name).toString());
                if(recordNumber > 10) break;
                recordNumber++;
            }

            // Determine type
            String typeName;

            if ( UtilsData.isInt32( values.toArray(new String[values.size()]) ) ) {
                //typeName = "Integer";
                typeName = "Double";
            }
            else if ( UtilsData.isDouble( values.toArray(new String[values.size()]) ) ) {
                typeName = "Double";
            }
            else {
                typeName = "String";
            }

            types.add(typeName);
        }

        return types;
    }

}
