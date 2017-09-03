package org.conceptoriented.bistro.core;

import org.apache.commons.lang3.StringUtils;

public class UtilsNames {

    public static boolean isNullOrEmpty(String param) {
        return param == null || param.trim().length() == 0;
    }

    //
    // Name validity
    //

    public static boolean validElementName(String name)
    {
        if (name == null) return false;
        if (UtilsNames.isNullOrEmpty(name)) return false;
        
        char[] validCharacters = {'_', '-'};
        for(char c : validCharacters) {
            name = name.replace(c, ' '); // Replace by space (which is valid)
        }
        
        return StringUtils.isAlphanumericSpace(name);
    }

    //
    // Name equality
    //

    public static boolean sameElementName(String n1, String n2)
    {
        if (n1 == null || n2 == null) return false;
        if (UtilsNames.isNullOrEmpty(n1) || UtilsNames.isNullOrEmpty(n2)) return false;
        return n1.equalsIgnoreCase(n2);
    }

    public static boolean sameSchemaName(String n1, String n2)
    {
        return sameElementName(n1, n2);
    }

    public static boolean sameTableName(String n1, String n2)
    {
        return sameElementName(n1, n2);
    }

    public static boolean sameColumnName(String n1, String n2)
    {
        return sameTableName(n1, n2);
    }

}
