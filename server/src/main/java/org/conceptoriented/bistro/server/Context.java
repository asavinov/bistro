package org.conceptoriented.bistro.server;

import org.conceptoriented.bistro.core.*;

/**
 This class contains parameters for data processing operations and it is also supposed to contain output results.
 */
public class Context {
    public Connector connector;
    public Server server;
    public Schema schema;
    public Table table;
    public Column column;
    public Object[] values;
    public Object parameters;
}
