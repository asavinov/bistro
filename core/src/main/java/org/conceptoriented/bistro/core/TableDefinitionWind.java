package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.List;

public class TableDefinitionWind implements TableDefinition {

    Table table; // Used only if materialized as a table

    List<BistroError> definitionErrors = new ArrayList<>();

    ColumnPath[] dimensions; // Dimensions for computing distance/weight
    // Examples:
    // Empty - row id
    // One - time or 1D space
    // Two - 2D space

    EvaluatorWind evaluator;
    // It knows how to compute one number from two points represented by dimensions which represents distance between them or weight
    // If not specified then auto: row id difference, time interval etc.

    double size; // Window size (past, positive)
    double sizeFuture; // Window size (future, negative)
    // Inclusive or exclusive? One can be inclusive and the other exclusive.
    // It is a constraint on window members. Only elements satisfying this criterion will be included in windows

    // Ordering
    // How to order? Either by dimensions or by distance or by arbitrary other columns
    // Is it really important or is an optimization mechanism?


    @Override
    public List<BistroError> getErrors() {
        return this.definitionErrors;
    }

    @Override
    public List<Element> getDependencies() {
        List<Element> ret = new ArrayList<>();

        // TODO:

        return ret;
    }

    @Override
    public void populate() {
        // Currently not used for populating/materializing a table - its records are generated and used on the fly
    }

    public TableDefinitionWind(ColumnPath dimension, double size) {

    }

    public TableDefinitionWind(Column dimension, double size) {

    }
}

@FunctionalInterface
interface EvaluatorWind {
    public double eval(Object[] point, Object[] center) throws BistroError;
}
