package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.List;

/**
 * This class knows how to populate a table by instantiate its set elements given elements in other tables.
 */
public interface TableDefinition {
	public void populate();
	public List<BistroError> getErrors();
	public List<Element> getDependencies();
}



class TableDefinitionProd implements TableDefinition {

    Table table;

    List<BistroError> definitionErrors = new ArrayList<>();

    @Override
    public List<BistroError> getErrors() {
        return this.definitionErrors;
    }

    @Override
    public List<Element> getDependencies() {
        // TODO:
        // Incoming: a list of all incoming (generating) proj-columns & these column incoming tables
        // Keys: (key) domains have to be populated
        return null;
    }

    @Override
    public void populate() {

    }

    public TableDefinitionProd(Table table) {
        this.table = table;
    }

}
