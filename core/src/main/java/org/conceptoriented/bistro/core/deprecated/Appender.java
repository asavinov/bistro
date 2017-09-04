package org.conceptoriented.bistro.core.deprecated;

import org.conceptoriented.bistro.core.Table;

/**
 * Objects of this class know where records come from. 
 * It can be viewed as a provider of records which are about to be appended to a table.
 * The records are requested from the appender just before physically adding them to a table.
 * It is used by the table population procedure which is responsible for and knows how adding and removing records.
 * In some cases tables are populated from other internal tables, for example, using product or filter.
 * In other cases tables are populated using external sources and this class is a way to provide such external sources of records.
 * 
 * Table population is normally performed automatically by producing all combinations of records (ids) from the key domains.
 * In other words, we take this table and retrieve all its key columns. Then we take all their domains and produce all combinations.
 * The combinations are appended to this table as new records.
 * 
 * A specific case of this approach is filtering. 
 * In this case, a table has a single key column (super) and hence it effectively copies records from another table.
 * However, the added records can be additionally filtered.
 * 
 * It is also possible to define an external data source as a record provider for this table.
 * If the table population procedure finds an external provider then it will call it to get records. 
 * The records returned by this provider have to be compatible with this table column structure. 
 * 
 * @author savinov
 */
public class Appender {
	
	private Table table;

	/**
	 * The method is called just before a new record is physically added to the table (and marked as new).
	 * As soon as the schema population procedure needs to populate the table, it will call this method to get them.
	 */
	public Record getRecord() {
		// Here we can retrieve a record or request it from somewhere. 
		// It is specific to the way we store/send data.
		return null;
	}

	/**
	 * It is a call back method which is used by an external asynchronous data provider like http-server or publisher.
	 * This method is specific to each appender (does not belong to appender api)
	 */
	public void onRecordReceived(Record record) {
	}

	public Appender(Table table) {
		this.table = table;
	}

}

/**
 * It is provided along with the appender (so maybe use only one class). 
 * It is used just after physically removing records from a table.
 * It knows what to do with just removed records.
 * This method produces records added to the table.  
 * 
 * @author savinov
 */
class Remover {

	private Table table;

	/**
	 * The method is called just after an existing record is physically removed from the table (and after it is cleaned).
	 * As soon as the schema population procedure needs to depopulate the table, it will call this method and pass the removed records to it.
	 * This method consumes records that are physically removed from the table.
	 */
	public void putRecord(Record record) {
		// Here we can send this record or store it somewhere.
		// It is specific to the way we store/send data.
	}

	/**
	 * This method knows the external data sink like pub-sub system or database.
	 * This method is specific to each remover (does not belong to appender api)
	 */
	public void onRecordSent(Record record) {
		// Really store/send record
	}

	public Remover(Table table) {
		this.table = table;
	}

}

// Dummy class instead of the previous Record used for serialization
class Record {

}
