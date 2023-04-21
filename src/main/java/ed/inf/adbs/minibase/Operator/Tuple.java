package ed.inf.adbs.minibase.Operator;

import java.util.List;

/**
 * The Tuple class represents a tuple  in a relational database.
 * It contains information about the table name, column names, column types,
 * and the values for each column in the tuple.
 * @author jackson-zhou
 */
public class Tuple {
	// The name of the table the tuple belongs to
	String tableName;

	// The list of column names for the tuple
	List<String> ColumnName;

	// The list of column types for the tuple
	List<String> ColumnType;

	// The list of values for each column in the tuple
	List<String> value;

	/**
	 * Constructs a Tuple object with the given table name, column names, column types,
	 * and values.
	 *
	 * @param tableName The name of the table the tuple belongs to.
	 * @param ColumnName The list of column names for the tuple.
	 * @param ColumnType The list of column types for the tuple.
	 * @param value The list of values for each column in the tuple.
	 */
	public Tuple(String tableName, List<String> ColumnName, List<String> ColumnType, List<String> value) {
		this.tableName = tableName;
		this.ColumnName = ColumnName;
		this.ColumnType = ColumnType;
		this.value = value;
	}

	/**
	 * Returns the table name of the tuple.
	 *
	 * @return The name of the table the tuple belongs to.
	 */
	public String getTableName() {
		return this.tableName;
	}

	/**
	 * Returns the list of column names for the tuple.
	 *
	 * @return The list of column names for the tuple.
	 */
	public List<String> getColumnName() {
		return ColumnName;
	}

	/**
	 * Returns the list of column types for the tuple.
	 *
	 * @return The list of column types for the tuple.
	 */
	public List<String> getColumnType() {
		return ColumnType;
	}

	/**
	 * Returns the list of values for each column in the tuple.
	 *
	 * @return The list of values for each column in the tuple.
	 */
	public List<String> getValue() {
		return value;
	}


}
