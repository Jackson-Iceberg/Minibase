package ed.inf.adbs.minibase.Operator;

import ed.inf.adbs.minibase.base.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * ProjectOperator performs the projection operation on tuples.
 * It eliminates duplicate columns, orders columns according to the query, and returns distinct tuples.
 * @author jackson-zhou
 **/
public class ProjectOperator extends Operator {
	// Initializaiton
	Head headAtom1;
	private List<ComparisonAtom> comparisonList;
	private DatabaseCatalog dbCatalog;
	Operator operator;
	List<Variable> headVariable;
	Tuple tuple;
	Tuple oldTuple;
	private SumAggregate headAgg = null;
	private List<Term> headAggVariable = new ArrayList<>();
	List<String> allVariable = new ArrayList<>();
	HashSet<List<String>> tupleSet = new HashSet<>();
	List<String> nonValideString = new ArrayList<>();
	Tuple nonValidTuple = new Tuple("Nonvalid", nonValideString, nonValideString, nonValideString);
	Tuple newTuple;

	/**
	 * Initializes the ProjectOperator with the provided parameters.
	 *
	 * @param operator       The underlying operator to be used
	 * @param headAtom       The Head atom containing variables and sum aggregate information
	 * @param comparisonList The list of comparison atoms for filtering
	 * @param dbCatalogs     The database catalog used to store and retrieve table data
	 */
	public ProjectOperator(Operator operator, Head headAtom, List<ComparisonAtom> comparisonList, DatabaseCatalog dbCatalogs) {
		// Set the provided parameters to their respective instance variables
		this.headAtom1 = headAtom;
		this.comparisonList = comparisonList;
		this.dbCatalog = dbCatalogs;
		this.operator = operator;
		this.headVariable = headAtom.getVariables();

		// Iterate through the head variables and add them to the allVariable list
		for (int i = 0; i < headVariable.size(); i++) {
			allVariable.add(headVariable.get(i).toString().trim());
		}

		// If there is a sum aggregate in the head atom, add its product terms to the allVariable list
		if (headAtom.getSumAggregate() != null) {
			this.headAgg = headAtom.getSumAggregate();
			this.headAggVariable = headAtom.getSumAggregate().getProductTerms();
			for (int j = 0; j < headAggVariable.size(); j++) {
				allVariable.add(headAggVariable.get(j).toString().trim());
			}
		}
	}

	/**
	 * Retrieves the next tuple after performing the project operation.
	 *
	 * @return The next tuple after projection or null if no more tuples are available
	 */
	@Override
	public Tuple getNextTuple() {
		// Fetch the next tuple from the underlying operator
		oldTuple = operator.getNextTuple();
		// If there is a tuple and it is not a "NonValid" tuple, perform the projection operation
		if ((oldTuple != null)&&(!oldTuple.getTableName().equals("NonValid"))) {
				oldTuple = runProject();
		}
		// Update the instance variable and return the tuple
		tuple = oldTuple;
		return tuple;
	}

	/**
	 * Performs the projection operation on the current tuple and returns the result.
	 *
	 * @return The projected tuple or null if there are no columns to project
	 */
	private Tuple runProject() {
		// Initialize the lists to store the ordered column names, column types, and values
		List<String> columnName = new ArrayList<>();
		List<String> columnType = new ArrayList<>();
		List<String> value = new ArrayList<>();

		// If there are variables in the query, perform the projection
		if (!allVariable.isEmpty()) {
			// Remove duplicate columns from the tuple
			removeDuplicateColumns(oldTuple);
			// Order the columns in the tuple based on the query
			orderColumns(oldTuple, columnName, columnType, value);

			// Create a new tuple with the ordered columns
			Tuple newTuple = new Tuple(oldTuple.getTableName(), columnName, columnType, value);

			// If there is no sum aggregate, return distinct tuples
			if (headAtom1.getSumAggregate() == null) {
				// If the new tuple already exists in the tupleSet, return the nonValidTuple
				if (tupleSet.contains(newTuple.getValue())) {
					return nonValidTuple;
				} else {
					// Add the new tuple to the tupleSet and return it
					tupleSet.add(newTuple.getValue());
					return newTuple;
				}
			} else {
				// If there is a sum aggregate, return the new tuple without checking for distinctness
				return newTuple;
			}
		} else {
			// If there are no columns to project, return null
			return null;
		}
	}
	/**
	 * Orders the columns of the given tuple based on the query and adds them to the provided lists.
	 *
	 * @param tuple      The tuple with unordered columns
	 * @param columnName The list that will store the ordered column names
	 * @param columnType The list that will store the ordered column types
	 * @param value      The list that will store the ordered values
	 */
	private void orderColumns(Tuple tuple, List<String> columnName, List<String> columnType, List<String> value) {
		// Iterate through all the variables in the query
		for (String var : allVariable) {
			// Iterate through the columns in the tuple
			for (int j = 0; j < tuple.getColumnName().size(); j++) {
				// If the column name matches the variable in the query
				if (tuple.getColumnName().get(j).equals(var)) {
					// Add the column name, column type, and value to their respective lists
					columnName.add(tuple.getColumnName().get(j));
					columnType.add(tuple.getColumnType().get(j));
					value.add(tuple.getValue().get(j));
				}
			}
		}
	}

	/**
	 * Removes duplicate columns from the given tuple.
	 *
	 * @param tuple The tuple from which duplicate columns will be removed
	 */
	private void removeDuplicateColumns(Tuple tuple) {
		// Iterate through the columns in the tuple
		for (int i = 0; i < tuple.getColumnName().size(); i++) {
			// Iterate through the remaining columns (after the current column)
			for (int j = i + 1; j < tuple.getColumnName().size(); j++) {
				// Check if the current column and the compared column have the same name
				if (tuple.getColumnName().get(i).equals(tuple.getColumnName().get(j))) {
					// Remove the duplicate column name, column type, and value from the tuple
					removeDuplicateTuple(tuple,j);
					// Decrement the index to account for the removed column
					j--;
				}
			}
		}
	}

	/**
	 * Removes the column at the specified index from the given tuple.
	 * This method also removes the associated column type and value for the removed column.
	 *
	 * @param tuple The tuple from which to remove the column, its type, and its value.
	 * @param index The index of the column to remove from the tuple.
	 * @throws IndexOutOfBoundsException If the index is out of range (index < 0 || index >= tuple.getColumnName().size()).
	 */
	private void removeDuplicateTuple(Tuple tuple, int index) {
		// Check if the provided index is within the bounds of the tuple's column list
		if (index < 0 || index >= tuple.getColumnName().size()) {
			throw new IndexOutOfBoundsException("Index out of range: " + index);
		}

		// Remove the column name at the specified index from the tuple's column name list
		tuple.getColumnName().remove(index);

		// Remove the column type at the specified index from the tuple's column type list
		tuple.getColumnType().remove(index);

		// Remove the value at the specified index from the tuple's value list
		tuple.getValue().remove(index);
	}


	/**
	 * Processes all remaining tuples in the operator and adds them to the database catalog.
	 */
	@Override
	public void dump() {
		// Continuously fetches tuples until there are no more tuples left
		while (tuple != null) {
			// If the tuple is not a "NonValid" tuple, it is added to the database catalog
			if (!tuple.getTableName().equals("NonValid")) {
				dbCatalog.addTupleList(tuple);
			}
			// Fetch the next tuple
			tuple = getNextTuple();
		}
	}

	/**
	 * Resets the ProjectOperator, clearing the tuple set.
	 */
	@Override
	public void reset() {
		// Resets the underlying operator
		operator.reset();
		// Clears the set of tuples to remove any existing tuple information
		tupleSet.clear();
	}

}

