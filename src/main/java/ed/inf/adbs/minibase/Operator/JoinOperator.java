package ed.inf.adbs.minibase.Operator;

import ed.inf.adbs.minibase.base.ComparisonAtom;
import ed.inf.adbs.minibase.base.RelationalAtom;

import java.util.ArrayList;
import java.util.List;
/*
* JoinOperator is responsible for performing join operations on a given list of relational atoms and comparison atoms.
* The class is part of a minimalist database management system and extends the Operator class.
* The join operation combines multiple tuples from different relational atoms according to the specified comparison atoms,
* creating a new tuple as a result.
* @author jackson-zhou
 */
public class JoinOperator extends Operator {
	// Variables to store the relational body, comparison body, and database catalog
	List<RelationalAtom> relationalBody;
	List<ComparisonAtom> comparisonBody;
	DatabaseCatalog dbCatalog;
	// List to store scan operators and tuples
	List<ScanOperator> scanOperatorList = new ArrayList<>();
	List<String> nonValidString = new ArrayList<>();
	List<Tuple> tupleList = new ArrayList<>();
	// Variables to store join tuple, non-valid tuple, table index, and first invocation flag
	Tuple joinTuple;
	Tuple nonValidTuple;
	int tableIndex;
	Boolean firstInvoke = true;
	/**
	 * JoinOperator constructor initializes the operator with the given
	 * relational body, comparison body, and database catalog.
	 *
	 * @param relationalBody A list of relational atoms.
	 * @param comparisonBody A list of comparison atoms.
	 * @param dbCatalog      The database catalog.
	 */
	public JoinOperator(List<RelationalAtom> relationalBody, List<ComparisonAtom> comparisonBody, DatabaseCatalog dbCatalog) {
		// Initialize scan operators for all required relational body
		this.relationalBody = relationalBody;
		this.comparisonBody = comparisonBody;
		this.dbCatalog = dbCatalog;

		this.nonValidString.add("NonValidString");
		this.nonValidTuple = new Tuple("NonValid", nonValidString, nonValidString, nonValidString);
		this.tableIndex = 0;

		initializeScanOperators();
	}
	/**
	 * Initializes the scan operators for each relational atom in the relational body.
	 */
	private void initializeScanOperators() {
		// Iterate through each relationalAtom in the relationalBody list
		for (RelationalAtom relationalAtom : relationalBody) {
			// Create a new ScanOperator object with the relationalAtom and the dbCatalog
			ScanOperator operator = new ScanOperator(relationalAtom, dbCatalog);
			// Add the newly created ScanOperator object to the scanOperatorList
			scanOperatorList.add(operator);
		}
	}
	/**
	 * Retrieves the next tuple in the join operation.
	 *
	 * @return The next valid tuple, non-valid tuple, or null.
	 */
	@Override
	public Tuple getNextTuple() {
		// Check if this is the first time the method is called
		if (firstInvoke) {
			// If it's the first time, call the handleFirstInvocation() method
			return handleFirstInvocation();
		} else {
			// If it's not the first time, call the handleSubsequentInvocation() method
			return handleSubsequentInvocation();
		}
	}
	/**
	 * Handles the first invocation of getNextTuple. Initializes the TupleList with the
	 * first tuple from each ScanOperator.
	 *
	 * @return A joined tuple if it's valid, otherwise a non-valid tuple.
	 */
	private Tuple handleFirstInvocation() {
		// Set the firstInvoke flag to false to indicate that the first invocation is done
		firstInvoke = false;
		// Iterate through each ScanOperator in the scanOperatorList
		for (ScanOperator scanOperator : scanOperatorList) {
			// Get the next tuple from the scanOperator
			Tuple tuple = scanOperator.getNextTuple();
			// Add the retrieved tuple to the tupleList
			tupleList.add(tuple);
		}
		// Perform a join operation on the tupleList and store the result in joinTuple
		joinTuple = tuple_join(tupleList);
		// Check if the joinTuple is valid
		if (tupleCheck(joinTuple)) {
			// If the joinTuple is valid, return the joinTuple
			return joinTuple;
		} else {
			// If the joinTuple is not valid, return the nonValidTuple
			return nonValidTuple;
		}
	}
	/**
	 * Handles subsequent invocations of getNextTuple.
	 * Iterates through scan operators and retrieves the next tuple.
	 * Verifies if the tuple is valid, non-valid or null.
	 *
	 * @return A joined tuple if it's valid, otherwise a non-valid tuple or null.
	 */
	private Tuple handleSubsequentInvocation() {
		// Loop through the scanOperatorList using the tableIndex as long as it is within the valid range
		while (tableIndex >= 0 && tableIndex < scanOperatorList.size()) {
			// Get the next tuple from the current ScanOperator in the list
			Tuple tempTuple = scanOperatorList.get(tableIndex).getNextTuple();

			// If the tempTuple is null, reset the current ScanOperator and move to the next one
			if (tempTuple == null) {
				resetScanOperatorAndMoveToNext();
				continue;
			}

			// Check if the tempTuple meets the comparison criteria defined in comparisonBody
			if (!compareTuple(tempTuple, comparisonBody)) {
				// If the tempTuple doesn't meet the comparison criteria, return the nonValidTuple
				return nonValidTuple;
			}

			// If it meets the criteria, update the tupleList at the tableIndex with the tempTuple
			tupleList.set(tableIndex, tempTuple);

			// Move the tableIndex back to the beginning (0) to process the next combination
			while (tableIndex > 0) {
				tableIndex--;
			}

			// Perform a join operation on the tupleList and store the result in joinTuple
			joinTuple = tuple_join(tupleList);

			// Check if the joinTuple is valid and return the appropriate result
			return tupleCheck(joinTuple) ? joinTuple : nonValidTuple;
		}

		// If the loop exits, return null as there are no more tuples to process
		return null;
	}

	/**
	 * Resets the current scan operator, moves to the next tuple, and increments the table index.
	 */
	private void resetScanOperatorAndMoveToNext() {
		// Reset the current ScanOperator at the tableIndex
		scanOperatorList.get(tableIndex).reset();
		// Get the next tuple from the current ScanOperator after resetting
		Tuple tempTuple = scanOperatorList.get(tableIndex).getNextTuple();
		// Update the tupleList at the tableIndex with the newly retrieved tempTuple
		tupleList.set(tableIndex, tempTuple);
		// Increment the tableIndex to move to the next ScanOperator in the list
		tableIndex++;
	}
	/**
	 * Compares a tuple with the comparison body using a SelectOperator.
	 *
	 * @param tuple          The tuple to be compared.
	 * @param comparisonBody The list of comparison atoms.
	 * @return True if the tuple is valid, false otherwise.
	 */
	private boolean compareTuple(Tuple tuple, List<ComparisonAtom> comparisonBody) {
		// Create a new SelectOperator with the given comparisonBody, tuple, and a flag set to true
		SelectOperator selectOperator = new SelectOperator(comparisonBody, tuple, true);
		// Get the next tuple after applying the SelectOperator
		tuple = selectOperator.getNextTuple();
		// Return true if the tuple's tableName is not equal to "NonValid", indicating it's a valid tuple
		return !tuple.getTableName().equals("NonValid");
	}
	/**
	 * Checks if a tuple is valid by comparing its values.
	 *
	 * @param tuple The tuple to be checked.
	 * @return True if the tuple is valid, false otherwise.
	 */
	private boolean tupleCheck(Tuple tuple) {
		// Get column names and values for easier access
		List<String> columnNames = tuple.getColumnName();
		List<String> values = tuple.getValue();
		int columnCount = columnNames.size();

		// Loop through each column name in the tuple
		for (int i = 0; i < columnCount; i++) {
			String currentColumnName = columnNames.get(i);
			String currentValue = values.get(i);

			// Compare the current column name with the other column names in the tuple
			for (int j = i + 1; j < columnCount; j++) {
				// If the column names are equal
				if (currentColumnName.equals(columnNames.get(j))) {
					// Check if the corresponding values are different
					if (!currentValue.equals(values.get(j))) {
						// If the values are different, return false (the tuple is invalid)
						return false;
					}
				}
			}
		}
		// If all column names with the same name have the same values, return true (the tuple is valid)
		return true;
	}

	/**
	 * Joins a list of tuples into a single tuple.
	 *
	 * @param tupleList2 The list of tuples to be joined.
	 * @return A new joined tuple.
	 */
	private Tuple tuple_join(List<Tuple> tupleList2) {
		// Initialize three new lists for column names, column types, and values
		List<String> columnName = new ArrayList<>();
		List<String> columnType = new ArrayList<>();
		List<String> value = new ArrayList<>();
		// Iterate through each tuple in the input tupleList2
		for (Tuple tuple : tupleList2) {
			// Add the column names, column types, and values from the current tuple to their respective lists
			columnName.addAll(tuple.ColumnName);
			columnType.addAll(tuple.ColumnType);
			value.addAll(tuple.value);
		}
		// Create and return a new Tuple object with the combined column names, column types, and values
		return new Tuple(tupleList2.get(0).tableName, columnName, columnType, value);
	}
	/**
	 * Resets the join operator by clearing the scan operator list and tuple list,
	 * and re-initializes scan operators.
	 */
	@Override
	public void reset() {
		// Clear the scanOperatorList, removing all ScanOperator objects from the list
		scanOperatorList.clear();
		// Clear the tupleList, removing all Tuple objects from the list
		tupleList.clear();
		// Reset the firstInvoke flag to true, to indicate that the next invocation is the first one
		firstInvoke = true;
		// Reinitialize the ScanOperator objects by calling the initializeScanOperators() method
		initializeScanOperators();
	}
	/**
	 * Dumps all the tuples in the join operation.
	 */
	@Override
	public void dump() {
		// Call the getNextTuple() method to get the next Tuple
		Tuple dumpTuple = getNextTuple();
		// Continue calling getNextTuple() and updating dumpTuple until no more tuples are available
		while (dumpTuple != null) {
			dumpTuple = getNextTuple();
		}
	}
}

