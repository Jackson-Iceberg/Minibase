package ed.inf.adbs.minibase.Operator;

import ed.inf.adbs.minibase.base.Head;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The SumOperator class is a part of the ed.inf.adbs.minibase.
 * Operator package and is responsible for performing the SUM aggregation operation on a given set of tuples.
 * This operation may include group by aggregation or simple summation, depending on the input.
 *
 * @author jackson-zhou
 */
public class SumOperator extends Operator {

    List<Tuple> tupleList = new ArrayList<>();
    List<Tuple> newTupleList = new ArrayList<>();
    DatabaseCatalog dbCatalog;
    Head head;
    HashMap<String, List<Integer>> map = new HashMap<>();

    /**
     * Constructor: SumOperator
     * Initializes the SumOperator class with the given head and dbCatalog.
     * It calls the appropriate method to handle either constant or variable aggregation.
     *
     * @param head      An instance of the Head class containing information about the query being executed.
     * @param dbCatalog An instance of the DatabaseCatalog class containing the database schema and other related information.
     */
    public SumOperator(Head head, DatabaseCatalog dbCatalog) {
        this.head = head;
        this.dbCatalog = dbCatalog;
        initializeTupleList(); // Populate the tupleList with tuples from the dbCatalog

        // Call the appropriate method to handle either constant or variable aggregation
        if (isConstantSumAggregate()) {
            handleConstantSumAggregate();
        } else {
            handleVariableSumAggregate();
        }
    }

    /**
     * Checks whether the given input is an integer (including negative and positive integers).
     *
     * @param input A string to be checked for being an integer.
     * @return true if the input is an integer, false otherwise.
     */
    private static boolean isInteger(String input) {
        // Compile the regular expression pattern to match integers,
        // including negative and positive integers.
        Pattern pattern = Pattern.compile("^[+-]?\\d+$");

        // Create a matcher with the input string and the pattern.
        Matcher matcher = pattern.matcher(input);

        // Return true if the input matches the integer pattern, false otherwise.
        return matcher.find();
    }

    /**
     * Method: initializeTupleList
     * Populates the tupleList based on the type of the first product term in the SUM aggregation.
     * If the first product term is an integer, it adds all tuples from the dbCatalog to tupleList.
     * Otherwise, it adds only non-empty tuples to tupleList.
     */
    private void initializeTupleList() {
        // Check if the first product term in the SUM aggregation is an integer
        if (!isInteger(head.getSumAggregate().getProductTerms().get(0).toString())) {
            // Iterate through the tuples in dbCatalog's tupleList
            for (int i = 0; i < dbCatalog.getTupleList().size(); i++) {
                // Add the tuple to tupleList if its value list is not empty
                if (dbCatalog.getTupleList().get(i).getValue().size() != 0) {
                    tupleList.add(dbCatalog.getTupleList().get(i));
                }
            }
        } else {
            // If the first product term is an integer, add all tuples from dbCatalog to tupleList
            tupleList.addAll(dbCatalog.getTupleList());
        }
    }

    /**
     * Method: isConstantSumAggregate
     * Checks if the SUM aggregation operation is a constant or variable aggregation.
     * It returns true if the first product term is an integer (constant aggregation) and false otherwise.
     *
     * @return boolean True if the SUM operation is a constant aggregation, false otherwise.
     */
    private boolean isConstantSumAggregate() {
        // return boolean True if the SUM operation is a constant aggregation, false otherwise.
        return isInteger(head.getSumAggregate().getProductTerms().get(0).toString());
    }

	/**
	 * Method: handleConstantSumAggregate
	 * Handles the constant aggregation scenario by creating a new tuple containing the sum of the constant value.
	 * It adds this new tuple to the newTupleList and updates the dbCatalog's tupleList.
	 */
	private void handleConstantSumAggregate() {
		List<String> newString = new ArrayList<>(); // Initialize an empty list to store the new value for the aggregated tuple
		int size = tupleList.size(); // Get the size of the tupleList (the number of tuples)
		String cons = head.getSumAggregate().getProductTerms().get(0).toString(); // Get the constant value from the first product term in the SUM aggregate

		int groupBy_cons = size * Integer.parseInt(cons); // Calculate the aggregated sum by multiplying the constant value by the number of tuples

		newString.add(Integer.toString(groupBy_cons)); // Add the calculated sum to the newString list
		// Create a new Tuple with the aggregated sum value and the same table name, column name, and column type as the first tuple in the tupleList
		Tuple tuple = new Tuple(tupleList.get(0).getTableName(), tupleList.get(0).getColumnName(), tupleList.get(0).getColumnType(), newString);
		newTupleList.add(tuple); // Add the newly created Tuple to the newTupleList
		dbCatalog.setTupleList(newTupleList); // Update the dbCatalog's tupleList with the newTupleList
	}

    /**
     * Method: handleVariableSumAggregate
     * Handles the variable aggregation scenario.
     * If a SUM aggregate operation is present, it processes the tuples and adds the results to the newTupleList.
     * Otherwise, it removes duplicate tuples from the tuple
     */
    private void handleVariableSumAggregate() {
        if (head.getSumAggregate() != null) {
            processTuples(); // Process the tuples for variable aggregation
            addTuplesFromMap(); // Add the results to the newTupleList
        } else {
            removeDuplicateTuples(); // Remove duplicate tuples from the tupleList
            newTupleList = tupleList; // Set newTupleList as the updated tupleList
        }
        dbCatalog.setTupleList(newTupleList); // Update the dbCatalog's tupleList
    }

	/**
	 * Method: processTuples
	 * Processes each tuple in the tupleList, calculates the sum for each group, and stores the result in a map.
	 * The map keys represent the group's key, and the values are a list of two integers:
	 *   - The first integer represents the sum of the group.
	 *   - The second integer represents the number of elements in the group.
	 */
	private void processTuples() {
		for (Tuple a : tupleList) { // Iterate through each tuple in the tupleList
			String b = getKey(a); // Get the key for the current tuple

			// Initialize the map if the current key is not already present
			if (map.get(getKey(a)) == null) {
				List<Integer> initInt = new ArrayList<>(); // Initialize a new list of integers
				initInt.add(0); // Set the initial sum to 0
				initInt.add(0); // Set the initial count to 0
				map.put(getKey(a), initInt); // Add the new list to the map with the current key
			}

			List<Integer> intList = map.get(getKey(a)); // Retrieve the list of integers for the current key
			// Increment the sum by the last value in the tuple's value list (converted to an integer)
			intList.set(0, intList.get(0) + Integer.parseInt(a.getValue().get(a.getValue().size() - 1)));
			intList.set(1, intList.get(1) + 1
			); // Increment the count by 1
			map.put(getKey(a), intList); // Update the map with the modified list for the current key
		}
	}

	/**

	 Method: addTuplesFromMap

	 Constructs new tuples based on the aggregated results stored in the map and adds them to the newTupleList.

	 Each new tuple has the same table name, column type, and column names as the first tuple in the tupleList.
	 */
	private void addTuplesFromMap() {
		for (String key : map.keySet()) { // Iterate through each key in the map
			String[] cataArr = key.split(","); // Split the key string using the comma delimiter
			List<String> newString = new ArrayList<>(); // Initialize an empty list to store the new value for the aggregated tuple
			if (key.length() != 0) {
				newString.addAll(Arrays.asList(cataArr)); // Add the split key values to the newString list
			}
			newString.add(map.get(key).get(0).toString()); // Add the aggregated sum (first integer in the list) to the newString list
			// Create a new Tuple with the aggregated values and the same table name, column type, and column names as the first tuple in the tupleList
			Tuple tuple = new Tuple(tupleList.get(0).getTableName(), tupleList.get(0).getColumnType(), tupleList.get(0).getColumnName(), newString);
			newTupleList.add(tuple); // Add the newly created Tuple to the newTupleList
		}
	}

	/**
	 * Method: removeDuplicateTuples
	 * Removes duplicate tuples from the tupleList.
	 * Compares each tuple's value in the list with all other tuples, and if a duplicate is found, it is removed.
	 */
	private void removeDuplicateTuples() {
		int numk = 0;
		int numm;
		while (numk < tupleList.size()) { // Outer loop, iterates through each tuple in the tupleList
			numm = 0;
			while (numm < tupleList.size()) { // Inner loop, iterates through each tuple again for comparison
				if (numm > numk) { // Only compare tuples with higher index to avoid redundant comparisons
					// If the current tuple's value (in outer loop) is equal to the compared tuple's value (in inner loop), remove the duplicate tuple
					if (tupleList.get(numk).getValue().toString().equals(tupleList.get(numm).getValue().toString())) {
						tupleList.remove(numm);
						numm--; // Decrement numm to account for the removed tuple
					}
				}
				numm++; // Increment inner loop counter
			}
			numk++; // Increment outer loop counter
		}
	}

	/**
	 * Method: getKey
	 * Generates a key string for a given tuple.
	 * The key is a concatenation of all values in the tuple (except the last one) separated by commas.
	 * @param tuple The input tuple for which to generate the key.
	 * @return A string representing the key for the input tuple.
	 */
	private String getKey(Tuple tuple) {
		StringBuilder key = new StringBuilder(); // Initialize a StringBuilder to build the key
		for (int i = 0; i < tuple.getValue().size() - 1; i++) { // Iterate through each value in the tuple, except the last one
			key.append(tuple.getValue().get(i)); // Append the current value to the key
			if (i < tuple.getValue().size() - 2) { // If there are more values to process, add a comma as a delimiter
				key.append(",");
			}
		}
		return key.toString(); // Return the generated key as a string
	}
	/**
	 Method: getNextTuple
	 (Dummy implementation) This method is overridden from an interface or superclass, but it is not implemented here.
	 @return Always returns null, as the method is not implemented.
	 */
	@Override
	public Tuple getNextTuple() {
		return null; // Return null, as the method is not implemented
	}

    @Override
    public void reset() {
    }

    @Override
    public void dump() {
    }
}


