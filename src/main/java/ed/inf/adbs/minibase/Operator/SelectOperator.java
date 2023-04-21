package ed.inf.adbs.minibase.Operator;

import ed.inf.adbs.minibase.base.ComparisonAtom;
import ed.inf.adbs.minibase.base.ComparisonOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Processes a list of ComparisonAtoms to filter tuples from an underlying operator.
 *
 * @author jackson-zhou
 */
public class SelectOperator extends Operator {

	private List<ComparisonAtom> comparisonList;
	private Tuple tuple;
	private DatabaseCatalog dbCatalog;
	private Operator operator;
	private boolean joinInvoke = false;
	private boolean condition = true;
	private static final Tuple NON_VALID_TUPLE = new Tuple("Nonvalid", new ArrayList<>(), new ArrayList<>(), new ArrayList<>());

	/**
	 * Constructs a SelectOperator that processes a list of ComparisonAtoms
	 * to filter tuples based on given conditions.
	 *
	 * @param operator        The input operator providing the tuples to be filtered.
	 * @param comparisonList  The list of ComparisonAtoms containing the conditions for filtering.
	 * @param dbCatalogs      The DatabaseCatalog instance containing the table schema information.
	 */
	public SelectOperator(Operator operator, List<ComparisonAtom> comparisonList, DatabaseCatalog dbCatalogs) {
		this.comparisonList = comparisonList;
		this.dbCatalog = dbCatalogs;
		this.operator = operator;
		this.tuple = operator.getNextTuple(); // Get the next tuple from the input operator
		checkCondition(comparisonList, tuple); // Check if the conditions are valid for the tuple
		this.operator.reset(); // Reset the input operator to the beginning of the data
	}

	/**
	 * Constructs a SelectOperator that processes a list of ComparisonAtoms
	 * to filter tuples based on given conditions, specifically for use within a JoinOperator.
	 *
	 * @param comparisonList  The list of ComparisonAtoms containing the conditions for filtering.
	 * @param tuple           The tuple to be checked against the conditions.
	 * @param joinInvoke      A boolean flag indicating that this SelectOperator is invoked within a JoinOperator.
	 */
	public SelectOperator(List<ComparisonAtom> comparisonList, Tuple tuple, boolean joinInvoke) {
		this.comparisonList = new ArrayList<>(comparisonList); // Create a new list to store the comparison atoms
		this.tuple = tuple; // Store the tuple to be checked against the conditions
		this.joinInvoke = joinInvoke; // Set the joinInvoke flag to indicate that it's used within a JoinOperator
	}

	/**
	 * Check if the given ComparisonAtom is valid.
	 */
	private void checkCondition(List<ComparisonAtom> comparisonList, Tuple tuple) {
		// Iterate through each ComparisonAtom in the list
		for (ComparisonAtom comparAtom : comparisonList) {
			String atom1 = comparAtom.getTerm1().toString().trim();
			String atom2 = comparAtom.getTerm2().toString().trim();
			String op = comparAtom.getOp().toString().trim();

			// Check if the ComparisonAtom variable is not in the RelationalAtom column name
			int numAtom1 = 0;
			int numAtom2 = 0;
			for (int i = 0; i < tuple.getColumnName().size(); i++) {
				if (!isVariable(atom1)) {
					numAtom1 = -1;
				}
				if (!isVariable(atom2)) {
					numAtom2 = -1;
				}
				if (isVariable(atom1) && tuple.getColumnName().get(i).equals(atom1)) {
					numAtom1++;
				}
				if (isVariable(atom2) && tuple.getColumnName().get(i).equals(atom2)) {
					numAtom2++;
				}
			}
			if (numAtom1 == 0) {
				condition = false;
			}
			if (numAtom2 == 0) {
				condition = false;
			}

			// If two atoms are the same, the operator should be '='
			if (condition) {
				if (isString(atom1) || isString(atom2)) {
					if (atom1.equals(atom2) && !op.equals("=")) {
						condition = false;
					}
				}
			}

			// Check if column types of both atoms match
			if (condition) {
				try {
					if (!tuple.getColumnType().get(tuple.getColumnName().indexOf(atom1))
							.equals(tuple.getColumnType().get(tuple.getColumnName().indexOf(atom2)))) {
						condition = false;
					}
				} catch (Exception e) {
					// Ignore the exception
				}
			}

			// Check if column types are incompatible with constant values
			if (condition) {
				try {
					if (tuple.getColumnType().get(tuple.getColumnName().indexOf(atom1)).equals("String") && isInteger(atom2)) {
						condition = false;
					}
				} catch (Exception e) {
					if (tuple.getColumnType().get(tuple.getColumnName().indexOf(atom2)).equals("String") && isInteger(atom1)) {
						condition = false;
					}
				}
			}
			// If both atoms are constants, check their compatibility based on the operator
			if (condition) {
				if (!isVariable(atom1) && !isVariable(atom2)) {
					checkConstantAtomsCompatibility(atom1, atom2, op);
				}
			}
		}
	}

	/**
	 * Check the compatibility of two constant atoms based on the operator.
	 */
	private void checkConstantAtomsCompatibility(String atom1, String atom2, String op) {
		if (op.equals("=") && !atom1.equals(atom2)) {
			condition = false;
		}
		if (op.equals("!=") && atom1.equals(atom2)) {
			condition = false;
		}
		if (isInteger(atom1) && isInteger(atom2)) {
			int value1 = Integer.parseInt(atom1);
			int value2 = Integer.parseInt(atom2);
			if (op.equals(">") && value1 <= value2) {
				condition = false;
			} else if (op.equals(">=") && value1 < value2) {
				condition = false;
			} else if (op.equals("<") && value1 >= value2) {
				condition = false;
			} else if (op.equals("<=") && value1 > value2) {
				condition = false;
			}
		}
	}

	/**
	 * Get the next tuple after filtering through the selection conditions.
	 */
	@Override
	public Tuple getNextTuple() {
		if (joinInvoke) {
			tuple = runSelect();
			joinInvoke = false;
			return tuple;
		}
		if (!condition) {
			return null;
		}
		tuple = operator.getNextTuple();
		if (tuple != null) {
			tuple = runSelect();
		}
		return tuple;
	}

	/**
	 * Run the selection process on the current tuple.
	 */
	private Tuple runSelect() {
		Tuple resTuple = tuple;
		// Remove constant variables in RelationalAtom
		for (int i = 0; i < tuple.getColumnName().size(); i++) {
			if (!isVariable(tuple.getColumnName().get(i)) && !tuple.getValue().get(i).equals(tuple.getColumnName().get(i))) {
				return NON_VALID_TUPLE;
			}
		}
		// Check compatibility based on ComparisonAtoms
		for (ComparisonAtom comparAtom : comparisonList) {
			String firstElem = comparAtom.getTerm1().toString().trim();
			String secondElem = comparAtom.getTerm2().toString().trim();
			ComparisonOperator op = comparAtom.getOp();

			resTuple = processComparison(resTuple, firstElem, secondElem, op);
			if (resTuple == NON_VALID_TUPLE) {
				break;
			}
		}
		return resTuple;
	}

	/**
	 * Process a single ComparisonAtom for the given tuple.
	 */
	private Tuple processComparison(Tuple tuple, String firstElem, String secondElem, ComparisonOperator op) {
		if (isVariable(firstElem) && isVariable(secondElem)) {
			return compareTwoColumns(tuple, firstElem, secondElem, op);
		} else if (isVariable(firstElem)) {
			return compareColumnWithConstant(tuple, firstElem, secondElem, op);
		} else if (isVariable(secondElem)) {
			return compareColumnWithConstant(tuple, secondElem, firstElem, op);
		}
		return tuple;
	}

	/**
	 * Compare two columns of a tuple based on the operator.
	 */
	private Tuple compareTwoColumns(Tuple tuple, String firstElem, String secondElem, ComparisonOperator op) {
		for (int i = 0; i < tuple.getColumnName().size(); i++) {
			if (tuple.getColumnName().get(i).trim().equals(firstElem)) {
				for (int j = 0; j < tuple.getColumnName().size(); j++) {
					if (tuple.getColumnName().get(j).trim().equals(secondElem)) {
						return checkOperator(op, tuple, tuple.getValue().get(i), tuple.getValue().get(j));
					}
				}
			}
		}
		return tuple;
	}

	/**
	 * Compare a column with a constant value based on the operator.
	 */
	private Tuple compareColumnWithConstant(Tuple tuple, String columnName, String constantValue, ComparisonOperator op) {
		for (int i = 0; i < tuple.getColumnName().size(); i++) {
			if (tuple.getColumnName().get(i).trim().equals(columnName)) {
				return checkOperator(op, tuple, tuple.getValue().get(i), constantValue);
			}
		}
		return tuple;
	}

	/**
	 * Check if the values satisfy the given operator and return the tuple accordingly.
	 */
	private Tuple checkOperator(ComparisonOperator op, Tuple tuple, String value1, String value2) {
		switch (op) {
			case EQ:
				if (!value1.equals(value2)) {
					return NON_VALID_TUPLE;
				}
				break;
			case NEQ:
				if (value1.equals(value2)) {
					return NON_VALID_TUPLE;
				}
				break;
			case GT:
				if (Integer.parseInt(value1) <= Integer.parseInt(value2)) {
					return NON_VALID_TUPLE;
				}
				break;
			case GEQ:
				if (Integer.parseInt(value1) < Integer.parseInt(value2)) {
					return NON_VALID_TUPLE;
				}
				break;
			case LT:
				if (Integer.parseInt(value1) >= Integer.parseInt(value2)) {
					return NON_VALID_TUPLE;
				}
				break;
			case LEQ:
				if (Integer.parseInt(value1) > Integer.parseInt(value2)) {
					return NON_VALID_TUPLE;
				}
				break;
		}
		return tuple;
	}

	/**
	 * Check if the input string represents a variable.
	 */
	private boolean isVariable(String str) {
		str = str.trim();
		return !(isString(str) || isInteger(str));
	}

	/**
	 * Check if the input string represents an integer value.
	 */
	private static boolean isInteger(String input) {
		Pattern pattern = Pattern.compile("^[+-]?\\d+$");
		Matcher matcher = pattern.matcher(input);
		return matcher.find();
	}

	/**
	 * Check if the input string represents a string value.
	 */
	private boolean isString(String str) {
		str = str.trim();
		return str.startsWith("'");
	}

	@Override
	public void reset() {
		operator.reset();
	}

	@Override
	public void dump() {
		tuple = getNextTuple();
		while (tuple != null) {
			tuple = getNextTuple();
		}
	}
}

