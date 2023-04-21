package ed.inf.adbs.minibase.Operator;

import ed.inf.adbs.minibase.base.ComparisonAtom;
import ed.inf.adbs.minibase.base.Head;
import ed.inf.adbs.minibase.base.RelationalAtom;

import java.util.List;

/**
 * QueryPlan is responsible for constructing an optimized query plan for a given query.
 * The query plan consists of various operators like Scan, Select, Join, and Project.
 * For more details on the algorithm, please refer to readme.md.
 *
 * @author jackson-zhou
 */
public class QueryPlan {

	private Operator operator;

	/**
	 * Constructs an optimized query plan using the given head, relationalBody, comparisonBody, and dbCatalog.
	 * The plan is a combination of various operators like Scan, Select, Join, and Project.
	 * The detailed explanation can be found in readme.md.
	 *
	 * @param head            the query head
	 * @param relationalBody  the list of relational atoms in the query body
	 * @param comparisonBody  the list of comparison atoms in the query body
	 * @param dbCatalog       the database catalog
	 */
	public QueryPlan(Head head, List<RelationalAtom> relationalBody, List<ComparisonAtom> comparisonBody, DatabaseCatalog dbCatalog) {
		if (relationalBody.size() == 1) {
			createSingleRelationPlan(relationalBody, comparisonBody, dbCatalog);
		} else {
			createMultiRelationPlan(relationalBody, comparisonBody, dbCatalog);
		}

		operator = new ProjectOperator(operator, head, comparisonBody, dbCatalog);
	}

	/**
	 * Creates a query plan for a single relational atom.
	 *
	 * @param relationalBody  the list of relational atoms in the query body
	 * @param comparisonBody  the list of comparison atoms in the query body
	 * @param dbCatalog       the database catalog
	 */
	private void createSingleRelationPlan(List<RelationalAtom> relationalBody, List<ComparisonAtom> comparisonBody, DatabaseCatalog dbCatalog) {
		operator = new ScanOperator(relationalBody.get(0), dbCatalog);

		if (!comparisonBody.isEmpty()) {
			operator = new SelectOperator(operator, comparisonBody, dbCatalog);
		}
	}

	/**
	 * Creates a query plan for multiple relational atoms.
	 *
	 * @param relationalBody  the list of relational atoms in the query body
	 * @param comparisonBody  the list of comparison atoms in the query body
	 * @param dbCatalog       the database catalog
	 */
	private void createMultiRelationPlan(List<RelationalAtom> relationalBody, List<ComparisonAtom> comparisonBody, DatabaseCatalog dbCatalog) {
		operator = new JoinOperator(relationalBody, comparisonBody, dbCatalog);

		if (!comparisonBody.isEmpty()) {
			operator = new SelectOperator(operator, comparisonBody, dbCatalog);
		}
	}

	/**
	 * Returns the root operator of the query plan.
	 *
	 * @return the root operator of the query plan
	 */
	public Operator getOperator() {
		return operator;
	}
}
