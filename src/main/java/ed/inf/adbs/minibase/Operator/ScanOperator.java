package ed.inf.adbs.minibase.Operator;

import ed.inf.adbs.minibase.base.RelationalAtom;
import ed.inf.adbs.minibase.base.Term;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * ScanOperator is responsible for scanning tuples in tables.
 * It extends the Operator class and implements the required methods for processing tuples.
 *
 * @author jackson-zhou
 */
public class ScanOperator extends Operator {
	private String tableName;
	private DatabaseCatalog dbCatalog;
	private List<String> columnNames;
	private List<String> columnTypes;
	private List<String> values;
	private BufferedReader bufferedReader;
	private String currentLine;
	/**
	 * Constructor for ScanOperator.
	 * Initializes the tableName, dbCatalog, columnNames, columnTypes, and sets up the bufferedReader.
	 *
	 * @param atom      A RelationalAtom object representing the table.
	 * @param dbCatalog A DatabaseCatalog object containing the database schema and file paths.
	 */
	public ScanOperator(RelationalAtom atom, DatabaseCatalog dbCatalog) {
		// Set the tableName from the provided RelationalAtom object
		this.tableName = atom.getName();
		// Set the DatabaseCatalog object to access the database schema and file paths
		this.dbCatalog = dbCatalog;
		// Initialize the column names and column types using the provided RelationalAtom
		initColumnNameAndType(atom);
		// Set up the BufferedReader to read from the CSV file corresponding to the table
		setupBufferedReader();
	}
	/**
	 * Initializes the columnNames and columnTypes lists using the provided RelationalAtom.
	 *
	 * @param atom A RelationalAtom object representing the table.
	 */
	private void initColumnNameAndType(RelationalAtom atom) {
		// Initialize the columnNames list
		columnNames = new ArrayList<>();
		// Initialize the columnTypes list
		columnTypes = new ArrayList<>();
		// Iterate through the terms (columns) of the provided RelationalAtom
		for (Term term : atom.getTerms()) {
			// Add the trimmed column name to the columnNames list
			columnNames.add(term.toString().trim());
		}
		// Get the column types from the DatabaseCatalog object and store it in the columnTypes list
		columnTypes = dbCatalog.dbCatalogType.get(tableName);
	}
	/**
	 * Sets up the BufferedReader for reading the CSV file corresponding to the table.
	 */
	private void setupBufferedReader() {
		// Create a File object that represents the CSV file for the table
		String filePath = String.format("%s%sfiles%s%s.csv",dbCatalog.databaseDir,File.separator,File.separator,tableName);
		File dbFile = new File(filePath);
//		File dbFile = new File(dbCatalog.databaseDir + File.separator + "files" + File.separator + tableName + ".csv");
		try {
			// Try to create a BufferedReader to read from the CSV file
			bufferedReader = new BufferedReader(new FileReader(dbFile));
		} catch (Exception e) {
			// If an exception occurs while setting up the BufferedReader, print an error message and the stack trace
			System.out.println("Datalog load failed");
			e.printStackTrace();
		}
	}
	/**
	 * Retrieves the next tuple from the table.
	 * Reads the next line from the CSV file, parses the values, and returns a Tuple object.
	 *
	 * @return A Tuple object with the values from the current line, or null if no more lines.
	 */
	@Override
	public Tuple getNextTuple() {
		// Read the next line from the CSV file and store it in currentLine
		readNextLine();

		// If currentLine is null (i.e., the end of the file is reached), return null
		if (currentLine == null) {
			return null;
		}

		// Parse the current line and store the values in the values list
		parseCurrentLine();

		// Return a new Tuple object containing the tableName, columnNames, columnTypes, and values
		return new Tuple(tableName, columnNames, columnTypes, values);
	}
	/**
 * Reads the next line from the CSV file
 *  * and stores it in the currentLine variable.
 *  * If the BufferedReader is null, it sets the currentLine to null.
 *  */
	private void readNextLine() {
		// Try to read the next line from the CSV file using the bufferedReader
		try {
			currentLine = bufferedReader == null ? null : bufferedReader.readLine();
		} catch (Exception e) {
			// If an exception occurs while reading the next line, print the stack trace
			e.printStackTrace();
		}
	}
	/**
	 * Parses the currentLine read from the CSV file.
	 * Splits the line by commas and trims the values, storing them in the values list.
	 */
	private void parseCurrentLine() {
		// Split the currentLine by commas to obtain individual column values
		String[] columnValues = currentLine.split(",");
		// Initialize the values list
		values = new ArrayList<>();
		// Iterate through the columnValues array
		for (String value : columnValues) {
			// Trim each value and add it to the values list
			values.add(value.trim());
		}
	}
	/**
	 * Resets the ScanOperator to start reading the table from the beginning.
	 * Re-initializes the BufferedReader to the start of the CSV file.
	 */
	@Override
	public void reset() {
		// Re-setup the BufferedReader to read from the beginning of the CSV file
		setupBufferedReader();
	}
	/**
	 * Reads and processes all tuples in the table.
	 * Continuously retrieves the next tuple and prints its contents until there are no more tuples.
	 */
	@Override
	public void dump() {
		// Declare a Tuple variable to store each tuple retrieved by getNextTuple()
		Tuple tuple;
		while ((tuple = getNextTuple()) != null) {
			System.out.println(tuple);
		}
	}
}