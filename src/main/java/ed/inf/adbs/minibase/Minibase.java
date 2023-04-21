package ed.inf.adbs.minibase;

import ed.inf.adbs.minibase.Operator.*;
import ed.inf.adbs.minibase.base.*;
import ed.inf.adbs.minibase.parser.QueryParser;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * In-memory database, can operate join, scan, select, project, sum.
 *
 * @author jackson-zhou
 */
public class Minibase {

    /**
     * The main entry point of the application.
     * It takes command line arguments for the database directory, input file, and output file,
     * then calls the evaluateCQ method to evaluate the query and write the results to the output file.
     *
     * @param args Command line arguments:
     *             args[0] - database directory
     *             args[1] - input file containing the query
     *             args[2] - output file to write the query results
     */
    public static void main(String[] args) {

        // Check if the correct number of command line arguments is provided
        if (args.length != 3) {
            System.err.println("Usage: Minibase database_dir input_file output_file");
            return;
        }

        // Assign command line arguments to variables
        String databaseDir = args[0];
        String inputFile = args[1];
        String outputFile = args[2];

        // Evaluate the query and write results to the output file
        evaluateCQ(databaseDir, inputFile, outputFile);

//        evaluateCQ(databaseDir, inputFile, outputFile);
//        for (int i = 2; i < 10; i++) {
//            String index = Integer.toString(i);
//            inputFile = inputFile.replace((i - 1) + "", index);
//            outputFile = outputFile.replace((i - 1) + "", index);
//            evaluateCQ(databaseDir, inputFile, outputFile);
//        }

    }

    /**
     * Main method for evaluating a conjunctive query (CQ).
     *
     * @param databaseDir The directory path containing the database schema and relations.
     * @param inputFile   The file path containing the input query to be evaluated.
     * @param outputFile  The file path where the query results should be written.
     */
    public static void evaluateCQ(String databaseDir, String inputFile, String outputFile) {
        // Create a HashMap to store the database catalog schema mapping
        HashMap<String, List<String>> dbCatalogMapper = new HashMap<>();

        // Create an ArrayList to store the tuples
        ArrayList<Tuple> tupleList = new ArrayList<>();

        // Generate the database catalog schema mapping
        dbCatalogMapGenerator(databaseDir, dbCatalogMapper);

        // Create a DatabaseCatalog object using the generated schema mapping, tuple list, and database directory
        DatabaseCatalog dbCatalog = new DatabaseCatalog(dbCatalogMapper, tupleList, databaseDir);

        // Declare a Head object to store the query's head (the output variables)
        Head head = null;

        // Declare a list to store the relational atoms from the query body
        List<RelationalAtom> relationBody = new ArrayList<>();

        // Declare a list to store the comparison atoms from the query body
        List<ComparisonAtom> comparisonBody = new ArrayList<>();

        // Parse the input query and separate it into head, relation body, and comparison body
        try {
            Query query = QueryParser.parse(Paths.get(inputFile));
            head = query.getHead();
            // Separates relational and comparison atoms from the query body.
            separateAtoms(query.getBody(), relationBody, comparisonBody);
        } catch (Exception e) {
            System.err.println("Exception occurred during parsing");
            e.printStackTrace();
        }

        // Execute the query plan to get the results
        executeQueryPlan(head, relationBody, comparisonBody, dbCatalog);
        //Checks if the sum aggregate function is present in the query head and applies the SumOperator if required.
        checkSumOperator(head, dbCatalog);
        //Write the result.
        writeToFile(outputFile, dbCatalog);
    }

    /**
     * Generates the database catalog schema mapping.
     * This function reads the schema file from the database directory and
     * parses each line, updating the provided HashMap with the schema mapping.
     *
     * @param databaseDir The directory path containing the database schema file.
     * @param dbTypeMapper The HashMap to store the schema mapping.
     */
    private static void dbCatalogMapGenerator(String databaseDir, HashMap<String, List<String>> dbTypeMapper) {
        // Create a File object for the schema file in the database directory
        File schemaFile = new File(databaseDir + File.separator + "schema.txt");

        // Read and parse the schema file
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(schemaFile))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                parseSchemaLine(line, dbTypeMapper);
            }
        } catch (Exception e) {
            System.err.println("Database Catalogue Fail to Load File");
            e.printStackTrace();
        }
    }

    /**
     * Parses a line from the schema file and updates the provided HashMap
     * with the schema mapping.
     * This function takes a line from the schema file and splits it into its
     * components. It then adds the mapping to the HashMap.
     *
     * @param line The line from the schema file to be parsed.
     * @param dbCatalogMapper The HashMap to store the schema mapping.
     */
    private static void parseSchemaLine(String line, HashMap<String, List<String>> dbCatalogMapper) {
        // Split the line into parts using whitespace as a delimiter
        String[] schemaLineParts = line.split("\\s+");

        // Create a list of schema attributes by removing the first element (table name)
        List<String> schemaAttributes = new ArrayList<>(Arrays.asList(schemaLineParts));
        schemaAttributes.remove(0);

        // Add the table name and its attributes to the HashMap
        dbCatalogMapper.put(schemaLineParts[0], schemaAttributes);
    }

    /**
     * Checks if the sum aggregate function is present in the query head and
     * applies the SumOperator if required.
     * This function examines the head of the query for the presence of a sum
     * aggregate function. If found and the tuple list is not empty, it applies
     * the SumOperator to perform the aggregation.
     *
     * @param head      The Head object representing the query's head.
     * @param dbCatalog The DatabaseCatalog object containing the database schema and relations.
     */
    private static void checkSumOperator(Head head, DatabaseCatalog dbCatalog) {
        // Check if the tuple list is not empty and if the head has a sum aggregate function
        if ((dbCatalog.getTupleList().size() > 0) && (head.getSumAggregate() != null)) {
            // Create and apply the SumOperator using the head and the database catalog
            SumOperator sumOperator = new SumOperator(head, dbCatalog);
        }
    }

    /**
     * Separates relational and comparison atoms from the query body.
     * This function processes the list of atoms in the query body and
     * categorizes them into relational atoms and comparison atoms, adding
     * them to their respective lists.
     *
     * @param body           The list of atoms from the query body.
     * @param relationBody   The list to store the relational atoms.
     * @param comparisonBody The list to store the comparison atoms.
     */
    private static void separateAtoms(List<Atom> body, List<RelationalAtom> relationBody, List<ComparisonAtom> comparisonBody) {
        // Iterate through each atom in the query body
        for (Atom atom : body) {
            try {
                // Try to cast the atom to a RelationalAtom and add it to the relationBody list
                relationBody.add((RelationalAtom) atom);
            } catch (Exception e) {
                // If casting fails, cast the atom to a ComparisonAtom and add it to the comparisonBody list
                comparisonBody.add((ComparisonAtom) atom);
            }
        }
    }

    /**
     * Executes the query plan and stores the results in the DatabaseCatalog.
     * This function creates a QueryPlan object using the query's head, relation body,
     * comparison body, and the DatabaseCatalog. It then iterates through the tuples
     * generated by the query plan's operator, adding them to the DatabaseCatalog.
     *
     * @param head           The Head object representing the query's head.
     * @param relationBody   The list of relational atoms from the query body.
     * @param comparisonBody The list of comparison atoms from the query body.
     * @param dbCatalog      The DatabaseCatalog object containing the database schema and relations.
     */
    private static void executeQueryPlan(Head head, List<RelationalAtom> relationBody, List<ComparisonAtom> comparisonBody, DatabaseCatalog dbCatalog) {
        // Create a QueryPlan object using the head, relation body, comparison body, and the database catalog
        QueryPlan queryPlan = new QueryPlan(head, relationBody, comparisonBody, dbCatalog);

        // Retrieve the operator for the query plan
        Operator operator = queryPlan.getOperator();

        // Get the first tuple from the operator
        Tuple tuple = operator.getNextTuple();

        // Iterate through the tuples generated by the operator
        while (tuple != null) {
            // Check if the tuple is valid (not marked as "NonValid")
            if (!tuple.getTableName().equals("NonValid")) {
                // Add the valid tuple to the DatabaseCatalog's tuple list
                dbCatalog.addTupleList(tuple);
            }
            // Get the next tuple from the operator
            tuple = operator.getNextTuple();
        }
    }

    /**
     * Writes the tuples from the DatabaseCatalog to a CSV file.
     * It iterates through the tuples in the DatabaseCatalog, builds CSV lines using buildCsvLine method,
     * and writes them to the output file using a FileWriter.
     *
     * @param outputFile The path of the output file where the results will be written
     * @param dbCatalog  The DatabaseCatalog containing the tuples to be written to the output file
     */
    private static void writeToFile(String outputFile, DatabaseCatalog dbCatalog) {
        // Create the output file and its parent directories if they do not exist
        File csvFile = new File(outputFile);
        if (!csvFile.getParentFile().exists()) {
            csvFile.getParentFile().mkdirs();
        }

        // Use try-with-resources to handle FileWriter resource
        try (FileWriter fileWriter = new FileWriter(csvFile)) {

            // Iterate through the tuples in the DatabaseCatalog
            for (int i = 0; i < dbCatalog.getTupleList().size(); i++) {
                Tuple tuple = dbCatalog.getTupleList().get(i);

                // Check if the tuple is empty and skip it
                if (tuple.getValue().isEmpty()) {
                    continue;
                }

                String line = buildCsvLine(tuple);
                // Write the line to the CSV file
                fileWriter.write(line);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Builds a CSV line from a given tuple.
     * This function iterates through the values in the tuple, appends them to a StringBuilder,
     * and inserts comma separators between the values, except for the last value.
     * It also adds a newline character at the end of the line.
     *
     * @param tuple The tuple whose values will be used to build the CSV line
     * @return A string representing the CSV line created from the tuple's values
     */
    private static String buildCsvLine(Tuple tuple) {
        // Create a StringBuilder to build the CSV line
        StringBuilder line = new StringBuilder();

        // Retrieve the values from the tuple
        List<String> values = tuple.getValue();

        // Iterate through the values in the tuple
        for (int i = 0; i < values.size(); i++) {
            // Append the current value to the line
            line.append(values.get(i));

            // Add a comma separator between values, except for the last value
            if (i != values.size() - 1) {
                line.append(", ");
            }
        }

        // Add a newline character at the end of the line
        line.append("\n");

        // Return the built CSV line as a string
        return line.toString();
    }

}
