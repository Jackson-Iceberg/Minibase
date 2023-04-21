package ed.inf.adbs.minibase;

import ed.inf.adbs.minibase.base.*;
import ed.inf.adbs.minibase.parser.QueryParser;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * @author Jackson Zhou
 * @create 20/03/2023-20:30
 * @projectName Minibase
 */

/**
 * Minimization of conjunctive queries
 */
public class CQMinimizer {

    /**
     * The main method serves as the entry point for the program. It expects two command-line arguments:
     * the input file is the path that containing the conjunctive query
     * the output file is the path to save the minimized query.
     * It calls the minimizeCQ method to perform the minimization.
     *
     * @param args Command-line arguments array, expecting two arguments: input_file and output_file.
     */
    public static void main(String[] args) {
        // Check if the number of command-line arguments is correct
        if (args.length != 2) {
            System.err.println("Usage: CQMinimizer input_file output_file");
            return;
        }

        // Assign the input and output file paths from command-line arguments
        String inputFile = args[0];
        String outputFile = args[1];

        // Call the minimizeCQ method to perform query minimization
        minimizeCQ(inputFile, outputFile);
    }

    /**
     * This function minimizes a Conjunctive Query (CQ) by iteratively removing atoms from the query body,
     * while ensuring that the free variables of the query remain unchanged and there exists a homomorphism
     * between the original and the minimized query.
     *
     * @param inputFile  The input file that contains the CQ to be minimized.
     * @param outputFile The output file where the minimized CQ will be written.
     */
    public static void minimizeCQ(String inputFile, String outputFile) {
        // Parse the input CQ.
        Query query;
        try {
            query = QueryParser.parse(Paths.get(inputFile));
        } catch (Exception e) {
            System.err.println("An exception occurred while parsing the input CQ.");
            e.printStackTrace();
            return;
        }

        List<Atom> body = query.getBody();
        boolean changed;
        do {
            changed = false;

            // Iterate over atoms in the query body.
            for (int i = 0; i < body.size(); i++) {
                Query tempQuery = removeAtomFromQuery(query, i);

                // Check if the free variables are still contained and if there is a homomorphism.
                if (checkFreeVariableContain(query.getHead(), tempQuery.getBody()) && checkQueryHomo(query, tempQuery)) {
                    query = tempQuery;
                    body = query.getBody();
                    changed = true;
                    break;
                }
            }
        } while (changed);

        // Write the resulting minimized query to the output file.
        writeFile(query, outputFile);
    }

    /**
     * This function removes a given atom from a Conjunctive Query (CQ).
     *
     * @param query The CQ from which the atom is to be removed.
     * @param index The index of the atom to be removed from the query.
     * @return A new CQ with the specified atom removed.
     */
    private static Query removeAtomFromQuery(Query query, int index) {
        List<Atom> body = query.getBody();
        List<Atom> tempBody = new ArrayList<>(body);
        tempBody.remove(index);
        return new Query(query.getHead(), tempBody);
    }

    /**
     * Writes a Query object to a file.
     *
     * @param query      The Query object to write.
     * @param outputFile The file path to write the Query object to.
     */
    private static void writeFile(Query query, String outputFile) {
        // Create the output file and its parent directories if they don't exist.
        File file = new File(outputFile);
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }

        // Use try-with-resources to automatically close the FileWriter when done.
        try (FileWriter fw = new FileWriter(outputFile)) {
            // Write the query to the output file.
            fw.write(query.toString());
        } catch (IOException e) {
            System.err.println("Exception occurred during writing query back to file");
            e.printStackTrace();
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
     * Checks if there is a homomorphism between two given queries.
     *
     * @param q1 The first query.
     * @param q2 The second query.
     * @return true if there is a homomorphism between the queries, false otherwise.
     */
    private static boolean checkQueryHomo(Query q1, Query q2) {
        // Create a new HashMap to store the current variable mapping between the two queries.
        HashMap<Variable, Variable> currentMap = new HashMap<>();

        // Call the depth-first search function to check for a homomorphism between the two queries.
        return dfsSearch(q1, q2, 0, currentMap);
    }

    /**
     * Checks if all free variables in the head are contained within the body.
     *
     * @param head The head of the query.
     * @param body The body of the query, represented as a list of atoms.
     * @return true if all free variables in the head are contained within the body, false otherwise.
     */
    private static boolean checkFreeVariableContain(Head head, List<Atom> body) {
        // Create a HashSet to store the free variables in the head.
        HashSet<Variable> freeVariables = new HashSet<>(head.getVariables());

        // If it is a boolean query (no free variables in the head), directly return true.
        if (freeVariables.isEmpty()) {
            return true;
        }

        // Create a HashSet to store the variables in the body.
        HashSet<Variable> variables = new HashSet<>();

        // Iterate through the atoms in the body and add their variables to the variables set.
        for (Atom a : body) {
            if (a instanceof RelationalAtom) {
                RelationalAtom ra = (RelationalAtom) a;
                for (Term term : ra.getTerms()) {
                    if (term instanceof Variable) {
                        variables.add((Variable) term);
                    }
                }
            }
        }

        // Check if the variables set contains all the free variables from the head.
        return variables.containsAll(freeVariables);
    }

    /**
     * Depth-first search for checking if there is a homomorphism between two queries.
     *
     * @param q1         The first query.
     * @param q2         The second query.
     * @param q1Index    The current index of the atom in q1's body.
     * @param currentMap The current variable mapping between q1 and q2.
     * @return true if a homomorphism exists between q1 and q2, false otherwise.
     */
    private static boolean dfsSearch(Query q1, Query q2, int q1Index, HashMap<Variable, Variable> currentMap) {
        List<Atom> body1 = q1.getBody();
        List<Atom> body2 = q2.getBody();

        // If all atoms in body1 have been checked, verify if the head atoms are equal.
        if (q1Index == body1.size()) {
            return checkHeadEqual(q1.getHead(), q2.getHead(), currentMap);
        }

        RelationalAtom ra = (RelationalAtom) body1.get(q1Index);

        // Iterate through the atoms in body2.
        for (Atom atom : body2) {
            RelationalAtom body2Atom = (RelationalAtom) atom;
            HashMap<Variable, Variable> newMap = homoBetween2Atom(ra, body2Atom);

            // Check if a homomorphism exists between the current atoms and if it can be merged with the previous mappings.
            if (newMap != null && checkMerge(currentMap, newMap)) {
                // Merge the current mapping with the new mapping and continue the search.
                HashMap<Variable, Variable> mergedMap = new HashMap<>(currentMap);
                mergedMap.putAll(newMap);

                // Recursively search with the next atom in body1 and the merged mapping.
                if (dfsSearch(q1, q2, q1Index + 1, mergedMap)) {
                    return true;
                }
            }
        }
        // If no suitable homomorphism is found for the current atom in body1, return false.
        return false;
    }

    /**
     * Checks if two head atoms are equal, considering the variable mappings.
     *
     * @param head1    The first head atom.
     * @param head2    The second head atom.
     * @param mappings The variable mappings between the two head atoms.
     * @return true if the head atoms are equal considering the variable mappings, false otherwise.
     */
    private static boolean checkHeadEqual(Head head1, Head head2, HashMap<Variable, Variable> mappings) {
        // If predicate names are not equal, return false early.
        if (!checkHeadNameEqual(head1, head2)) {
            return false;
        }

        List<Variable> variableList1 = head1.getVariables();
        List<Variable> mappedVars1 = new ArrayList<>();
        // Iterate through the variables in variableList1 and add their mappings to the mappedVars1 list.
        for (Variable var : variableList1) {
            mappedVars1.add(mappings.get(var));
        }
        // Check if the mapped variables are equal.
        return mappedVars1.equals(head2.getVariables());
    }

    /**
     * Checks if the names of the two given head atoms are equal.
     *
     * @param head1 The first head atom.
     * @param head2 The second head atom.
     * @return true if the names of the two head atoms are equal, false otherwise.
     */
    private static boolean checkHeadNameEqual(Head head1, Head head2) {
        return head1.getName().equals(head2.getName());
    }


    /**
     * Checks if two variable mappings can be merged without conflicts.
     *
     * @param map1 The first variable mapping.
     * @param map2 The second variable mapping.
     * @return true if the variable mappings can be merged, false otherwise.
     */
    private static boolean checkMerge(HashMap<Variable, Variable> map1, HashMap<Variable, Variable> map2) {
        // Create a set of keys from both map1 and map2.
        HashSet<Variable> intersection = new HashSet<>(map1.keySet());

        // Retain only the keys that are present in both sets, resulting in the intersection of the key sets.
        intersection.retainAll(map2.keySet());

        // Iterate through the intersection set, checking if the values for the shared keys are the same.
        for (Variable sharedKey : intersection) {
            // If the values for a shared key are not equal, the maps cannot be merged, so return false.
            if (!map1.get(sharedKey).equals(map2.get(sharedKey))) {
                return false;
            }
        }

        // If all shared keys have the same values, the maps can be merged, so return true.
        return true;
    }

    /**
     * Determines if there's a homomorphism between two relational atoms.
     *
     * @param ra1 The first relational atom.
     * @param ra2 The second relational atom.
     * @return A variable mapping if a homomorphism exists between the two relational atoms, null otherwise.
     */
    private static HashMap<Variable, Variable> homoBetween2Atom(RelationalAtom ra1, RelationalAtom ra2) {
        // If two atoms have different names or different term sizes, directly return null.
        if (!checkAtomsNameEqual(ra1, ra2) || !checkAtomTermsSizeEqual(ra1, ra2)) {
            return null;
        }

        // Create a variable mapping between the terms of ra1 and ra2.
        HashMap<Variable, Variable> map = new HashMap<>();
        for (int i = 0; i < ra1.getTerms().size(); i++) {
            map.put(new Variable(ra1.getTerms().get(i).toString()), new Variable(ra2.getTerms().get(i).toString()));
        }

        // If any integer constant is not mapped to itself, return null.
        for (Variable key : map.keySet()) {
            if (isInteger(key.toString()) && !key.equals(map.get(key))) {
                return null;
            }
        }

        // If all integer constants are mapped to themselves, return the variable mapping.
        return map;
    }

    /**
     * Checks if the names of the two given relational atoms are equal.
     *
     * @param ra1 The first relational atom.
     * @param ra2 The second relational atom.
     * @return true if the names of the two relational atoms are equal, false otherwise.
     */
    private static boolean checkAtomsNameEqual(RelationalAtom ra1, RelationalAtom ra2) {
        return ra1.getName().equals(ra2.getName());
    }

    /**
     * Checks if the terms size of the two given relational atoms are equal.
     *
     * @param ra1 The first relational atom.
     * @param ra2 The second relational atom.
     * @return true if the terms size of the two relational atoms are equal, false otherwise.
     */
    private static boolean checkAtomTermsSizeEqual(RelationalAtom ra1, RelationalAtom ra2) {
        return ra1.getTerms().size() == ra2.getTerms().size();
    }


}
