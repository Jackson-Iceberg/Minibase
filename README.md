# Minibase

#### DatabaseCatalog

The `DatabaseCatalog` class is part of the `ed.inf.adbs.minibase.Operator` package and serves as a container for storing essential information about the database schema, the tuple list, and the database directory path. This class is designed to provide methods for accessing and modifying the schema, tuple list, and database directory.

#### JoinOperator

The `JoinOperator` class, found in the `ed.inf.adbs.minibase.Operator` package, is responsible for performing join operations on a given list of relational atoms and comparison atoms. The class is part of a minimalist database management system and extends the `Operator` class. The join operation combines multiple tuples from different relational atoms according to the specified comparison atoms, creating a new tuple as a result.

#### Operator

The `Operator` class is an abstract base class located in the `ed.inf.adbs.minibase.Operator` package. It serves as a foundation for various types of operators used in the query execution process. The class provides a common interface for interacting with operators, which allows for retrieving the next tuple, resetting the operator state, and dumping the output. The `Operator` class acts as a blueprint for its concrete subclasses, ensuring they implement the required methods.

#### ProjectOperator

The `ProjectOperator` class, located in the `ed.inf.adbs.minibase.Operator` package, is a concrete implementation of the `Operator` abstract class. It is responsible for performing the projection operation on tuples. The projection operation eliminates duplicate columns, orders columns according to the query, and returns distinct tuples.

#### QueryPlan

The `QueryPlan` class, located in the `ed.inf.adbs.minibase.Operator` package, is responsible for constructing an optimized query plan for a given query. The query plan consists of various operators, such as Scan, Select, Join, and Project. 

#### ScanOperator

The `ScanOperator` class, located in the `ed.inf.adbs.minibase.Operator` package, is responsible for scanning tuples in tables. It extends the abstract `Operator` class and implements the required methods for processing tuples. This class is used for reading and processing data from CSV files representing tables in the database.

#### SelectOperator

The `SelectOperator` class is an implementation of the `Operator` abstract class, specifically designed to filter tuples from an underlying operator based on a list of selection conditions. These selection conditions are represented as `ComparisonAtom` objects. The class also provides functionality to support filtering tuples within a `JoinOperator`.

The primary purpose of the `SelectOperator` is to iterate through the tuples provided by the input operator, and determine if they satisfy the given conditions specified by the list of `ComparisonAtom` objects. If a tuple meets these conditions, it is included in the output. Otherwise, it is filtered out.

#### SumOperator

The `SumOperator` class is part of the `ed.inf.adbs.minibase.Operator` package and is responsible for performing the SUM aggregation operation on a given set of tuples. This operation can include either group-by aggregation or simple summation, depending on the input. The class handles both constant and variable aggregation scenarios, as well as removing duplicate tuples when necessary.

#### Tuple

The `Tuple` class represents a single row or record in a relational database table. It contains information about the table name, column names, column types, and values for each column in the tuple. This class provides a structured way to store and manipulate tuples as they are processed within the `ed.inf.adbs.minibase` system.

## Optimisation:

Assuming the select operator's selectivity is 0.5, and both the left and right subtrees in the join operation have 'n' tuples each, this design is efficient. By first performing a join and then applying the select operator, the number of operations required would be approximately n². However, if we first apply the select operator to half of the tuples and then execute the join operation, the number of operations would be reduced to just 0.25n².

 