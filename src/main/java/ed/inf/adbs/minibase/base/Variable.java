package ed.inf.adbs.minibase.base;

import java.util.Objects;

public class Variable extends Term {
    private String name;

    public Variable(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Variable other = (Variable) obj;
        // Compare the properties of the two Variable objects to determine equality
        // For example, if your Variable class has a 'name' property, you can compare them like this:
        return Objects.equals(name, other.name);
    }

    @Override
    public int hashCode() {
        // Use the properties of the Variable class to calculate the hashCode
        // For example, if your Variable class has a 'name' property, you can use it like this:
        return Objects.hash(name);
    }
}
