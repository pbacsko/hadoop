package org.apache.hadoop.yarn.server.resourcemanager.placement;

import com.google.common.collect.ImmutableSet;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class is a key-value store for the variables and their respective values
 * during an application placement. The class gives support for immutable
 * variables, which can be set only once, and has helper methods for replacing
 * the variables with their respective values in provided strings.
 * We don't extend the map interface, because we don't need all the features
 * a map provides, this class try to be as simple as possible.
 */
public class VariableContext {
  /**
   * This is our actual variable store,
   */
  private Map<String, String> variables = new HashMap<>();
  /**
   * This set contains the names of the immutable variables if null it is
   * ignored
   */
  private Set<String> immutableNames;

  /**
   * Checks if the provided variable is immutable.
   * @param name Name of the variable to check
   * @return true if the variable is immutable
   */
  boolean isImmutable(String name) {
    return (immutableNames != null && immutableNames.contains(name));
  }

  /**
   * Can be used to provide a set which contains the name of the variables which
   * should be immutable
   * @param immutableNames Set containing the names of the immutable variables
   * @throws IllegalStateException if the immutable set is already provided.
   * @return same instance of VariableContext for daisy chaining.
   */
  public VariableContext setImmutables(Set<String> immutableNames) {
    if (this.immutableNames != null) {
      throw new IllegalStateException("Immutable variables are already defined,"
          + " variable immutability cannot be changed once set!");
    }
    this.immutableNames = ImmutableSet.copyOf(immutableNames);
    return this;
  }

  /**
   * Can be used to provide a set which contains the name of the variables which
   * should be immutable
   * @param immutableNames Set containing the names of the immutable variables
   * @throws IllegalStateException if the immutable set is already provided.
   * @return same instance of VariableContext for daisy chaining.
   */
  public VariableContext setImmutables(String... immutableNames) {
    if (this.immutableNames != null) {
      throw new IllegalStateException("Immutable variables are already defined,"
          + " variable immutability cannot be changed once set!");
    }
    this.immutableNames = ImmutableSet.copyOf(immutableNames);
    return this;
  }

  /**
   * Adds a variable with value to the context or overrides an already existing
   * one. If the variable is already set and immutable and IllegalStateException
   * is thrown.
   * @param name Name of the variable to be added to the context
   * @param value Value of the variable
   * @throws IllegalStateException if the variable is immutable and already set
   * @return same instance of VariableContext for daisy chaining.
   */
  public VariableContext put(String name, String value) {
    if (variables.containsKey(name) && isImmutable(name)) {
      throw new IllegalStateException(
          "Variable '" + name + "' is immutable, cannot update it's value!");
    }
    variables.put(name, value);
    return this;
  }

  /**
   * Returns the value of a variable, null values are replaced with ""
   * @param name Name of the variable
   * @return The value of the variable
   */
  public String get(String name) {
    String ret = variables.get(name);
    return ret == null ? "" : ret;
  }

  /**
   * Check if a variable is part of the context
   * @param name Name of the variable to be checked
   * @return True if the variable is added to the context, false otherwise
   */
  public boolean containsKey(String name) {
    return variables.containsKey(name);
  }

  /**
   * This method replaces all variables in the provided string. The variables
   * are reverse ordered by the length of their names in order to avoid partial
   * replaces when a shorter named variable is a substring of a longer named
   * variable.
   * All variables will be replaced in the string.
   * Null values will be considered as empty strings during the replace.
   * If the input is null, always null will be returned.
   * @param input The string with variables
   * @return A string with all the variables substituted with their respective
   *         values.
   */
  public String replaceVariables(String input) {
    if (input == null) return null;

    String[] keys = variables.keySet().toArray(new String[]{});
    //Replacing variables starting longest first, to avoid collision when a
    //shorter variable name matches the beginning of a longer one.
    //e.g. %user_something, if %user is defined it may replace the %user before
    //we would reach the %user_something variable, so we start with the longer
    //names first
    Arrays.sort(keys, (a,b) -> b.length() - a.length());

    String ret = input;
    for (String key : keys) {
      //we cannot match for null, so we just skip if we have a variable "name"
      //with null
      if (key == null) continue;
      ret = ret.replace(key, get(key));
    }

    return ret;
  }

  /**
   * This method will consider the input as a queue path, which is a '.'
   * separated string. The input will be split along the dots ('.') and all
   * parts will be replaced individually. Replace only occur if a part exactly
   * matches a variable name, no composite names or additional characters are
   * supported.
   * e.g. With variables %user and %default "%user.%default" will be substituted
   * while "%user%default.something" won't.
   * Null values will be considered as empty strings during the replace.
   * If the input is null, always null will be returned.
   * @param input The string with variables
   * @return A string with all the variable only path parts substituted with
   *         their respective values.
   */
  public String replacePathVariables(String input) {
    if (input == null) return null;

    String[] parts = input.split("\\.");
    for (int i = 0; i < parts.length; i++) {
      //if the part is a variable it should be in the map, otherwise we keep
      //it's original value. This means undefined variables will return the
      //name of the variable, but this is working as intended.
      String newVal = variables.getOrDefault(parts[i], parts[i]);
      //if a variable's value is null, we use empty string instead
      if (newVal == null) {
        newVal = "";
      }
      parts[i] = newVal;
    }

    return String.join(".", parts);
  }

}
