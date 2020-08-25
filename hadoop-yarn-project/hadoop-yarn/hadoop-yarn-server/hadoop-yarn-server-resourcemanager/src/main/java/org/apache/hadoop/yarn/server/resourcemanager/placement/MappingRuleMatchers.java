/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.placement;

import java.util.Arrays;

/**
 * This class contains all the matcher and some helper methods to generate them.
 */
public class MappingRuleMatchers {

  /**
   * MatchAllMatcher is a matcher which matches everything.
   */
  public static class MatchAllMatcher implements MappingRuleMatcher {
    /**
     * The match will return true in all cases, to match all submissions
     * @param variables The variable context, which contains all the variables
     * @return true
     */
    @Override
    public boolean match(VariableContext variables) {
      return true;
    }

    @Override
    public String toString() {
      return "MatchAllMatcher";
    }
  }

  /**
   * VariableMatcher will check if a provided variable's value matches the
   * provided value. The provided value might contain variables as well, which
   * will get evaluated before the comparison.
   */
  public static class VariableMatcher implements MappingRuleMatcher {
    /**
     * Name of the variable to be checked
     */
    private String variable;
    /**
     * The value which should match the variable's value
     */
    private String value;

    VariableMatcher(String variable, String value) {
      this.variable = variable;
      this.value = value == null ? "" : value;
    }

    /**
     * The method will replace all variables in the value, then compares this
     * substituted value against the variable's value, if they match we return
     * true.
     * If the variable is null we always return false.
     * @param variables The variable context, which contains all the variables
     * @return true if the value matches the variable's value, false otherwise
     */
    @Override
    public boolean match(VariableContext variables) {
      if (variable == null) {
        return false;
      }

      String substituted = variables.replaceVariables(value);
      return substituted.equals(variables.get(variable));
    }

    @Override
    public String toString() {
      return "VariableMatcher{" +
        "variable='" + variable + '\'' +
        ", value='" + value + '\'' +
        '}';
    }
  }

  /**
   * AndMatcher is a basic boolean matcher which takes multiple other
   * matcher as it's arguments, and on match it checks if all of them are true
   */
  public static class AndMatcher implements MappingRuleMatcher {
    /**
     * The list of matchers to be checked during evaluation
     */
    private MappingRuleMatcher matchers[];

    /**
     * Constructor
     * @param matchers List of matchers to be checked during evaluation
     */
    AndMatcher(MappingRuleMatcher ...matchers) {
      this.matchers = matchers;
    }

    /**
     * This match method will go through all the provided matchers and call
     * their match method, if all match we return true
     * @param variables The variable context, which contains all the variables
     * @return true if all matchers match
     */
    @Override
    public boolean match(VariableContext variables) {
      for (MappingRuleMatcher matcher : matchers) {
        if (!matcher.match(variables)) {
          return false;
        }
      }

      return true;
    }

    @Override
    public String toString() {
      return "AndMatcher{" +
          "matchers=" + Arrays.toString(matchers) +
          '}';
    }
  }

  /**
   * OrMatcher is a basic boolean matcher which takes multiple other
   * matcher as it's arguments, and on match it checks if any of them are true
   */
  public static class OrMatcher implements MappingRuleMatcher {
    /**
     * The list of matchers to be checked during evaluation
     */
    private MappingRuleMatcher matchers[];

    /**
     * Constructor
     * @param matchers List of matchers to be checked during evaluation
     */
    OrMatcher(MappingRuleMatcher ...matchers) {
      this.matchers = matchers;
    }

    /**
     * This match method will go through all the provided matchers and call
     * their match method, if any of them match we return true
     * @param variables The variable context, which contains all the variables
     * @return true if any of the matchers match
     */
    @Override
    public boolean match(VariableContext variables) {
      for (MappingRuleMatcher matcher : matchers) {
        if (matcher.match(variables)) {
          return true;
        }
      }

      return false;
    }

    @Override
    public String toString() {
      return "OrMatcher{" +
          "matchers=" + Arrays.toString(matchers) +
          '}';
    }
  }

  /**
   * Convenience method to create a variable matcher which matches against the
   * username.
   * @param userName The username to be matched
   * @return VariableMatcher with %user as the variable
   */
  public static MappingRuleMatcher createUserMatcher(String userName) {
    return new VariableMatcher("%user", userName);
  }

  /**
   * Convenience method to create a variable matcher which matches against the
   * user's primary group.
   * @param groupName The groupName to be matched
   * @return VariableMatcher with %primary_group as the variable
   */
  public static MappingRuleMatcher createGroupMatcher(String groupName) {
    return new VariableMatcher("%primary_group", groupName);
  }

  /**
   * Convenience method to create a composite matcher which matches agains the
   * user's user name and the user's primary group. Only matches if both matches
   * @param userName The username to be matched
   * @param groupName The groupName to be matched
   * @return AndMatcher with two matchers one for userName and one for
   * primaryGroup
   */
  public static MappingRuleMatcher createUserGroupMatcher(
      String userName, String groupName) {
    return new AndMatcher(
        createUserMatcher(userName),
        createGroupMatcher(groupName));
  }

  /**
   * Convenience method to create a variable matcher which matches against the
   * submitted application's name.
   * @param name The name to be matched
   * @return VariableMatcher with %application as the variable
   */
  public static MappingRuleMatcher createApplicationNameMatcher(String name) {
    return new VariableMatcher("%application", name);
  }


  /**
   * Convenience method to create a matcher that matches all
   * @return MatchAllMatcher
   */
  public static MappingRuleMatcher createAllMatcher() {
    return new MatchAllMatcher();
  }
}
