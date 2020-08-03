package org.apache.hadoop.yarn.server.resourcemanager.placement;

import java.util.Arrays;

public class MappingRuleMatchers {
  public static class MatchAllMatcher implements MappingRuleMatcher {
    @Override
    public boolean match(VariableContext variables) {
      return true;
    }

    @Override
    public String toString() {
      return "MatchAllMatcher";
    }
  }

  public static class VariableMatcher implements MappingRuleMatcher {
    private String variable;
    private String value;

    VariableMatcher(String variable, String value) {
      this.variable = variable;
      this.value = value == null ? "" : value;
    }

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

  public static class AndMatcher implements MappingRuleMatcher {
    private MappingRuleMatcher matchers[];

    AndMatcher(MappingRuleMatcher ...matchers) {
      this.matchers = matchers;
    }

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

  public static class OrMatcher implements MappingRuleMatcher {
    private MappingRuleMatcher matchers[];

    OrMatcher(MappingRuleMatcher ...matchers) {
      this.matchers = matchers;
    }

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

  public static MappingRuleMatcher createUserMatcher(String userName) {
    return new VariableMatcher("%user", userName);
  }

  public static MappingRuleMatcher createGroupMatcher(String groupName) {
    return new VariableMatcher("%primary_group", groupName);
  }

  public static MappingRuleMatcher createUserGroupMatcher(
      String userName, String groupName) {
    return new AndMatcher(
        createUserMatcher(userName),
        createGroupMatcher(groupName));
  }

  public static MappingRuleMatcher createApplicationNameMatcher(String name) {
    return new VariableMatcher("%application", name);
  }

  public static MappingRuleMatcher createAllMatcher() {
    return new MatchAllMatcher();
  }
}
