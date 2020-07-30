package org.apache.hadoop.yarn.server.resourcemanager.placement;

public class MappingRuleMatchers {
  public static class MatchAllMatcher implements MappingRuleMatcher {
    @Override
    public boolean match(VariableContext variables) {
      return true;
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
  }

  public static MappingRuleMatcher createUserMatcher(String userName) {
    return new VariableMatcher("%user", userName);
  }

  public static MappingRuleMatcher createGroupMatcher(String groupName) {
    return new VariableMatcher("%primary_group", groupName);
  }

  public static MappingRuleMatcher createApplicationNameMatcher(String name) {
    return new VariableMatcher("%application", name);
  }
}
