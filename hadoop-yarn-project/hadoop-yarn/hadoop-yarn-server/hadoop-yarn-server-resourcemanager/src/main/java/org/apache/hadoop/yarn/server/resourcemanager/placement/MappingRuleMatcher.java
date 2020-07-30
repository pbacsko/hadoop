package org.apache.hadoop.yarn.server.resourcemanager.placement;

public interface MappingRuleMatcher {
  /**
   * Returns true if the matcher matches the current context.
   * @param variables The variable context, which contains all the variables
   * @return true if this matcher matches to the provided variable set
   */
  boolean match(VariableContext variables);
}
