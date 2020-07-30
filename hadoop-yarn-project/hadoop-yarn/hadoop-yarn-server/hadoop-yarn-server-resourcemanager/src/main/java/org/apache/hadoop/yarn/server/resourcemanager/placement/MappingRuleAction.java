package org.apache.hadoop.yarn.server.resourcemanager.placement;

public interface MappingRuleAction {
  /**
   * Returns the fallback action to be taken if the main action (result returned
   * by the execute method) fails.
   * e.g. Target queue does not exist, or reference is ambiguous
   * @return The fallback action to be taken if the main action fails
   */
  MappingRuleResult getFallback();

  /**
   * This method is the main logic of the action, it shall determine based on the
   * mapping context, what should be the action's result.
   * @param variables The variable context, which contains all the variables
   * @return The result of the action
   */
  MappingRuleResult execute(VariableContext variables);
}
