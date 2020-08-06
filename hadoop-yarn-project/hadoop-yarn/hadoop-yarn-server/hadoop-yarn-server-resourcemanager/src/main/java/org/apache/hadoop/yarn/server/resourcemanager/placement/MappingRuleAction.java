package org.apache.hadoop.yarn.server.resourcemanager.placement;

import org.apache.hadoop.yarn.exceptions.YarnException;

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


  /**
   * Sets the fallback method to reject, if the action cannot be executed the
   * application will get rejected
   */
  MappingRuleActionBase setFallbackReject();

  /**
   * Sets the fallback method to skip, if the action cannot be executed
   * We move onto the next rule, ignoring this one
   */
  MappingRuleActionBase setFallbackSkip();

  /**
   * Sets the fallback method to place to default, if the action cannot be executed
   * The application will be placed into the default queue, if the default queue
   * does not exist the application will get rejected
   */
  MappingRuleActionBase setFallbackDefaultPlacement();

  /**
   * This method is responsible for config validation, the context contains all
   * information required for validation, method should throw an exception on
   * detectable setup errors.
   * @param ctx
   * @throws YarnException
   */
  void validate(MappingRuleValidationContext ctx) throws YarnException;
}
