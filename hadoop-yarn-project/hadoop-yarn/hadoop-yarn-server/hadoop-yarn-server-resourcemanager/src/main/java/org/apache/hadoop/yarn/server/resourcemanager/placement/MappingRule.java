package org.apache.hadoop.yarn.server.resourcemanager.placement;

import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * Mapping rule represents a single mapping setting defined by the user. All
 * rules have matchers and actions. Matcher determine if a mapping rule applies
 * to a given applicationSubmission, while action represent the course of action
 * we need to take when a rule applies.
 *
 * MappingRules also support fallback actions, which will be evaluated when the
 * main action fails due to any reason (Eg. trying to place to a queue which
 * does not exist)
 */
public class MappingRule {
  private final MappingRuleMatcher matcher;
  private final MappingRuleAction action;

  public MappingRule(MappingRuleMatcher matcher, MappingRuleAction action) {
    this.matcher = matcher;
    this.action = action;
  }

  /**
   * This method evaluates the rule, and returns the MappingRuleResult, if
   * the rule matches, null otherwise.
   * @param variables The variable context, which contains all the variables
   * @return The rule's result or null if the rule doesn't apply
   */
  public MappingRuleResult evaluate(VariableContext variables) {
    if (matcher.match(variables)) {
      return action.execute(variables);
    }

    return null;
  }

  /**
   * Returns the associated action's fallback
   * @return The fallback of the action
   */
  public MappingRuleResult getFallback() {
    return action.getFallback();
  }

  public static MappingRule createLegacyRule(String source, String path) {
    return createLegacyRule("a", source, path);
  }

  public static MappingRule createLegacyRule(
      String type, String source, String path) {
    MappingRuleMatcher matcher;
    MappingRuleAction action = new MappingRuleActions.PlaceToQueueAction(path);
    switch (type) {
      case "u":
        matcher = MappingRuleMatchers.createUserMatcher(source);
      break;
      case "g":
        matcher = MappingRuleMatchers.createGroupMatcher(source);
      break;
      default:
        matcher = MappingRuleMatchers.createApplicationNameMatcher(source);
    }

    return new MappingRule(matcher, action);
  }

  public void validate(MappingRuleValidationContext ctx)
      throws YarnException {
    this.action.validate(ctx);
  }

  @Override
  public String toString() {
    return "MappingRule{" +
      "matcher=" + matcher +
      ", action=" + action +
      '}';
  }
}
