package org.apache.hadoop.yarn.server.resourcemanager.placement;

public class MappingRuleActions {
  public static class PlaceToQueueAction extends MappingRuleActionBase {
    private String queueName;

    PlaceToQueueAction(String queueName) {
      this.queueName = queueName == null ? "" : queueName;
    }

    @Override
    public MappingRuleResult execute(VariableContext variables) {
      String substituted = variables.replacePathVariables(queueName);
      return MappingRuleResult.createPlacementResult(substituted);
    }
  }

  public static class RejectAction extends MappingRuleActionBase {
    @Override
    public MappingRuleResult execute(VariableContext variables) {
      return MappingRuleResult.createRejectResult();
    }
  }

  public static class VariableUpdateAction extends MappingRuleActionBase {
    private String variableName;
    private String variableValue;

    VariableUpdateAction(String variableName, String variableValue) {
      this.variableName = variableName;
      this.variableValue = variableValue;
    }

    @Override
    public MappingRuleResult execute(VariableContext variables) {
      variables.put(variableName, variables.replaceVariables(variableValue));
      return MappingRuleResult.createSkipResult();
    }
  }

  public static MappingRuleAction createUpdateDefaultAction(String queue) {
    return new VariableUpdateAction("%default", queue);
  }
}
