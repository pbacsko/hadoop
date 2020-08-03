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

    @Override
    public String toString() {
      return "PlaceToQueueAction{" +
        "queueName='" + queueName + '\'' +
        '}';
    }
  }

  public static class RejectAction extends MappingRuleActionBase {
    @Override
    public MappingRuleResult execute(VariableContext variables) {
      return MappingRuleResult.createRejectResult();
    }

    @Override
    public String toString() {
      return "RejectAction";
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

    @Override
    public String toString() {
      return "VariableUpdateAction{" +
        "variableName='" + variableName + '\'' +
        ", variableValue='" + variableValue + '\'' +
        '}';
    }
  }

  public static MappingRuleAction createUpdateDefaultAction(String queue) {
    return new VariableUpdateAction("%default", queue);
  }

  public static MappingRuleAction createPlaceToQueueAction(String queue) {
    return new PlaceToQueueAction(queue);
  }

  public static MappingRuleAction createRejectAction() {
    return new RejectAction();
  }
}
