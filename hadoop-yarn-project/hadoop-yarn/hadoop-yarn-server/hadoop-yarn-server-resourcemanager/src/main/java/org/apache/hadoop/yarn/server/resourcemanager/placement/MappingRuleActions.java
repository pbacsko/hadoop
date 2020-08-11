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

import org.apache.hadoop.yarn.exceptions.YarnException;

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
    public void validate(MappingRuleValidationContext ctx)
        throws YarnException {
      ctx.validateQueuePath(this.queueName);
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
    public void validate(MappingRuleValidationContext ctx) throws
        YarnException {}

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
    public void validate(MappingRuleValidationContext ctx)
        throws YarnException {
      ctx.addVariable(this.variableName);
    }

    @Override
    public String toString() {
      return "VariableUpdateAction{" +
        "variableName='" + variableName + '\'' +
        ", variableValue='" + variableValue + '\'' +
        '}';
    }
  }

  public static class PlaceToDefaultAction extends MappingRuleActionBase {
    @Override
    public MappingRuleResult execute(VariableContext variables) {
      return MappingRuleResult.createDefaultPlacementResult();
    }

    @Override
    public void validate(MappingRuleValidationContext ctx)
        throws YarnException {
      // nop
    }

    @Override
    public String toString() {
      return "PlaceToDefaultAction";
    }
  }

  public static MappingRuleAction createUpdateDefaultAction(String queue) {
    return new VariableUpdateAction("%default", queue);
  }

  public static MappingRuleAction createPlaceToQueueAction(String queue) {
    return new PlaceToQueueAction(queue);
  }

  public static MappingRuleAction createPlaceToDefaultAction() {
    return new PlaceToDefaultAction();
  }

  public static MappingRuleAction createRejectAction() {
    return new RejectAction();
  }
}
