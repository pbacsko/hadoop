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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.yarn.server.resourcemanager.placement.MappingRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.MappingRuleAction;
import org.apache.hadoop.yarn.server.resourcemanager.placement.MappingRuleActions;
import org.apache.hadoop.yarn.server.resourcemanager.placement.MappingRuleMatcher;
import org.apache.hadoop.yarn.server.resourcemanager.placement.MappingRuleMatchers;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.MappingRulesDescription;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.NestedUserRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.NestedUserRule.OuterRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule.FallbackResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule.Policy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule.Type;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

public class MappingRuleCreator {
  private static final String FULL_DEFAULT_QUEUE_PATH = "root.default";
  private static final String DEFAULT_QUEUE = "default";
  private static final String ALL_USER = "*";

  public MappingRulesDescription getMappingRulesFromJson(String jsonPath)
      throws IOException {
    byte[] fileContents = Files.readAllBytes(Paths.get(jsonPath));
    return getMappingRulesFromJson(fileContents);
  }

  MappingRulesDescription getMappingRulesFromJson(byte[] contents)
      throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.readValue(contents, MappingRulesDescription.class);
  }

  public List<MappingRule> getMappingRules(String jsonPath) throws IOException {
    MappingRulesDescription desc = getMappingRulesFromJson(jsonPath);
    return getMappingRules(desc);
  }

  @VisibleForTesting
  List<MappingRule> getMappingRules(MappingRulesDescription rules) {
    List<MappingRule> mappingRules = new ArrayList<>();

    for (Rule rule : rules.getRules()) {
      checkMandatoryParameters(rule);

      String matches = rule.getMatches();
      Type type = rule.getType();
      Policy policy = rule.getPolicy();
      String queue = rule.getQueue();
      FallbackResult fallbackResult = rule.getFallbackResult();
      boolean updateDefaultQueue = false;

      MappingRuleMatcher matcher;
      switch (type) {
      case USER:
        if (ALL_USER.equals(matches)) {
          matcher = MappingRuleMatchers.createAllMatcher();
        } else {
          matcher = MappingRuleMatchers.createUserMatcher(matches);
        }
        break;
      case GROUP:
        checkArgument(!ALL_USER.equals(matches), "Cannot match '*' for groups");
        matcher = MappingRuleMatchers.createGroupMatcher(matches);
        break;
      case APPLICATION:
        matcher = MappingRuleMatchers.createApplicationNameMatcher(matches);
        break;
      default:
        throw new IllegalArgumentException("Unknown type: " + type);
      }

      MappingRuleAction action = null;
      switch (policy) {
      case DEFAULT_QUEUE:
        action = MappingRuleActions.createPlaceToDefaultAction();
        updateDefaultQueue = true;
        break;
      case REJECT:
        action = MappingRuleActions.createRejectAction();
        break;
      case PRIMARY_GROUP:
        action = MappingRuleActions.createPlaceToQueueAction(
            getTargetQueue(queue, "%primary_group"));
        break;
      case SPECIFIED_PLACEMENT:
        action = MappingRuleActions.createPlaceToQueueAction("%specified");
        break;
      case CUSTOM:
        throw new UnsupportedOperationException("custom policy");
      case NESTED_USER:
        action = getActionForNested(rule.getNestedUserRule());
        break;
      case SECONDARY_GROUP:
        action = MappingRuleActions.createPlaceToQueueAction(
            getTargetQueue(queue, "%secondary_group"));
        break;
      case USER:
        action = MappingRuleActions.createPlaceToQueueAction(
            getTargetQueue(queue, "%user"));
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported policy: " + policy);
      }

      switch (fallbackResult) {
      case PLACE_DEFAULT:
        action.setFallbackDefaultPlacement();
        break;
      case REJECT:
        action.setFallbackReject();
        break;
      case SKIP:
        action.setFallbackSkip();
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported fallback rule " + fallbackResult);
      }

      if (updateDefaultQueue) {
        Pair<Boolean, String> defaultQueue = getDefaultQueue(queue);
        // add an extra rule that causes the evaluator to update "%default" if necessary
        if (defaultQueue.getLeft()) {
          MappingRuleMatcher updateMatcher =
              MappingRuleMatchers.createAllMatcher();
          MappingRuleAction updateAction =
              MappingRuleActions.createUpdateDefaultAction(defaultQueue.getRight());

          MappingRule updateRule = new MappingRule(updateMatcher, updateAction);
          mappingRules.add(updateRule);
        }
      }

      MappingRule mappingRule = new MappingRule(matcher, action);
      mappingRules.add(mappingRule);
    }

    return mappingRules;
  }

  private MappingRuleAction getActionForNested(NestedUserRule nestedRule) {
    Preconditions.checkArgument(nestedRule != null,
        "nested rule is undefined");
    OuterRule outerRule = nestedRule.getOuterRule();
    String parent = nestedRule.getParent();
    String targetQueue = "";

    if (parent != null) {
      targetQueue += parent;
    }

    switch (outerRule) {
    case PRIMARY_GROUP:
      if (targetQueue.isEmpty()) {
        targetQueue = "%primary_group";
      } else {
        targetQueue += ".%primary_group";
      }
      break;
    case SECONDARY_GROUP:
      if (targetQueue.isEmpty()) {
        targetQueue = "%secondary_group";
      } else {
        targetQueue += ".%secondary_group";
      }
      break;
    case QUEUE:
      checkArgument(parent != null, "parent queue is null in nested rule");
      break;
    default:
      throw new IllegalArgumentException("Unknown outer rule: " + outerRule);
    }

    targetQueue += ".%user";

    return MappingRuleActions.createPlaceToQueueAction(targetQueue);
  }

  private Pair<Boolean, String> getDefaultQueue(String ruleDefaultQueue) {
    if (ruleDefaultQueue == null ||
        (FULL_DEFAULT_QUEUE_PATH.equals(ruleDefaultQueue) ||
        DEFAULT_QUEUE.equals(ruleDefaultQueue))) {
      return Pair.of(false, null);
    } else {
      return Pair.of(true, ruleDefaultQueue);
    }
  }

  private String getTargetQueue(String parent, String placeholder) {
    return (parent == null) ? placeholder : parent + "." + placeholder;
  }

  private void checkMandatoryParameters(Rule rule) {
    checkArgument(rule.getType() != null, "Rule type is undefined");
    checkArgument(rule.getPolicy() != null, "Rule policy is undefined");
    checkArgument(rule.getMatches() != null, "Match string is undefined");
    checkArgument(rule.getFallbackResult() != null,
        "Fallback result is undefined");
    checkArgument(!StringUtils.isEmpty(rule.getMatches()),
        "Match string is empty");
  }
}
