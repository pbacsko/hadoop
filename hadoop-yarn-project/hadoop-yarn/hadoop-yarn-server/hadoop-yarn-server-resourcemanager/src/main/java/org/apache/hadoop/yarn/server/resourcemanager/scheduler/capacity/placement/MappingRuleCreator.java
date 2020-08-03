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

import org.apache.hadoop.yarn.server.resourcemanager.placement.MappingRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.MappingRuleAction;
import org.apache.hadoop.yarn.server.resourcemanager.placement.MappingRuleActionBase;
import org.apache.hadoop.yarn.server.resourcemanager.placement.MappingRuleActions;
import org.apache.hadoop.yarn.server.resourcemanager.placement.MappingRuleMatcher;
import org.apache.hadoop.yarn.server.resourcemanager.placement.MappingRuleMatchers;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.NestedUserRule.OuterRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.Rule.FallbackResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.Rule.Policy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.Rule.Type;

import com.fasterxml.jackson.databind.ObjectMapper;
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

  public List<MappingRule> getMappingRules(String appSubmissionQueue,
      String jsonPath) throws IOException {
    MappingRulesDescription desc = getMappingRulesFromJson(jsonPath);
    return getMappingRules(desc);
  }

  // TODO: main logic
  // needs some fine-tuning after things are finalized
  List<MappingRule> getMappingRules(MappingRulesDescription rules) {
    List<MappingRule> mappingRules = new ArrayList<>();

    for (Rule rule : rules.getRules()) {
      String matches = rule.getMatches();
      Type type = rule.getType();
      Policy policy = rule.getPolicy();
      String queue = rule.getQueue();
      FallbackResult fallbackResult = rule.getFallbackResult();
      String ruleDefaultQueue = rule.getDefaultQueue();

      MappingRuleMatcher matcher;
      switch (type) {
      case USER:
        if (ALL_USER.equals(matches)) {
          matcher = MappingRuleMatchers.createUserMatcher(matches);
        } else {
          matcher = MappingRuleMatchers.createAllMatcher();
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
        String defaultQueue = getDefaultQueue(ruleDefaultQueue);
        action = MappingRuleActions.createPlaceToQueueAction(defaultQueue);
        break;
      case REJECT:
        action = MappingRuleActions.createRejectAction();
        break;
      case PRIMARY_GROUP:
        action = MappingRuleActions.createPlaceToQueueAction(
            getTargetQueue(queue, "%primary_group"));
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
        ((MappingRuleActionBase)action).setFallbackDefaultPlacement();
        break;
      case REJECT:
        ((MappingRuleActionBase)action).setFallbackReject();
        break;
      case SKIP:
        ((MappingRuleActionBase)action).setFallbackSkip();
      default:
        throw new IllegalArgumentException(
            "Unsupported fallback rule " + fallbackResult);
      }

      MappingRule mappingRule = new MappingRule(matcher, action);
      mappingRules.add(mappingRule);
    }

    return mappingRules;
  }

  public MappingRuleAction getActionForNested(NestedUserRule nestedRule) {
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
      targetQueue += ".%primary_group";
      break;
    case SECONDARY_GROUP:
      targetQueue += ".%secondary_group";
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

  public String getDefaultQueue(String ruleDefaultQueue) {
    if (ruleDefaultQueue == null ||
        (FULL_DEFAULT_QUEUE_PATH.equals(ruleDefaultQueue) ||
        DEFAULT_QUEUE.equals(ruleDefaultQueue))) {
      return FULL_DEFAULT_QUEUE_PATH;
    } else {
      return ruleDefaultQueue;
    }
  }

  public String getTargetQueue(String parent, String placeholder) {
    return (parent== null) ? placeholder : parent + "." + placeholder;
  }
}
