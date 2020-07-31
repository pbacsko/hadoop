package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement;

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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.Rule.FallbackResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.Rule.Policy;

import com.fasterxml.jackson.databind.ObjectMapper;

public class MappingRuleCreator {
  private static final String USER_TYPE = "user";
  private static final String GROUP_TYPE = "group";

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
    return getMappingRules(appSubmissionQueue, desc);
  }

  // TODO: main logic
  // needs some fine-tuning after things are finalized
  List<MappingRule> getMappingRules(String appSubmissionQueue,
      MappingRulesDescription rules) {
    List<MappingRule> mappingRules = new ArrayList<>();

    for (Rule rule : rules.getRules()) {
      String matches = rule.getMatches();
      String type = rule.getType();
      Policy policy = rule.getPolicy();
      String queue = rule.getQueue();
      FallbackResult fallbackResult = rule.getFallbackResult();

      MappingRuleMatcher matcher;
      if (USER_TYPE.equals(type)) {
        matcher = MappingRuleMatchers.createUserMatcher(matches);
      } else if (GROUP_TYPE.equals(type)) {
        matcher = MappingRuleMatchers.createGroupMatcher(matches);
      } else {
        throw new UnsupportedOperationException("User/group only right now");
      }

      // action based on the policy
      MappingRuleAction action = null;
      switch (policy) {
      case DEFAULT_QUEUE:
        String defaultQueue = (rule.getDefaultQueue() != null) ?
            rule.getDefaultQueue() : "root.default";
        action = MappingRuleActions.createPlaceToQueueAction(defaultQueue);
        break;
      case REJECT:
        action = MappingRuleActions.createRejectAction();
        break;
      case RULE_PLACEMENT:
        action = MappingRuleActions.createPlaceToQueueAction(queue);
        break;
      case SPECIFIED_PLACEMENT:
        action = MappingRuleActions.createPlaceToQueueAction(appSubmissionQueue);
        break;
      }

      // fallback - what happens if the action fails?
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
}
