package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.server.resourcemanager.placement.MappingRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.MappingRuleResult;
import org.apache.hadoop.yarn.server.resourcemanager.placement.MappingRuleResultType;
import org.apache.hadoop.yarn.server.resourcemanager.placement.VariableContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.MappingRulesDescription;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.NestedUserRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.NestedUserRule.OuterRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule.FallbackResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule.Policy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule.Type;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestMappingRuleCreator {
  private static final String MATCH_ALL = "*";

  private static final String DEFAULT_QUEUE = "root.default";
  private static final String SECONDARY_GROUP = "users";
  private static final String PRIMARY_GROUP = "superuser";
  private static final String APPLICATION_NAME = "MapReduce";
  private static final String SPECIFIED_QUEUE = "root.users.hadoop";
  private static final String USER_NAME = "testuser";

  private MappingRuleCreator ruleCreator;

  private VariableContext variableContext;
  private MappingRulesDescription description;
  private Rule rule;

  @org.junit.Rule
  public ExpectedException expected = ExpectedException.none();

  @Before
  public void setup() {
    ruleCreator = new MappingRuleCreator();
    prepareMappingRuleDescription();
    variableContext = new VariableContext();

    variableContext.put("%user", USER_NAME);
    variableContext.put("%specified", SPECIFIED_QUEUE);
    variableContext.put("%application", APPLICATION_NAME);
    variableContext.put("%primary_group", PRIMARY_GROUP);
    variableContext.put("%secondary_group", SECONDARY_GROUP);
    variableContext.put("%default", DEFAULT_QUEUE);
  }

  @Test
  public void testAllUserMatcher() {
    variableContext.put("%user", USER_NAME);
    verifyPlacementSucceeds(USER_NAME);

    variableContext.put("%user", "dummyuser");
    verifyPlacementSucceeds("dummyuser");
  }

  @Test
  public void testSpecificUserMatcherPasses() {
    rule.setMatches(USER_NAME);

    verifyPlacementSucceeds(USER_NAME);
  }

  @Test
  public void testSpecificUserMatcherFails() {
    rule.setMatches(USER_NAME);
    variableContext.put("%user", "dummyuser");

    verifyPlacementFails();
  }

  @Test
  public void testSpecificGroupMatcher() {
    rule.setMatches(PRIMARY_GROUP);
    rule.setType(Type.GROUP);

    verifyPlacementSucceeds();
  }

  @Test
  public void testAllGroupMatcher() {
    rule.setType(Type.GROUP);

    expected.expect(IllegalArgumentException.class);
    expected.expectMessage("Cannot match");

    ruleCreator.getMappingRules(description);
  }

  @Test
  public void testApplicationNameMatcherPasses() {
    rule.setType(Type.APPLICATION);
    rule.setMatches(APPLICATION_NAME);

    verifyPlacementSucceeds();
  }

  @Test
  public void testApplicationNameMatcherFails() {
    rule.setType(Type.APPLICATION);
    rule.setMatches("dummyApplication");

    verifyPlacementFails();
  }

  @Test
  public void testDefaultRule() {
    rule.setPolicy(Policy.DEFAULT_QUEUE);

    verifyPlacementSucceeds(DEFAULT_QUEUE);
  }

  @Test
  public void testDefaultRuleWithDifferentQueue() {
    rule.setPolicy(Policy.DEFAULT_QUEUE);
    rule.setQueue("root.test");

    verifyPlacementSucceeds("root.test");
  }

  @Test
  public void testDefaultRuleWithDefaultQueue() {
    rule.setPolicy(Policy.DEFAULT_QUEUE);
    rule.setQueue("default");

    verifyPlacementSucceeds(DEFAULT_QUEUE);
  }

  @Test
  public void testDefaultRuleWithRootDefaultQueue() {
    rule.setPolicy(Policy.DEFAULT_QUEUE);
    rule.setQueue("root.default");

    verifyPlacementSucceeds(DEFAULT_QUEUE);
  }

  @Test
  public void testSpecifiedRule() {
    rule.setPolicy(Policy.SPECIFIED_PLACEMENT);

    verifyPlacementSucceeds(SPECIFIED_QUEUE);
  }

  @Test
  public void testRejectRule() {
    rule.setPolicy(Policy.REJECT);

    verifyPlacementRejected();
  }

  @Test
  public void testPrimaryGroupRule() {
    rule.setPolicy(Policy.PRIMARY_GROUP);

    verifyPlacementSucceeds(PRIMARY_GROUP);
  }

  @Test
  public void testPrimaryGroupRuleWithParent() {
    rule.setPolicy(Policy.PRIMARY_GROUP);
    rule.setQueue("root");

    verifyPlacementSucceeds("root." + PRIMARY_GROUP);
  }

  @Test
  public void testSecondaryGroupRule() {
    rule.setPolicy(Policy.SECONDARY_GROUP);

    verifyPlacementSucceeds(SECONDARY_GROUP);
  }

  @Test
  public void testSecondaryGroupRuleWithParent() {
    rule.setPolicy(Policy.SECONDARY_GROUP);
    rule.setQueue("root");

    verifyPlacementSucceeds("root." + SECONDARY_GROUP);
  }

  @Test
  public void testUserRule() {
    rule.setPolicy(Policy.USER);

    verifyPlacementSucceeds(USER_NAME);
  }

  @Test
  public void testPrimayGroupNestedRule() {
    rule.setPolicy(Policy.NESTED_USER);
    NestedUserRule nestedRule = new NestedUserRule();
    nestedRule.setOuterRule(OuterRule.PRIMARY_GROUP);
    rule.setNestedUserRule(nestedRule);

    verifyPlacementSucceeds("superuser.testuser");
  }

  @Test
  public void testPrimayGroupNestedRuleWithParent() {
    rule.setPolicy(Policy.NESTED_USER);
    NestedUserRule nestedRule = new NestedUserRule();
    nestedRule.setOuterRule(OuterRule.PRIMARY_GROUP);
    nestedRule.setParent("root");
    rule.setNestedUserRule(nestedRule);

    verifyPlacementSucceeds("root.superuser.testuser");
  }
  
  @Test
  public void testSecondaryGroupNestedRule() {
    rule.setPolicy(Policy.NESTED_USER);
    NestedUserRule nestedRule = new NestedUserRule();
    nestedRule.setOuterRule(OuterRule.SECONDARY_GROUP);
    rule.setNestedUserRule(nestedRule);

    verifyPlacementSucceeds("users.testuser");
  }

  @Test
  public void testSecondaryGroupNestedRuleWithParent() {
    rule.setPolicy(Policy.NESTED_USER);
    NestedUserRule nestedRule = new NestedUserRule();
    nestedRule.setOuterRule(OuterRule.SECONDARY_GROUP);
    nestedRule.setParent("root");
    rule.setNestedUserRule(nestedRule);

    verifyPlacementSucceeds("root.users.testuser");
  }

  @Test
  public void testQueueNestedRule() {
    rule.setPolicy(Policy.NESTED_USER);
    NestedUserRule nestedRule = new NestedUserRule();
    nestedRule.setOuterRule(OuterRule.QUEUE);
    nestedRule.setParent("root.parent");
    rule.setNestedUserRule(nestedRule);

    verifyPlacementSucceeds("root.parent.testuser");
  }

  @Test
  public void testQueueNestedRuleWithoutParent() {
    rule.setPolicy(Policy.NESTED_USER);
    NestedUserRule nestedRule = new NestedUserRule();
    nestedRule.setOuterRule(OuterRule.QUEUE);
    rule.setNestedUserRule(nestedRule);

    expected.expect(IllegalArgumentException.class);
    expected.expectMessage("parent queue is null");

    ruleCreator.getMappingRules(description);
  }

  @Test
  public void testNestedRuleWithoutDefinition() {
    rule.setPolicy(Policy.NESTED_USER);

    expected.expect(IllegalArgumentException.class);
    expected.expectMessage("nested rule is undefined");

    ruleCreator.getMappingRules(description);
  }

  @Test
  public void testDefaultQueueFallback() {
    rule.setFallbackResult(FallbackResult.PLACE_DEFAULT);

    List<MappingRule> rules = ruleCreator.getMappingRules(description);
    MappingRule mpr = rules.get(0);

    assertEquals("Fallback result",
        MappingRuleResultType.PLACE_TO_DEFAULT, mpr.getFallback().getResult());
  }

  @Test
  public void testRejectFallback() {
    rule.setFallbackResult(FallbackResult.REJECT);

    List<MappingRule> rules = ruleCreator.getMappingRules(description);
    MappingRule mpr = rules.get(0);

    assertEquals("Fallback result",
        MappingRuleResultType.REJECT, mpr.getFallback().getResult());
  }

  @Test
  public void testSkipFallback() {
    rule.setFallbackResult(FallbackResult.SKIP);

    List<MappingRule> rules = ruleCreator.getMappingRules(description);
    MappingRule mpr = rules.get(0);

    assertEquals("Fallback result",
        MappingRuleResultType.SKIP, mpr.getFallback().getResult());
  }

  @Test
  public void testFallbackResultUnset() {
    rule.setFallbackResult(null);

    expected.expect(IllegalArgumentException.class);
    expected.expectMessage("Fallback result is undefined");

    ruleCreator.getMappingRules(description);
  }

  @Test
  public void testTypeUnset() {
    rule.setType(null);

    expected.expect(IllegalArgumentException.class);
    expected.expectMessage("Rule type is undefined");

    ruleCreator.getMappingRules(description);
  }

  @Test
  public void testMatchesUnset() {
    rule.setMatches(null);

    expected.expect(IllegalArgumentException.class);
    expected.expectMessage("Match string is undefined");

    ruleCreator.getMappingRules(description);
  }

  @Test
  public void testMatchesEmpty() {
    rule.setMatches("");

    expected.expect(IllegalArgumentException.class);
    expected.expectMessage("Match string is empty");

    ruleCreator.getMappingRules(description);
  }

  @Test
  public void testPolicyUnset() {
    rule.setPolicy(null);

    expected.expect(IllegalArgumentException.class);
    expected.expectMessage("Rule policy is undefined");

    ruleCreator.getMappingRules(description);
  }

  private void prepareMappingRuleDescription() {
    description = new MappingRulesDescription();

    rule = new Rule();
    rule.setType(Type.USER);
    rule.setFallbackResult(FallbackResult.REJECT);
    rule.setPolicy(Policy.USER);
    rule.setMatches(MATCH_ALL);

    List<Rule> rules = new ArrayList<>();
    rules.add(rule);

    description.setRules(rules);
  }

  private void verifyPlacementSucceeds() {
    verifyPlacement(MappingRuleResultType.PLACE, null);
  }

  private void verifyPlacementSucceeds(String expectedQueue) {
    verifyPlacement(MappingRuleResultType.PLACE, expectedQueue);
  }

  private void verifyPlacementRejected() {
    verifyPlacement(MappingRuleResultType.REJECT, null);
  }

  private void verifyPlacementFails() {
    verifyPlacement(null, null);
  }

  private void verifyPlacement(MappingRuleResultType expectedResultType,
      String expectedQueue) {
    List<MappingRule> rules = ruleCreator.getMappingRules(description);
    MappingRule mpr = rules.get(0);
    MappingRuleResult result = mpr.evaluate(variableContext);

    if (expectedResultType != null) {
      assertEquals("Mapping rule result",
          expectedResultType, result.getResult());
    } else {
      assertNull(result);
    }

    if (expectedQueue != null) {
      assertEquals("Evaluated queue", expectedQueue, result.getQueue());
    }
  }
}