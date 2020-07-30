package org.apache.hadoop.yarn.server.resourcemanager.placement;

import com.google.common.collect.ImmutableSet;
import junit.framework.TestCase;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

public class TestMappingRule extends TestCase {
  VariableContext setupVariables(
      String user, String group, String secGroup, String appName) {
    VariableContext variables = new VariableContext();
    variables.put("%default", "root.default");
    variables.put("%user", user);
    variables.put("%primary_group", group);
    variables.put("%secondary_group", secGroup);
    variables.put("%application", appName);
    variables.put("%sub", "xxx");
    variables.put("%empty", "");
    variables.put("%null", null);
    variables.setImmutables(ImmutableSet.of(
        "%user", "%primary_group", "%secondary_group", "%application"));

    return variables;
  }

  void assertRejectResult(MappingRuleResult result) {
    assertTrue(
        MappingRuleResult.MappingRuleResultType.REJECT == result.getResult());
  }

  void assertSkipResult(MappingRuleResult result) {
    assertTrue(
        MappingRuleResult.MappingRuleResultType.SKIP == result.getResult());
  }

  void assertPlaceResult(MappingRuleResult result, String queue) {
    assertTrue(
        MappingRuleResult.MappingRuleResultType.PLACE == result.getResult());
    assertEquals(queue, result.getQueue());
  }

  @Test
  public void testMappingRuleEvaluation() {
    VariableContext matching = setupVariables(
        "bob", "developer", "users", "MR");
    VariableContext mismatching = setupVariables(
        "joe", "tester", "admins", "Spark");

    MappingRule rule = new MappingRule(
        MappingRuleMatchers.createUserMatcher("bob"),
        (new MappingRuleActions.PlaceToQueueAction("%default.%default"))
            .setFallbackSkip()
    );

    assertSkipResult(rule.getFallback());

    MappingRuleResult matchingResult = rule.evaluate(matching);
    MappingRuleResult mismatchingResult = rule.evaluate(mismatching);

    assertNull(mismatchingResult);
    assertPlaceResult(matchingResult, "root.default.root.default");
  }

  MappingRule createMappingRuleFromLegacyString(String legacyMapping) {
    String[] mapping =
        StringUtils
            .getTrimmedStringCollection(legacyMapping, ":")
            .toArray(new String[] {});

    if (mapping.length == 2) {
      return MappingRule.createLegacyRule(mapping[0], mapping[1]);
    }

    return MappingRule.createLegacyRule(mapping[0], mapping[1], mapping[2]);
  }

  void evaluateLegacyStringTestcase(
      String legacyString, VariableContext variables, String expectedQueue) {
    MappingRule rule = createMappingRuleFromLegacyString(legacyString);
    MappingRuleResult result = rule.evaluate(variables);

    if (expectedQueue == null) {
      assertNull(result);
      return;
    }

    assertPlaceResult(result, expectedQueue);
  }

  @Test
  public void testLegacyEvaluation() {
    VariableContext matching = setupVariables(
        "bob", "developer", "users", "MR");
    VariableContext mismatching = setupVariables(
        "joe", "tester", "admins", "Spark");

    evaluateLegacyStringTestcase(
        "u:bob:root.%primary_group", matching, "root.developer");
    evaluateLegacyStringTestcase(
        "u:bob:root.%primary_group", mismatching, null);
    evaluateLegacyStringTestcase(
        "g:developer:%secondary_group.%user", matching, "users.bob");
    evaluateLegacyStringTestcase(
        "g:developer:%secondary_group.%user", mismatching, null);
    evaluateLegacyStringTestcase(
        "MR:root.static", matching, "root.static");
    evaluateLegacyStringTestcase(
        "MR:root.static", mismatching, null);

    //catch all tests
    evaluateLegacyStringTestcase(
        "u:%user:root.%primary_group", matching, "root.developer");
    evaluateLegacyStringTestcase(
        "u:%user:root.%primary_group", mismatching, "root.tester");
  }
}