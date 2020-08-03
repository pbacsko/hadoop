package org.apache.hadoop.yarn.server.resourcemanager.placement;

import junit.framework.TestCase;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestMappingRuleMatchers extends TestCase {

  @Test
  public void testCatchAll() {
    VariableContext variables = new VariableContext();
    MappingRuleMatcher matcher = new MappingRuleMatchers.MatchAllMatcher();

    assertTrue(matcher.match(variables));
  }

  @Test
  public void testVariableMatcher() {
    VariableContext matchingContext = new VariableContext();
    matchingContext.put("%user", "bob");
    matchingContext.put("%primary_group", "developers");
    matchingContext.put("%application", "TurboMR");
    matchingContext.put("%custom", "Matching string");

    VariableContext mismatchingContext = new VariableContext();
    mismatchingContext.put("%user", "dave");
    mismatchingContext.put("%primary_group", "testers");
    mismatchingContext.put("%application", "Tester APP");
    mismatchingContext.put("%custom", "Not matching string");

    VariableContext emptyContext = new VariableContext();

    Map<String, MappingRuleMatcher> matchers = new HashMap<>();
    matchers.put("User matcher", MappingRuleMatchers.createUserMatcher("bob"));
    matchers.put("Group matcher",
        MappingRuleMatchers.createGroupMatcher("developers"));
    matchers.put("Application name matcher",
        MappingRuleMatchers.createApplicationNameMatcher("TurboMR"));
    matchers.put("Custom matcher",
        new MappingRuleMatchers.VariableMatcher("%custom", "Matching string"));

    matchers.forEach((matcherName,matcher) -> {
      assertTrue(matcherName + " with matchingContext should match",
          matcher.match(matchingContext));
      assertFalse(matcherName + " with mismatchingContext shouldn't match",
          matcher.match(mismatchingContext));
      assertFalse(matcherName + " with emptyContext shouldn't match",
          matcher.match(emptyContext));
    });
  }

  @Test
  public void testVariableMatcherSubstitutions() {
    VariableContext matchingContext = new VariableContext();
    matchingContext.put("%user", "bob");
    matchingContext.put("%primary_group", "developers");
    matchingContext.put("%application", "TurboMR");
    matchingContext.put("%custom", "bob");
    matchingContext.put("%cus", "b");
    matchingContext.put("%tom", "ob");

    VariableContext mismatchingContext = new VariableContext();
    mismatchingContext.put("%user", "dave");
    mismatchingContext.put("%primary_group", "testers");
    mismatchingContext.put("%application", "Tester APP");
    mismatchingContext.put("%custom", "bob");
    mismatchingContext.put("%cus", "b");
    mismatchingContext.put("%tom", "ob");

    MappingRuleMatcher customUser =
        new MappingRuleMatchers.VariableMatcher("%custom", "%user");

    MappingRuleMatcher userCusTom =
        new MappingRuleMatchers.VariableMatcher("%user", "%cus%tom");

    MappingRuleMatcher userCustom =
        new MappingRuleMatchers.VariableMatcher("%user", "%custom");

    MappingRuleMatcher userUser =
        new MappingRuleMatchers.VariableMatcher("%user", "%user");

    MappingRuleMatcher userStatic =
        new MappingRuleMatchers.VariableMatcher("%user", "bob");

    assertTrue(customUser.match(matchingContext));
    assertTrue(userCustom.match(matchingContext));
    assertTrue(userCusTom.match(matchingContext));
    assertTrue(userUser.match(matchingContext));
    assertTrue(userStatic.match(matchingContext));

    assertFalse(customUser.match(mismatchingContext));
    assertFalse(userCustom.match(mismatchingContext));
    assertFalse(userCusTom.match(mismatchingContext));
    assertTrue(userUser.match(mismatchingContext));
    assertFalse(userStatic.match(mismatchingContext));
  }

  @Test
  public void testVariableMatcherWithEmpties() {
    VariableContext emptyContext = new VariableContext();
    VariableContext allNull = new VariableContext();
    allNull.put("%null", null);
    allNull.put("%empty", null);

    VariableContext allEmpty = new VariableContext();
    allEmpty.put("%null", "");
    allEmpty.put("%empty", "");

    VariableContext allMakesSense = new VariableContext();
    allMakesSense.put("%null", null);
    allMakesSense.put("%empty", "");

    VariableContext allFull = new VariableContext();
    allFull.put("%null", "full");
    allFull.put("%empty", "full");

    MappingRuleMatcher nullMatcher =
        new MappingRuleMatchers.VariableMatcher("%null", null);

    MappingRuleMatcher emptyMatcher =
        new MappingRuleMatchers.VariableMatcher("%empty", "");

    //nulls should be handled as empty strings, so all should match
    assertTrue(nullMatcher.match(emptyContext));
    assertTrue(emptyMatcher.match(emptyContext));
    assertTrue(nullMatcher.match(allEmpty));
    assertTrue(emptyMatcher.match(allEmpty));
    assertTrue(nullMatcher.match(allNull));
    assertTrue(emptyMatcher.match(allNull));
    assertTrue(nullMatcher.match(allMakesSense));
    assertTrue(emptyMatcher.match(allMakesSense));

    //neither null nor "" should match an actual string
    assertFalse(nullMatcher.match(allFull));
    assertFalse(emptyMatcher.match(allFull));

    MappingRuleMatcher nullVarMatcher =
        new MappingRuleMatchers.VariableMatcher(null, "");

    //null variable should never match
    assertFalse(nullVarMatcher.match(emptyContext));
    assertFalse(nullVarMatcher.match(allNull));
    assertFalse(nullVarMatcher.match(allEmpty));
    assertFalse(nullVarMatcher.match(allMakesSense));
    assertFalse(nullVarMatcher.match(allFull));
  }

  @Test
  public void testBoolOperatorMatchers() {
    VariableContext developerBob = new VariableContext();
    developerBob.put("%user", "bob");
    developerBob.put("%primary_group", "developers");


    VariableContext testerBob = new VariableContext();
    testerBob.put("%user", "bob");
    testerBob.put("%primary_group", "testers");

    VariableContext testerDave = new VariableContext();
    testerDave.put("%user", "dave");
    testerDave.put("%primary_group", "testers");


    VariableContext accountantDave = new VariableContext();
    accountantDave.put("%user", "dave");
    accountantDave.put("%primary_group", "accountants");

    MappingRuleMatcher userBob =
        new MappingRuleMatchers.VariableMatcher("%user", "bob");

    MappingRuleMatcher groupDevelopers =
        new MappingRuleMatchers.VariableMatcher(
           "%primary_group", "developers");

    MappingRuleMatcher groupAccountants =
        new MappingRuleMatchers.VariableMatcher(
            "%primary_group", "accountants");

    MappingRuleMatcher developerBobMatcher = new MappingRuleMatchers.AndMatcher(
            userBob, groupDevelopers);

    MappingRuleMatcher testerDaveMatcher =
        MappingRuleMatchers.createUserGroupMatcher("dave", "testers");

    MappingRuleMatcher accountantOrBobMatcher =
        new MappingRuleMatchers.OrMatcher(groupAccountants, userBob);

    assertTrue(developerBobMatcher.match(developerBob));
    assertFalse(developerBobMatcher.match(testerBob));
    assertFalse(developerBobMatcher.match(testerDave));
    assertFalse(developerBobMatcher.match(accountantDave));

    System.out.println(testerDaveMatcher);
    assertFalse(testerDaveMatcher.match(developerBob));
    assertFalse(testerDaveMatcher.match(testerBob));
    assertTrue(testerDaveMatcher.match(testerDave));
    assertFalse(testerDaveMatcher.match(accountantDave));

    assertTrue(accountantOrBobMatcher.match(developerBob));
    assertTrue(accountantOrBobMatcher.match(testerBob));
    assertFalse(accountantOrBobMatcher.match(testerDave));
    assertTrue(accountantOrBobMatcher.match(accountantDave));
  }

  @Test
  public void testToStrings() {
    MappingRuleMatcher var = new MappingRuleMatchers.VariableMatcher("%a", "b");
    MappingRuleMatcher all = new MappingRuleMatchers.MatchAllMatcher();
    MappingRuleMatcher and = new MappingRuleMatchers.AndMatcher(var, all, var);
    MappingRuleMatcher or = new MappingRuleMatchers.OrMatcher(var,all, var);

    assertEquals("VariableMatcher{variable='%a', value='b'}", var.toString());
    assertEquals("MatchAllMatcher", all.toString());
    assertEquals("AndMatcher{matchers=[" + var.toString() +
        ", " + all.toString() +
        ", " + var.toString() + "]}", and.toString());
    assertEquals("OrMatcher{matchers=[" + var.toString() +
        ", " + all.toString() +
        ", " + var.toString() + "]}", or.toString());
  }

}