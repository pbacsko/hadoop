package org.apache.hadoop.yarn.server.resourcemanager.placement;

import com.google.common.collect.ImmutableSet;
import junit.framework.TestCase;
import org.junit.Test;

public class TestMappingRuleActions extends TestCase {
  void assertRejectResult(MappingRuleResult result) {
    assertTrue(
        MappingRuleResultType.REJECT == result.getResult());
  }


  void assertSkipResult(MappingRuleResult result) {
  assertTrue(
        MappingRuleResultType.SKIP == result.getResult());
  }

  void assertPlaceDefaultResult(MappingRuleResult result) {
    assertTrue(
        MappingRuleResultType.PLACE_TO_DEFAULT == result.getResult());
  }

  void assertPlaceResult(MappingRuleResult result, String queue) {
    assertTrue(
        MappingRuleResultType.PLACE == result.getResult());
    assertEquals(queue, result.getQueue());
  }

  @Test
  public void testRejectAction() {
    VariableContext variables = new VariableContext();
    MappingRuleAction reject = new MappingRuleActions.RejectAction();

    assertRejectResult(reject.execute(variables));
  }

  @Test
  public void testActionFallbacks() {
    MappingRuleActionBase action =
        new MappingRuleActions.PlaceToQueueAction("a");

    action.setFallbackDefaultPlacement();
    assertPlaceDefaultResult(action.getFallback());

    action.setFallbackReject();
    assertRejectResult(action.getFallback());

    action.setFallbackSkip();
    assertSkipResult(action.getFallback());
  }

  @Test
  public void testVariableUpdateAction() {
    VariableContext variables = new VariableContext();
    ImmutableSet<String> immutables =
        ImmutableSet.of("%immutable");
    variables.put("%default", "root.default");
    variables.put("%immutable", "immutable");
    variables.put("%empty", "");
    variables.put("%null", null);
    variables.put("%sub", "xxx");
    variables.setImmutables(immutables);

    MappingRuleAction updateDefaultManual =
        new MappingRuleActions.VariableUpdateAction("%default", "root.%sub");
    MappingRuleAction updateDefaultHelper =
        MappingRuleActions.createUpdateDefaultAction("root.%sub%sub");
    MappingRuleAction updateImmutable =
        new MappingRuleActions.VariableUpdateAction("%immutable", "changed");
    MappingRuleAction updateEmpty =
        new MappingRuleActions.VariableUpdateAction("%empty", "something");
    MappingRuleAction updateNull =
        new MappingRuleActions.VariableUpdateAction("%null", "non-null");

    MappingRuleResult result;

    result = updateDefaultManual.execute(variables);
    assertSkipResult(result);
    assertEquals("root.xxx", variables.get("%default"));

    result = updateDefaultHelper.execute(variables);
    assertSkipResult(result);
    assertEquals("root.xxxxxx", variables.get("%default"));

    result = updateEmpty.execute(variables);
    assertSkipResult(result);
    assertEquals("something", variables.get("%empty"));
    result = updateNull.execute(variables);
    assertSkipResult(result);
    assertEquals("non-null", variables.get("%null"));

    try {
      updateImmutable.execute(variables);
      assertTrue("Should've failed with exception", false);
    } catch (Exception e){
      assertTrue(e instanceof IllegalStateException);
    }
  }

  @Test
  public void testPlaceToQueueAction() {
    VariableContext variables = new VariableContext();
    ImmutableSet<String> immutables =
        ImmutableSet.of("%immutable");
    variables.put("%default", "root.default");
    variables.put("%immutable", "immutable");
    variables.put("%empty", "");
    variables.put("%null", null);
    variables.put("%sub", "xxx");
    variables.setImmutables(immutables);

    MappingRuleAction placeToStatic =
        new MappingRuleActions.PlaceToQueueAction("root.static.queue");

    MappingRuleAction placeToDynamic =
        new MappingRuleActions.PlaceToQueueAction("root.%sub.%immutable");

    MappingRuleAction placeToDynamicDoubleSub =
        new MappingRuleActions.PlaceToQueueAction("root.%sub%sub.%immutable");

    MappingRuleAction placeToNull =
        new MappingRuleActions.PlaceToQueueAction(null);

    MappingRuleAction placeToEmpty =
        new MappingRuleActions.PlaceToQueueAction("");

    MappingRuleAction placeToNulRef =
        new MappingRuleActions.PlaceToQueueAction("%null");

    MappingRuleAction placeToEmptyRef =
        new MappingRuleActions.PlaceToQueueAction("%empty");

    MappingRuleAction placeToDefaultRef =
        new MappingRuleActions.PlaceToQueueAction("%default");

    assertPlaceResult(placeToStatic.execute(variables), "root.static.queue");
    assertPlaceResult(placeToDynamic.execute(variables), "root.xxx.immutable");
    assertPlaceResult(placeToDynamicDoubleSub.execute(variables),
        "root.%sub%sub.immutable");

    assertPlaceResult(placeToNull.execute(variables), "");
    assertPlaceResult(placeToEmpty.execute(variables), "");
    assertPlaceResult(placeToNulRef.execute(variables), "");
    assertPlaceResult(placeToEmptyRef.execute(variables), "");
    assertPlaceResult(placeToDefaultRef.execute(variables), "root.default");
  }

  @Test
  public void testToStrings() {
    MappingRuleAction place = new MappingRuleActions.PlaceToQueueAction("queue");
    MappingRuleAction varUpdate = new MappingRuleActions.VariableUpdateAction(
        "%var", "value");
    MappingRuleAction reject = new MappingRuleActions.RejectAction();

    assertEquals("PlaceToQueueAction{queueName='queue'}", place.toString());
    assertEquals("VariableUpdateAction{variableName='%var'" +
            ", variableValue='value'}", varUpdate.toString());
    assertEquals("RejectAction", reject.toString());
  }
}