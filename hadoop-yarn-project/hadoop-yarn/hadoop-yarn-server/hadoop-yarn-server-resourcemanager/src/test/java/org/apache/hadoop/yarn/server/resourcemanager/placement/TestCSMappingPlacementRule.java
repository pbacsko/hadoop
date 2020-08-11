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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import junit.framework.TestCase;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.yarn.server.resourcemanager.placement.FairQueuePlacementUtils.DOT;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCSMappingPlacementRule extends TestCase {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestCSMappingPlacementRule.class);
  Map<String, Set<String>> userGroups = ImmutableMap.of(
      "alice", ImmutableSet.of("p_alice", "user", "developer"),
      "bob", ImmutableSet.of("p_bob", "user", "developer"),
      "charlie", ImmutableSet.of("p_charlie", "user", "tester"),
      "dave", ImmutableSet.of("user", "tester"),
      "emily", ImmutableSet.of("user", "tester", "developer")
  );

  public void createQueueHierarchy(CapacitySchedulerQueueManager queueManager) {
    MockQueueHierarchyBuilder.create()
        .withQueueManager(queueManager)
        .withQueue("root.unman")
        .withQueue("root.default")
        .withManagedParentQueue("root.man")
        .withQueue("root.user.alice")
        .withQueue("root.user.bob")
        .withQueue("root.ambiguous.user.charlie")
        .withQueue("root.ambiguous.user.dave")
        .withQueue("root.ambiguous.user.ambi")
        .withQueue("root.ambiguous.group.tester")
        .withManagedParentQueue("root.ambiguous.managed")
        .withQueue("root.ambiguous.deep.user.charlie")
        .withQueue("root.ambiguous.deep.user.dave")
        .withQueue("root.ambiguous.deep.user.ambi")
        .withQueue("root.ambiguous.deep.group.tester")
        .withManagedParentQueue("root.ambiguous.deep.managed")
        .withQueue("root.disambiguous.deep.disambiuser.emily")
        .withQueue("root.disambiguous.deep.disambiuser.disambi")
        .withQueue("root.disambiguous.deep.group.developer")
        .withManagedParentQueue("root.disambiguous.deep.dman")
        .build();

    when(queueManager.getQueue(isNull())).thenReturn(null);
    when(queueManager.isAmbiguous("primarygrouponly")).thenReturn(true);
  }

  CSMappingPlacementRule setupEngine(
      boolean overrideUserMappings, List<MappingRule> mappings)
      throws IOException {
    return setupEngine(overrideUserMappings, mappings, false);
  }

  CSMappingPlacementRule setupEngine(
      boolean overrideUserMappings, List<MappingRule> mappings,
      boolean failOnConfigError)
      throws IOException {

    CapacitySchedulerConfiguration csConf =
        mock(CapacitySchedulerConfiguration.class);
    when(csConf.getMappingRules()).thenReturn(mappings);
    when(csConf.getOverrideWithQueueMappings())
        .thenReturn(overrideUserMappings);

    CapacitySchedulerQueueManager qm =
        mock(CapacitySchedulerQueueManager.class);
    createQueueHierarchy(qm);

    CapacityScheduler cs = mock(CapacityScheduler.class);
    when(cs.getConfiguration()).thenReturn(csConf);
    when(cs.getCapacitySchedulerQueueManager()).thenReturn(qm);

    CSMappingPlacementRule engine = new CSMappingPlacementRule();
    Groups groups = mock(Groups.class);

    //Initializing group provider to return groups specified in the userGroup
    //  map for each respective user
    for (String user : userGroups.keySet()) {
      when(groups.getGroupsSet(user)).thenReturn(userGroups.get(user));
    }
    engine.setGroups(groups);
    engine.setFailOnConfigError(failOnConfigError);
    engine.initialize(cs);

    return engine;
  }

  ApplicationSubmissionContext createApp(String name, String queue) {
    ApplicationSubmissionContext ctx = Records.newRecord(ApplicationSubmissionContext.class);
    ctx.setApplicationName(name);
    ctx.setQueue(queue);
    return ctx;
  }

  ApplicationSubmissionContext createApp(String name) {
    return createApp(name, YarnConfiguration.DEFAULT_QUEUE_NAME);
  }

  void assertReject(CSMappingPlacementRule engine,
      ApplicationSubmissionContext asc, String user) {
    assertReject("Placement should throw exception!", engine, asc, user);
  }

  void assertReject(String message, CSMappingPlacementRule engine,
      ApplicationSubmissionContext asc, String user) {
    try {
      engine.getPlacementForApp(asc, user);
      fail(message);
    } catch (YarnException e) {
      //To prevent PlacementRule chaining present in PlacementManager
      //when an application is rejected an exception is thrown to make sure
      //no other engine will try to place it.
      assertTrue(true);
    }
  }

  void assertPlace(CSMappingPlacementRule engine,
      ApplicationSubmissionContext asc, String user, String expectedQueue) {
    assertPlace("Placement should not throw exception!",
        engine, asc, user, expectedQueue);
  }

  void assertPlace(String message, CSMappingPlacementRule engine,
      ApplicationSubmissionContext asc, String user, String expectedQueue) {
    try {
      ApplicationPlacementContext apc = engine.getPlacementForApp(asc, user);
      assertNotNull(apc);
      String queue = apc.getParentQueue() == null ? "" :
          (apc.getParentQueue() + DOT);
      queue += apc.getQueue();
      assertEquals(expectedQueue,  queue);
    } catch (YarnException e) {
      LOG.error("{} {}", message, e);
      fail(message);
    }
  }

  void assertNull(CSMappingPlacementRule engine,
      ApplicationSubmissionContext asc, String user) {
    assertNull("Placement should not throw exception!", engine, asc, user);
  }

  void assertNull(String message, CSMappingPlacementRule engine,
      ApplicationSubmissionContext asc, String user) {
    try {
      assertNull(engine.getPlacementForApp(asc, user));
    } catch (YarnException e) {
      LOG.error("{} {}", message, e);
      fail(message);
    }
  }

  @Test
  public void testLegacyPlacementToExistingQueue() throws IOException {
    ArrayList<MappingRule> rules = new ArrayList<>();
    rules.add(MappingRule.createLegacyRule(
        "u", "alice", "root.ambiguous.user.ambi"));
    rules.add(MappingRule.createLegacyRule("u", "bob", "ambi"));
    rules.add(MappingRule.createLegacyRule("u", "dave", "disambi"));
    rules.add(MappingRule.createLegacyRule("u", "%user", "disambiuser.%user"));

    CSMappingPlacementRule engine = setupEngine(true, rules);
    ApplicationSubmissionContext asc = createApp("Default");
    assertPlace(engine, asc, "alice", "root.ambiguous.user.ambi");
    assertReject("Should be rejected because ambi is ambiguous",
        engine, asc, "bob");
    assertPlace(engine, asc, "emily",
        "root.disambiguous.deep.disambiuser.emily");
    assertReject("Should be rejected because disambiuser.charile does not exit",
        engine, asc, "charlie");
    assertPlace(engine, asc, "dave",
        "root.disambiguous.deep.disambiuser.disambi");
  }

  @Test
  public void testLegacyPlacementToManagedQueues() throws IOException {
    ArrayList<MappingRule> rules = new ArrayList<>();
    rules.add(MappingRule.createLegacyRule(
        "u", "alice", "root.ambiguous.managed.%user"));
    rules.add(MappingRule.createLegacyRule(
        "u", "bob", "managed.%user"));
    rules.add(MappingRule.createLegacyRule(
        "u", "charlie", "root.unman.charlie"));
    rules.add(MappingRule.createLegacyRule(
        "u", "dave", "non-existent.%user"));
    rules.add(MappingRule.createLegacyRule(
        "u", "%user", "root.man.%user"));

    CSMappingPlacementRule engine = setupEngine(true, rules);
    ApplicationSubmissionContext asc = createApp("Default");
    assertPlace(engine, asc, "alice", "root.ambiguous.managed.alice");
    assertReject("Should be rejected because managed is ambiguous",
        engine, asc, "bob");
    assertReject("Should be rejected because root.unman is not managed",
        engine, asc, "charlie");
    assertReject("Should be rejected because parent queue does not exist",
        engine, asc, "dave");
    assertPlace(engine, asc, "emily",
        "root.man.emily");
  }

  @Test
  public void testLegacyPlacementShortReference() throws IOException {
    ArrayList<MappingRule> rules = new ArrayList<>();
    rules.add(MappingRule.createLegacyRule(
        "u", "alice", "non-existent"));
    rules.add(MappingRule.createLegacyRule(
        "u", "bob", "root"));
    rules.add(MappingRule.createLegacyRule(
        "u", "charlie", "man"));
    rules.add(MappingRule.createLegacyRule(
        "u", "dave", "ambi"));

    CSMappingPlacementRule engine = setupEngine(true, rules);
    ApplicationSubmissionContext asc = createApp("Default");
    assertReject("Should be rejected non-existent does not exist",
        engine, asc, "alice");
    assertReject("Should be rejected root is never managed",
        engine, asc, "bob");
    assertReject("Should be rejected managed parent is not a leaf queue",
        engine, asc, "charlie");
    assertReject("Should be rejected ambi is an ambiguous reference",
        engine, asc, "dave");
  }

  @Test
  public void testRuleFallbackHandling() throws IOException {
    ArrayList<MappingRule> rules = new ArrayList<>();
    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("alice"),
            (new MappingRuleActions.PlaceToQueueAction("non-existent"))
                .setFallbackReject()));
    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("bob"),
            (new MappingRuleActions.PlaceToQueueAction("non-existent"))
                .setFallbackSkip()));
    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("bob"),
            MappingRuleActions.createUpdateDefaultAction("root.invalid")));
    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("bob"),
            new MappingRuleActions.PlaceToQueueAction("%default")));
    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("charlie"),
            (new MappingRuleActions.PlaceToQueueAction("non-existent"))
                .setFallbackDefaultPlacement()));
    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("emily"),
            MappingRuleActions.createUpdateDefaultAction("root.invalid")));
    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("emily"),
            (new MappingRuleActions.PlaceToQueueAction("non-existent"))
                .setFallbackDefaultPlacement()));
    //This rule is to catch all shouldfail applications, and place them to a
    // queue, so we can detect they were not rejected nor null-ed
    rules.add(
        new MappingRule(
            MappingRuleMatchers.createApplicationNameMatcher("ShouldFail"),
            new MappingRuleActions.PlaceToQueueAction("root.default")));

    CSMappingPlacementRule engine = setupEngine(true, rules);
    ApplicationSubmissionContext fail = createApp("ShouldFail");
    ApplicationSubmissionContext success = createApp("ShouldSucceed");

    assertReject("Alice has a straight up reject rule, " +
        "her application should be rejected",
        engine, fail, "alice");
    assertReject(
        "Bob should fail to place to non-existent -> should skip to next rule" +
        "\nBob should update the %default to root.invalid" +
        "\nBob should fail to place the app to %default which is root.invalid",
        engine, fail, "bob");
    assertPlace(
        "Charile should be able to place the app to root.default as the" +
        "non-existent queue does not exist, but fallback is place to default",
        engine, success, "charlie", "root.default");
    assertNull(
        "Dave with success app has no matching rule, so we expect a null",
        engine, success, "dave");
    assertReject(
        "Emily should update the %default to root.invalid" +
        "\nBob should fail to place the app to non-existent and since the" +
        " fallback is placeToDefault, it should also fail, because we have" +
        " just updated default to an invalid value",
        engine, fail, "emily");
  }

  @Test
  public void testConfigValidation() throws IOException {
    ArrayList<MappingRule> nonExistantStatic = new ArrayList<>();
    nonExistantStatic.add(MappingRule.createLegacyRule(
        "u", "alice", "non-existent"));

    //since the %token is an unknown variable, it will be considered as
    //a literal string, and since %token queue does not exit, it should fail
    ArrayList<MappingRule> tokenAsStatic = new ArrayList<>();
    tokenAsStatic.add(MappingRule.createLegacyRule(
        "u", "alice", "%token"));

    ArrayList<MappingRule> tokenAsDynamic = new ArrayList<>();
    //this rule might change the value of the %token, so the validator will be
    //aware of the %token variable
    tokenAsDynamic.add(new MappingRule(
        new MappingRuleMatchers.MatchAllMatcher(),
        new MappingRuleActions.VariableUpdateAction("%token", "non-existent")
    ));
    //since %token is an known variable, this rule is considered dynamic
    //so it cannot be entirely validated, this init should be successful
    tokenAsDynamic.add(MappingRule.createLegacyRule(
        "u", "alice", "%token"));


    CSMappingPlacementRule engine;

    try {
      engine = setupEngine(true, nonExistantStatic, true);
      fail("We expect the setup to fail");
    } catch (IOException e) {}

    try {
      engine = setupEngine(true, tokenAsStatic, true);
      fail("We expect the setup to fail");
    } catch (IOException e) {}

    try {
      engine = setupEngine(true, tokenAsDynamic, true);
    } catch (IOException e) {
      fail("We expect the setup to succeed");
    }
  }

}