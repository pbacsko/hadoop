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
import io.netty.util.Mapping;
import junit.framework.TestCase;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.junit.Test;

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
  CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
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
    //Capacity scheduler config mocks
    CapacitySchedulerConfiguration csConf =
        mock(CapacitySchedulerConfiguration.class);
    when(csConf.getMappingRules()).thenReturn(mappings);
    when(csConf.getOverrideWithQueueMappings())
        .thenReturn(overrideUserMappings);

    //Setting up queue manager and emulated queue hierarchy
    CapacitySchedulerQueueManager qm =
        mock(CapacitySchedulerQueueManager.class);
    createQueueHierarchy(qm);

    //setting up capacity scheduler mock with qm and config
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
    return ApplicationSubmissionContext.newInstance(
        null, name, queue, null, null, false, false, 2, null, null, false);
  }

  ApplicationSubmissionContext createApp(String name) {
    return createApp(name, YarnConfiguration.DEFAULT_QUEUE_NAME);
  }

  void assertReject(CSMappingPlacementRule engine,
      ApplicationSubmissionContext asc, String user) {
    try {
      engine.getPlacementForApp(asc, user);
      fail("Placement should throw exception!");
    } catch (YarnException e) {
      //unnecessary, but feels more consistent
      assertTrue(true);
    }

  }

  void assertPlace(CSMappingPlacementRule engine,
      ApplicationSubmissionContext asc, String user,
      String expectedQueue) {

    try {
      ApplicationPlacementContext apc = engine.getPlacementForApp(asc, user);
      assertNotNull(apc);
      String queue = apc.getParentQueue() == null ? "" :
          (apc.getParentQueue() + DOT);
      queue += apc.getQueue();
      assertEquals(expectedQueue,  queue);
    } catch (YarnException e) {
      e.printStackTrace();
      fail("Placement should not throw exception!");
    }
  }

  void assertNull(CSMappingPlacementRule engine,
      ApplicationSubmissionContext asc, String user) {
    try {
      assertNull(engine.getPlacementForApp(asc, user));
    } catch (YarnException e) {
      e.printStackTrace();
      fail("Placement should not throw exception!");
    }
  }

  @Test
  public void testLegacyPlacementToExistingQueue() throws IOException {
    ArrayList<MappingRule> rules = new ArrayList<>();
    rules.add(MappingRule.createLegacyRule(
        "u", "alice", "root.ambiguous.user.ambi"));
    rules.add(MappingRule.createLegacyRule(
        "u", "bob", "ambi"));
    rules.add(MappingRule.createLegacyRule(
        "u", "dave", "disambi"));
    rules.add(MappingRule.createLegacyRule(
        "u", "%user", "disambiuser.%user"));

    CSMappingPlacementRule engine = setupEngine(true, rules);
    ApplicationSubmissionContext asc = createApp("Default");
    assertPlace(engine, asc, "alice", "root.ambiguous.user.ambi");
    //should be rejected because ambi is ambiguous
    assertReject(engine, asc, "bob");
    assertPlace(engine, asc, "emily",
        "root.disambiguous.deep.disambiuser.emily");
    //should be rejected because disambiuser.charile does not exit
    assertReject(engine, asc, "charlie");
    assertPlace(engine, asc, "dave",
        "root.disambiguous.deep.disambiuser.disambi");
  }

  public void testLegacyPlacementToManagedQueues() throws IOException {
    ArrayList<MappingRule> rules = new ArrayList<>();
    rules.add(MappingRule.createLegacyRule(
        "u", "alice", "root.ambiguous.managed.%user"));
    rules.add(MappingRule.createLegacyRule(
        "u", "bob", "managed.%user"));
    rules.add(MappingRule.createLegacyRule(
        "u", "charlie", "root.unman.charlie"));
    rules.add(MappingRule.createLegacyRule(
        "u", "dave", "non-existant.%user"));
    rules.add(MappingRule.createLegacyRule(
        "u", "%user", "root.man.%user"));

    CSMappingPlacementRule engine = setupEngine(true, rules);
    ApplicationSubmissionContext asc = createApp("Default");
    assertPlace(engine, asc, "alice", "root.ambiguous.managed.alice");
    //should be rejected because managed is ambiguous
    assertReject(engine, asc, "bob");
    //should be rejected because root.unman is not managed
    assertReject(engine, asc, "charlie");
    //should be rejected because parent queue does not exist
    assertReject(engine, asc, "dave");
    assertPlace(engine, asc, "emily",
        "root.man.emily");
  }

  public void testLegacyPlacementShortReference() throws IOException {
    ArrayList<MappingRule> rules = new ArrayList<>();
    rules.add(MappingRule.createLegacyRule(
        "u", "alice", "non-existant"));
    rules.add(MappingRule.createLegacyRule(
        "u", "bob", "root"));
    rules.add(MappingRule.createLegacyRule(
        "u", "charlie", "man"));
    rules.add(MappingRule.createLegacyRule(
        "u", "dave", "ambi"));

    CSMappingPlacementRule engine = setupEngine(true, rules);
    ApplicationSubmissionContext asc = createApp("Default");
    //should be rejected non-existant does not exist
    assertReject(engine, asc, "alice");
    //should be rejected root is never managed
    assertReject(engine, asc, "bob");
    //should be rejected managed parent is not a leaf queue
    assertReject(engine, asc, "charlie");
    //should be rejected ambi is an ambiguous reference
    assertReject(engine, asc, "dave");
  }

  public void testRuleFallbackHandling() throws IOException {
    ArrayList<MappingRule> rules = new ArrayList<>();
    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("alice"),
            (new MappingRuleActions.PlaceToQueueAction("non-existant"))
                .setFallbackReject()));
    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("bob"),
            (new MappingRuleActions.PlaceToQueueAction("non-existant"))
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
            (new MappingRuleActions.PlaceToQueueAction("non-existant"))
                .setFallbackDefaultPlacement()));
    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("emily"),
            MappingRuleActions.createUpdateDefaultAction("root.invalid")));
    rules.add(
        new MappingRule(
            MappingRuleMatchers.createUserMatcher("emily"),
            (new MappingRuleActions.PlaceToQueueAction("non-existant"))
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

    //Alice has a straight up reject rule, her application should be rejected
    assertReject(engine, fail, "alice");
    //Bob should fail to place to non-existant -> should skip to next rule
    //Bob should update the %default to root.invalid
    //Bob should fail to place the app to %default which is root.invalid
    assertReject(engine, fail, "bob");
    //Charile should be able to place the app to root.default as the
    // non-existant queue does not exist, but fallback is place to default
    assertPlace(engine, success, "charlie", "root.default");
    //Dave with success app has no matching rule, so we expect a null result
    assertNull(engine, success, "dave");
    //Emily should update the %default to root.invalid
    //Bob should fail to place the app to non-existant and since the fallback
    // is placeToDefault, it should also fail, because we have just updated
    // default to an invalid valie
    assertReject(engine, fail, "emily");
  }


  public void testConfigValidation() throws IOException {
    ArrayList<MappingRule> nonExistantStatic = new ArrayList<>();
    nonExistantStatic.add(MappingRule.createLegacyRule(
        "u", "alice", "non-existant"));

    //since the %token is an unkown variable, it will be considered as
    //a literal string, and since %token queue does not exit, it should fail
    ArrayList<MappingRule> tokenAsStatic = new ArrayList<>();
    tokenAsStatic.add(MappingRule.createLegacyRule(
        "u", "alice", "%token"));

    ArrayList<MappingRule> tokenAsDynamic = new ArrayList<>();
    //this rule might change the value of the %token, so the validator will be
    //aware of the %token variable
    tokenAsDynamic.add(new MappingRule(
        new MappingRuleMatchers.MatchAllMatcher(),
        new MappingRuleActions.VariableUpdateAction("%token", "non-existant")
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