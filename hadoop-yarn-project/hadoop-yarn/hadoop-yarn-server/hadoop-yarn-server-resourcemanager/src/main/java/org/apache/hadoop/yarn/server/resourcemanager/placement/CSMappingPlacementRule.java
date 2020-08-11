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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;

/**
 * This class is responsible for making application submissions to queue
 * assignments, based on the configured ruleset. This class supports all
 * features supported by UserGroupMappingPlacementRule and
 * AppNameMappingPlacementRule classes, also adding some features which are
 * present in fair scheduler queue placement. This helps to reduce the gap
 * between the two schedulers.
 */
public class CSMappingPlacementRule extends PlacementRule {
  private static final Logger LOG = LoggerFactory
      .getLogger(CSMappingPlacementRule.class);

  private CapacitySchedulerQueueManager queueManager;
  private List<MappingRule> mappingRules;

  /**
   * These are the variables we associate a special meaning, these should be
   * immutable for each variable context.
   */
  private ImmutableSet<String> immutableVariables = ImmutableSet.of(
      "%user",
      "%primary_group",
      "%secondary_group",
      "%application",
      "%specified"
      );

  private Groups groups;
  private boolean overrideWithQueueMappings;
  private boolean failOnConfigError = true;

  @VisibleForTesting
  public void setGroups(Groups groups) {
    this.groups = groups;
  }

  public void setFailOnConfigError(boolean failOnConfigError) {
    this.failOnConfigError = failOnConfigError;
  }

  @Override
  public boolean initialize(ResourceScheduler scheduler) throws IOException {
    if (!(scheduler instanceof CapacityScheduler)) {
      throw new IOException(
        "CSMappingPlacementRule can be only used with CapacityScheduler");
    }
    LOG.info("Initializing CSMappingPlacementRule queue mapping manager.");

    CapacitySchedulerContext csx = (CapacitySchedulerContext) scheduler;
    queueManager = csx.getCapacitySchedulerQueueManager();

    CapacitySchedulerConfiguration conf = csx.getConfiguration();
    overrideWithQueueMappings = conf.getOverrideWithQueueMappings();

    if (groups == null) {
      groups = Groups.getUserToGroupsMappingService(conf);
    }

    MappingRuleValidationContext validationContext =
        new MappingRuleValidationContextImpl(queueManager);

    //Adding all immutable variables to the known variable list
    immutableVariables.forEach(var -> validationContext.addVariable(var));
    //Immutables + %default are the only officially supported variables,
    //We initialize the context with these, and let the rules to extend the list
    validationContext.addVariable("%default");

    //Getting and validating mapping rules
    mappingRules = conf.getMappingRules();
    for (MappingRule rule : mappingRules) {
      try {
        rule.validate(validationContext);
      } catch (YarnException e) {
        LOG.error("Error initializing queue mappings, rule '" + rule + "' " +
            "has encountered a validation error: " + e.getMessage());
        if (failOnConfigError) {
          throw new IOException(e);
        }
      }
    }

    LOG.info("Initialized queue mappings, can override user specified " +
      "queues: {}  number of rules: {}",
      overrideWithQueueMappings, mappingRules.size());

    if (LOG.isDebugEnabled()) {
      mappingRules.forEach(rule -> LOG.debug(rule.toString()));
    }

    return mappingRules.size() > 0;
  }

  private String getPrimaryGroup(String user) throws IOException {
    return groups.getGroupsSet(user).iterator().next();
  }

  /**
   * Traverse all secondary groups (as there could be more than one
   * and position is not guaranteed) and ensure there is queue with
   * the same name
   * @param user Name of the user
   * @return Name of the secondary group if found, null otherwise
   * @throws IOException
   */
  private String getSecondaryGroup(String user) throws IOException {
    Set<String> groupsSet = groups.getGroupsSet(user);
    String secondaryGroup = null;
    Iterator<String> it = groupsSet.iterator();
    it.next();
    while (it.hasNext()) {
      String group = it.next();
      if (this.queueManager.getQueue(group) != null) {
        secondaryGroup = group;
        break;
      }
    }

    if (secondaryGroup == null && LOG.isDebugEnabled()) {
      LOG.debug("User {} is not associated with any Secondary "
        + "Group. Hence it may use the 'default' queue", user);
    }
    return secondaryGroup;
  }

  private VariableContext createVariableContext(
      ApplicationSubmissionContext asc, String user) throws IOException {
    VariableContext vctx = new VariableContext();

    vctx.put("%user", user);
    vctx.put("%specified", asc.getQueue());
    vctx.put("%application", asc.getApplicationName());
    vctx.put("%primary_group", getPrimaryGroup(user));
    vctx.put("%secondary_group", getSecondaryGroup(user));
    vctx.put("%default", "root.default");

    vctx.setImmutables(immutableVariables);
    return vctx;
  }

  private String validateAndNormalizeQueue(String queueName)
      throws YarnException {
    MappingQueuePath path = new MappingQueuePath(queueName);
    String leaf = path.getLeafName();
    String parent = path.getParent();

    String normalizedName;
    if (parent != null) {
      normalizedName = validateAndNormalizeQueueWithParent(parent, leaf);
    } else {
      normalizedName = validateAndNormalizeQueueWithNoParent(leaf);
    }

    CSQueue queue = queueManager.getQueueByFullName(normalizedName);
    if (queue != null && !(queue instanceof LeafQueue)) {
      throw new YarnException("Mapping rule returned a non-leaf queue '" +
          normalizedName + "', cannot place application in it.");
    }

    return normalizedName;
  }

  private String validateAndNormalizeQueueWithParent(String parent, String leaf)
      throws YarnException {
    CSQueue parentQueue = queueManager.getQueue(parent);
    //we don't find the specified parent, so the placement rule is invalid
    //for this case
    if (parentQueue == null) {
      if (queueManager.isAmbiguous(parent)) {
        throw new YarnException("Mapping rule specified a parent queue '" +
            parent + "', but it is ambiguous.");
      } else {
        throw new YarnException("Mapping rule specified a parent queue '" +
            parent + "', but it does not exist.");
      }
    }

    //normalizing parent path
    String parentPath = parentQueue.getQueuePath();
    String fullPath = parentPath + DOT + leaf;

    //if we have a parent which is not a managed parent, we check if the leaf
    //queue exists under this parent
    if (!(parentQueue instanceof ManagedParentQueue)) {
      CSQueue queue = queueManager.getQueue(fullPath);
      //if the queue doesn't exit we return null
      if (queue == null) {
        throw new YarnException("Mapping rule specified a parent queue '" +
            parent + "', but it is not a managed parent queue, " +
            "and no queue exists with name '" + leaf + "' under it.");
      }
    }
    //at this point we either have a managed parent or the queue actually
    //exists so we have a placement context, returning it
    return fullPath;
  }

  private String validateAndNormalizeQueueWithNoParent(String leaf)
      throws YarnException {
    //in this case we don't have a parent specified so we expect the queue to
    //exist, otherwise the mapping will not be valid for this case
    CSQueue queue = queueManager.getQueue(leaf);
    if (queue == null) {
      if (queueManager.isAmbiguous(leaf)) {
        throw new YarnException("Queue '" + leaf + "' specified in mapping" +
            " rule is ambiguous");
      } else {
        throw new YarnException("Queue '" + leaf + "' specified in mapping" +
            " rule does not exist.");
      }
    }

    //normalizing queue path
    return queue.getQueuePath();
  }

  /**
   * Evaluates the mapping rule using the provided variable context. For
   * placement results we check if the placement is valid, and in case of
   * invalid placements we use the rule's fallback settings to get the result.
   * @param rule The mapping rule to be evaluated
   * @param variables The variables and their respective values
   * @return Evaluation result
   */
  private MappingRuleResult evaluateRule(
      MappingRule rule, VariableContext variables) {
    MappingRuleResult result = rule.evaluate(variables);

    if (result.getResult() == MappingRuleResultType.PLACE) {
      try {
        result.updateNormalizedQueue(
            validateAndNormalizeQueue(result.getQueue()));
      } catch (Exception e) {
        LOG.info("Cannot place to queue '" + result.getQueue() +
          "' returned by mapping rule.", e);
        result = rule.getFallback();
      }
    }

    return result;
  }

  private ApplicationPlacementContext createPlacementContext(String queueName) {
    int parentQueueNameEndIndex = queueName.lastIndexOf(DOT);
    if (parentQueueNameEndIndex > -1) {
      String parent = queueName.substring(0, parentQueueNameEndIndex).trim();
      String leaf = queueName.substring(parentQueueNameEndIndex + 1).trim();
      return new ApplicationPlacementContext(leaf, parent);
    }

    //this statement is here only for future proofing and consistency.
    // Currently there is no valid queue name which does not have a parent
    // and valid for app placement. Since we normalize all paths, the only queue
    // which can have no parent at this point is 'root', which is neither a
    // leaf queue nor a managerParent queue. But it might become one, and
    // it's better to leave the code consistent.
    return new ApplicationPlacementContext(queueName);
  }

  @Override
  public ApplicationPlacementContext getPlacementForApp(
      ApplicationSubmissionContext asc, String user) throws YarnException {
    //We only use the mapping rules if overrideWithQueueMappings enabled
    //  or the application is submitted to the default queue, which effectively
    //  means the application doesn't have any specific queue.
    String appQueue = asc.getQueue();
    if (appQueue != null &&
        !appQueue.equals(YarnConfiguration.DEFAULT_QUEUE_NAME) &&
        !overrideWithQueueMappings) {
      return null;
    }

    VariableContext variables;
    try {
      variables = createVariableContext(asc, user);
    } catch (IOException e) {
      LOG.error("Unable to setup variable context", e);
      throw new YarnException(e);
    }

    for (MappingRule rule : mappingRules) {
      MappingRuleResult result = evaluateRule(rule, variables);
      switch (result.getResult()) {
        case PLACE_TO_DEFAULT:
          return placeToDefault(asc, variables, rule);
        case PLACE:
          return placeToQueue(asc, rule, result);
        case REJECT:
          LOG.info("Rejecting application '{}', reason: Mapping rule '{}' "
            + " fallback action action is set to REJECT.",
            asc.getApplicationName(), rule);
          //We intentionally omit the details, we don't want any server side
          //config information to leak to the client side
          throw new YarnException("Application have been rejected by a" +
              " mapping rule. Please see the logs for details");
        case SKIP:
        //SKIP means skip to the next rule, which is the default behaviour of
        //the for loop, so we don't need to take any extra actions
          break;
        default:
          LOG.error("Invalid result '{}'", result);
      }
    }

    //If no rule was applied we return null, to let the engine move onto the
    //next placementRule class
    return null;
  }

  private ApplicationPlacementContext placeToQueue(
      ApplicationSubmissionContext asc,
      MappingRule rule,
      MappingRuleResult result) {
    LOG.debug("Application '{}' have been placed to queue '{}' by " +
      "rule {}", asc.getApplicationName(), result.getNormalizedQueue(),
      rule);
    //evaluateRule will only return a PLACE rule, if it is verified
    //and normalized, so it is safe here to simply create the placement
    //context
    return createPlacementContext(result.getNormalizedQueue());
  }

  private ApplicationPlacementContext placeToDefault(
      ApplicationSubmissionContext asc,
      VariableContext variables,
      MappingRule rule) throws YarnException {
    try {
      String queueName = validateAndNormalizeQueue(
          variables.replacePathVariables("%default"));
      LOG.debug("Application '{}' have been placed to queue '{}' by " +
              "the fallback option of rule {}",
          asc.getApplicationName(), queueName, rule);
      return createPlacementContext(queueName);
    } catch (YarnException e) {
      LOG.error("Rejecting application due to a failed fallback" +
          " action '{}'" + ", reason: {}", asc.getApplicationName(),
          e.getMessage());
      //We intentionally omit the details, we don't want any server side
      //config information to leak to the client side
      throw new YarnException("Application have been rejected by a" +
          " mapping rule. Please see the logs for details");
    }
  }
}
