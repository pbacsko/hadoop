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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.*;

import java.util.Set;

public class MappingRuleValidationContextImpl
    implements MappingRuleValidationContext {
  private Set<String> knownVariables = Sets.newHashSet();
  private CapacitySchedulerQueueManager queueManager;

  MappingRuleValidationContextImpl(CapacitySchedulerQueueManager qm) {
    queueManager = qm;
  }

  private boolean validateStaticQueuePath(MappingQueuePath path)
      throws YarnException {
      //Try getting queue by its full path name, if it exists it is a static
      //leaf queue indeed, without any auto creation magic
      CSQueue queue = queueManager.getQueue(path.getFullPath());
      if (queue == null) {
        //We might not be able to find the queue, because the reference was
        // ambiguous this should only happen if the queue was referenced by
        // leaf name only
        if (queueManager.isAmbiguous(path.getFullPath())) {
          throw new YarnException(
              "Target queue is an ambiguous leaf queue '" +
              path.getFullPath() + "'");
        }

        //if leaf queue does not exist,
        //we need to check if the parent exists and is a managed parent
        if (!path.hasParent()) {
          throw new YarnException(
              "Target queue does not exist and has no parent defined '" +
              path.getFullPath() + "'");
        }

        CSQueue parentQueue = queueManager.getQueue(path.getParent());
        if (parentQueue == null) {
          if (queueManager.isAmbiguous(path.getParent())) {
            throw new YarnException("Target queue path '" + path +
                "' contains an ambiguous parent queue '" +
                path.getParent() + "' reference");
          } else {
            throw new YarnException("Target queue path '" + path + "' " +
                "contains an invalid parent queue '" + path.getParent() + "'.");
          }
        }

        if (!(parentQueue instanceof ManagedParentQueue)) {
          //If the parent path was referenced by short name, and it is not
          // managed, we look up if there is a queue under it with the leaf
          // queue's name
          String normalizedParentPath = parentQueue.getQueuePath() + "."
              + path.getLeafName();
          CSQueue normalizedQueue = queueManager.getQueue(normalizedParentPath);
          if (normalizedQueue instanceof LeafQueue) {
            return true;
          }

          if (normalizedQueue == null) {
            throw new YarnException(
                "Target queue '" + path.getFullPath() + "' does not exist" +
                " and has a non-managed parent queue defined.");
          } else {
            throw new YarnException("Target queue '" + path + "' references" +
                "a non-leaf queue, target queues must always be " +
                "leaf queues.");
          }

        }

      } else {
        // if queue exists, validate if its an instance of leaf queue
        if (!(queue instanceof LeafQueue)) {
          throw new YarnException("Target queue '" + path + "' references" +
              "a non-leaf queue, target queues must always be " +
              "leaf queues.");
        }
      }
    return true;
  }

  private boolean validateDynamicQueuePath(MappingQueuePath path)
      throws YarnException{
    //if the queue is dynamic and we don't have a parent path, we cannot do
    //any validation, since the dynamic part can be substituted to anything
    //and that is the only part
    if (!path.hasParent()) {
      return true;
    }

    String parent = path.getParent();
    //if the parent path has dynamic parts, we cannot do any more validations
    if (!isPathStatic(parent)) {
      return true;
    }

    //We check if the parent queue exists
    CSQueue parentQueue = queueManager.getQueue(parent);
    if (parentQueue == null) {
      throw new YarnException("Target queue path '" + path + "' contains an " +
          "invalid parent queue");
    }

    if (!(parentQueue instanceof ManagedParentQueue)) {
      for (CSQueue queue : parentQueue.getChildQueues()) {
        if (queue instanceof LeafQueue) {
          //if a non managed parent queue has at least one leaf queue, this
          //mapping can be valid, we cannot do any more checks
          return true;
        }
      }

      //There is no way we can place anything into the queue referenced by the
      // rule, because we cannot auto create, and we don't have any leaf queues
      //Actually this branch is not accessibe with the current queue hierarchy,
      //there should be no parents without any leaf queues. This condition says
      //for sanity checks
      throw new YarnException("Target queue path '" + path + "' has" +
          "a non-managed parent queue which has no LeafQueues either.");
    }

    return true;
  }

  public boolean validateQueuePath(String queuePath) throws YarnException {
    MappingQueuePath path = new MappingQueuePath(queuePath);

    if (isPathStatic(queuePath)) {
      return validateStaticQueuePath(path);
    } else {
      return validateDynamicQueuePath(path);
    }
  }

  public boolean isPathStatic(String queuePath) {
    String[] parts = queuePath.split("\\.");
    for (int i = 0; i < parts.length; i++) {
      if (knownVariables.contains(parts[i])) {
        return false;
      }
    }

    return true;
  }

  public void addVariable(String variable) {
    knownVariables.add(variable);
  }

  public Set<String> getVariables() {
    return ImmutableSet.copyOf(knownVariables);
  }
}
