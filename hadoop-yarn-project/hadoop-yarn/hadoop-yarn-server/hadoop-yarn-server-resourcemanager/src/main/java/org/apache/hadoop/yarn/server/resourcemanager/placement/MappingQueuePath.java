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

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;

public class MappingQueuePath {
  private String parent;
  private String leaf;
  private boolean normalized = false;

  public MappingQueuePath(String parent, String leaf) {
    this.parent = parent;
    this.leaf = leaf;
  }

  public MappingQueuePath(String fullPath) {
    setFromFullPath(fullPath);
  }

  private void setFromFullPath(String fullPath) {
    parent = null;
    leaf = fullPath;

    int lastDotIdx = fullPath.lastIndexOf(DOT);
    if (lastDotIdx > -1) {
      parent = fullPath.substring(0, lastDotIdx).trim();
      leaf = fullPath.substring(lastDotIdx + 1).trim();
    }
  }

  public String getParent() {
    return parent;
  }

  public String getLeafName() {
    return leaf;
  }

  public String getFullPath() {
    return hasParent() ? (parent + DOT + leaf) : leaf;
  }

  public void normalize(String normalizedPath) {
    normalized = true;
    setFromFullPath(normalizedPath);
  }

  public boolean hasParent() {
    return null != parent;
  }

  public boolean isNormalized() {
    return normalized;
  }

  @Override
  public String toString() {
    return getFullPath();
  }
}
