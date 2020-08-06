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
