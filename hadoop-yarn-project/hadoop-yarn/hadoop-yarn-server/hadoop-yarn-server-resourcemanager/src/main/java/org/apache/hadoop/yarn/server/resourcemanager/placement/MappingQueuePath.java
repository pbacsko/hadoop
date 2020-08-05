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

  String getParent() {
    return parent;
  }

  String getLeafName() {
    return leaf;
  }

  String getFullPath() {
    return hasParent() ? (parent + DOT + leaf) : leaf;
  }

  void normalize(String normalizedPath) {
    normalized = true;
    setFromFullPath(normalizedPath);
  }

  boolean hasParent() {
    return null == parent;
  }

  boolean isNormalized() {
    return normalized;
  }
}
