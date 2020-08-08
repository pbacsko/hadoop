package org.apache.hadoop.yarn.server.resourcemanager.placement;

public enum MappingRuleResultType {
  /**
   * Represents a result where we simply ignore the current rule
   * and move onto the next one
   */
  SKIP,

  /**
   * Represents a result where the application gets rejected
   */
  REJECT,

  /**
   * Represents a result where the application gets placed into a queue
   */
  PLACE,

  /**
   * Special placement, which means the application is to be placed to the
   * queue marked by %default variable
   */
  PLACE_TO_DEFAULT
}
