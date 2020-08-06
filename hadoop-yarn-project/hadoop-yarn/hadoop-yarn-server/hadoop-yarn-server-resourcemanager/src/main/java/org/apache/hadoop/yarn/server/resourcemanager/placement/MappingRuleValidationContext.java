package org.apache.hadoop.yarn.server.resourcemanager.placement;

import org.apache.hadoop.yarn.exceptions.YarnException;

import java.util.Set;

interface MappingRuleValidationContext {
  boolean validateQueuePath(String queuePath) throws YarnException;

  boolean isPathStatic(String queuePath);

  void addVariable(String variable);

  Set<String> getVariables();
}