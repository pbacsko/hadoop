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

import org.apache.hadoop.yarn.exceptions.YarnException;

import java.util.Set;

interface MappingRuleValidationContext {
  /**
   * This method should determine if the provided queue path can result in
   * a possible placement. It should fail if the provided path cannot be placed
   * into any of the known queues regardless of the variable context.
   * @param queuePath The path to check
   * @return true if the validation was successful
   * @throws YarnException if the provided queue path is invalid
   */
  boolean validateQueuePath(String queuePath) throws YarnException;

  /**
   * Method to determine if the provided queue path contains any dynamic parts
   * A part is dynamic if a known variable is referenced in it.
   * @param queuePath The path to check
   * @return true if no dynamic parts were found
   */
  boolean isPathStatic(String queuePath);

  /**
   * This method will add a known variable to the validation context, known
   * variables can be used to determine if a path is static or dynamic
   * @param variable Name of the variable
   * @throws YarnException If the variable to be added has already added as an
   * immutable one, an exception is thrown
   */
  void addVariable(String variable) throws YarnException;

  /**
   * This method will add a known immutable variable to the validation context,
   * known variables can be used to determine if a path is static or dynamic
   * @param variable Name of the immutable variable
   * @throws YarnException If the variable to be added has already added as a
   * regular, mutable variable an exception is thrown
   */
  void addImmutableVariable(String variable) throws YarnException;

  /**
   * This method will return all the known variables
   * @return Set of the known variables
   */
  Set<String> getVariables();
}