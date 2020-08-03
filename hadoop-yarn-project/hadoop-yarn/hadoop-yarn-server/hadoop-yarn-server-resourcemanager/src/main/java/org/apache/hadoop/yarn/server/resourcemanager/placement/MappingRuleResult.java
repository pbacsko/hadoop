package org.apache.hadoop.yarn.server.resourcemanager.placement;

/**
 * This class represents the outcome of an action
 */
public class MappingRuleResult {

  //The name of the queue we should place our application into
  //Only valid if result == PLACE
  private String queue;

  //The normalized name of the queue, since CS allows users to referencce queues
  //by only their leaf name, we need to normalize those queues to have full
  //reference
  private String normalizedQueue;

  //The result of the action
  private MappingRuleResultType result;

  //To the reject result has no variable field, so we don't have to create
  // a new instance all the time
  // this is THE instance which will be used to represent REJECT
  private static final MappingRuleResult RESULT_REJECT
      = new MappingRuleResult(null, MappingRuleResultType.REJECT);

  //To the skip result has no variable field, so we don't have to create
  // a new instance all the time
  // this is THE instance which will be used to represent SKIP
  private static final MappingRuleResult RESULT_SKIP
      = new MappingRuleResult(null, MappingRuleResultType.SKIP);

  //To the default placement result has no variable field, so we don't have to
  // create a new instance all the time
  // this is THE instance which will be used to represent default placement
  private static final MappingRuleResult RESULT_DEFAULT_PLACEMENT
      = new MappingRuleResult(null, MappingRuleResultType.PLACE_TO_DEFAULT);

  /**
   * Constructor is private to force the user to use the predefined generator
   * methods to create new instances in order to avoid inconsistent states.
   * @param queue Name of the queue in which the application is supposed to be
   *              placed, only valid if result == PLACE
   *              otherwise it should be null
   * @param result The type of the result
   */
  private MappingRuleResult(String queue, MappingRuleResultType result) {
    this.queue = queue;
    this.normalizedQueue = queue;
    this.result = result;
  }

  public String getQueue() {
    return queue;
  }

  public void updateNormalizedQueue(String normalizedQueue) {
    this.normalizedQueue = normalizedQueue;
  }

  public String getNormalizedQueue() {
    return normalizedQueue;
  }

  public MappingRuleResultType getResult() {
    return result;
  }

  /**
   * Generator method for place results.
   * @param queue The name of the queue in which we shall place the application
   * @return The generated MappingRuleResult
   */
  public static MappingRuleResult createPlacementResult(String queue) {
    return new MappingRuleResult(queue, MappingRuleResultType.PLACE);
  }

  /**
   * Generator method for reject results.
   * @return The generated MappingRuleResult
   */
  public static MappingRuleResult createRejectResult() {
    return RESULT_REJECT;
  }

  /**
   * Generator method for skip results.
   * @return The generated MappingRuleResult
   */
  public static MappingRuleResult createSkipResult() {
    return RESULT_SKIP;
  }

  /**
   * Generator method for default placement results. It is a specialized
   * placement result which will only use the "%default" as a queue name
   * @return The generated MappingRuleResult
   */
  public static MappingRuleResult createDefaultPlacementResult() {
    return RESULT_DEFAULT_PLACEMENT;
  }

  @Override
  public String toString() {
    if (result == MappingRuleResultType.PLACE) {
      return result.name() + ": '" + normalizedQueue + "' ('" + queue + "')";
    } else {
      return result.name();
    }
  }
}
