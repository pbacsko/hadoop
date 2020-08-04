package org.apache.hadoop.yarn.server.resourcemanager.placement;

import com.google.common.collect.Maps;
import org.apache.commons.compress.utils.Lists;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.*;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MockQueueHierarchyBuilder {
  private static final String ROOT = "root";
  private static final String QUEUE_SEP = ".";
  private List<String> queuePaths = Lists.newArrayList();
  private List<String> managedParentQueues = Lists.newArrayList();
  private CapacitySchedulerQueueManager queueManager;

  public static MockQueueHierarchyBuilder create() {
    return new MockQueueHierarchyBuilder();
  }

  public MockQueueHierarchyBuilder withQueueManager(
      CapacitySchedulerQueueManager queueManager) {
    this.queueManager = queueManager;
    return this;
  }

  public MockQueueHierarchyBuilder withQueue(String queue) {
    this.queuePaths.add(queue);
    return this;
  }

  public MockQueueHierarchyBuilder withManagedParentQueue(
      String managedQueue) {
    this.managedParentQueues.add(managedQueue);
    return this;
  }

  public void build() {
    if (this.queueManager == null) {
      throw new IllegalStateException(
          "QueueManager instance is not provided!");
    }

    for (String managedParentQueue : managedParentQueues) {
      if (!queuePaths.contains(managedParentQueue)) {
        queuePaths.add(managedParentQueue);
      } else {
        throw new IllegalStateException("Cannot add a managed parent " +
            "and a simple queue with the same path");
      }
    }

    Map<String, AbstractCSQueue> queues = Maps.newHashMap();
    for (String queuePath : queuePaths) {
      addQueues(queues, queuePath);
    }
  }

  private void addQueues(Map<String, AbstractCSQueue> queues,
      String queuePath) {
    final String[] pathComponents = queuePath.split("\\" + QUEUE_SEP);

    String currentQueuePath = "";
    for (int i = 0; i < pathComponents.length; ++i) {
      boolean isLeaf = i == pathComponents.length - 1;
      String queueName = pathComponents[i];
      String parentPath = currentQueuePath;
      currentQueuePath += currentQueuePath.equals("") ?
          queueName : QUEUE_SEP + queueName;

      if (managedParentQueues.contains(parentPath) && !isLeaf) {
        throw new IllegalStateException("Cannot add a queue under " +
            "managed parent");
      }
      if (!queues.containsKey(currentQueuePath)) {
        ParentQueue parentQueue = (ParentQueue) queues.get(parentPath);
        AbstractCSQueue queue = createQueue(parentQueue, queueName,
            currentQueuePath, isLeaf);
        queues.put(currentQueuePath, queue);
      }
    }
  }

  private AbstractCSQueue createQueue(ParentQueue parentQueue,
      String queueName, String currentQueuePath, boolean isLeaf) {
    if (queueName.equals(ROOT)) {
      return createRootQueue(ROOT);
    } else if (managedParentQueues.contains(currentQueuePath)) {
      return addManagedParentQueueAsChildOf(parentQueue, queueName);
    } else if (isLeaf) {
      return addLeafQueueAsChildOf(parentQueue, queueName);
    } else {
      return addParentQueueAsChildOf(parentQueue, queueName);
    }
  }

  private AbstractCSQueue createRootQueue(String rootQueueName) {
    ParentQueue root = mock(ParentQueue.class);
    when(root.getQueuePath()).thenReturn(rootQueueName);
    when(queueManager.getQueue(rootQueueName)).thenReturn(root);
    when(queueManager.getQueueByFullName(rootQueueName)).thenReturn(root);
    return root;
  }

  private AbstractCSQueue addParentQueueAsChildOf(ParentQueue parent,
      String queueName) {
    ParentQueue queue = mock(ParentQueue.class);
    setQueueFields(parent, queue, queueName);
    return queue;
  }

  private AbstractCSQueue addManagedParentQueueAsChildOf(ParentQueue parent,
      String queueName) {
    ManagedParentQueue queue = mock(ManagedParentQueue.class);
    setQueueFields(parent, queue, queueName);
    return queue;
  }

  private AbstractCSQueue addLeafQueueAsChildOf(ParentQueue parent,
      String queueName) {
    LeafQueue queue = mock(LeafQueue.class);
    setQueueFields(parent, queue, queueName);
    return queue;
  }

  private void setQueueFields(ParentQueue parent, AbstractCSQueue newQueue,
      String queueName) {
    String fullPathOfQueue = parent.getQueuePath() + QUEUE_SEP + queueName;
    addQueueToQueueManager(queueName, newQueue, fullPathOfQueue);

    when(newQueue.getParent()).thenReturn(parent);
    when(newQueue.getQueuePath()).thenReturn(fullPathOfQueue);
    when(newQueue.getQueueName()).thenReturn(queueName);
  }

  private void addQueueToQueueManager(String queueName, AbstractCSQueue queue,
      String fullPathOfQueue) {
    when(queueManager.getQueue(queueName)).thenReturn(queue);
    when(queueManager.getQueue(fullPathOfQueue)).thenReturn(queue);
    when(queueManager.getQueueByFullName(fullPathOfQueue)).thenReturn(queue);
  }
}
