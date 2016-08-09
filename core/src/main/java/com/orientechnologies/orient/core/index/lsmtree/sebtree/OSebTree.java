/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientdb.com
 */

package com.orientechnologies.orient.core.index.lsmtree.sebtree;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.index.lsmtree.*;
import com.orientechnologies.orient.core.index.lsmtree.encoders.OEncoder;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OSessionStoragePerformanceStatistic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Implementation of SB-Tree (Sequentially Efficient B-tree) by Patrick E. O'Neil.
 *
 * @author Sergey Sitnikov
 */
public class OSebTree<K, V> extends ODurableComponent implements OTree<K, V> {

  /* internal */ static final int ENCODERS_VERSION             = 0;
  /* internal */ static final int BLOCK_SIZE                   = 16 /* pages, must be even */;
  /* internal */ static final int INLINE_KEYS_SIZE_THRESHOLD   = 16 /* bytes */;
  /* internal */ static final int INLINE_VALUES_SIZE_THRESHOLD = 10 /* bytes */;

  private static final int BLOCK_HALF = BLOCK_SIZE / 2;

  private OEncoder.Provider<K> keyProvider;
  private OEncoder.Provider<V> valueProvider;
  private OBinarySerializer<K> keySerializer;

  private OType[] keyTypes;
  private int     keySize;
  private boolean nullKeyAllowed;

  private long fileId;

  private boolean opened           = false;
  private String  currentOperation = null;

  public OSebTree(OAbstractPaginatedStorage storage, String name, String extension) {
    super(storage, name, extension, name + extension);
  }

  public void create(OBinarySerializer<K> keySerializer, OType[] keyTypes, int keySize, boolean nullKeyAllowed,
      OBinarySerializer<V> valueSerializer) {
    assert !opened;

    final OSessionStoragePerformanceStatistic statistic = start();
    try {
      final OAtomicOperation atomicOperation = startAtomicOperation("creation", false);
      try {
        this.keySerializer = keySerializer;
        this.keyProvider = selectKeyProvider(keySerializer, keyTypes);
        this.keyTypes = keyTypes == null ? null : Arrays.copyOf(keyTypes, keyTypes.length);
        this.keySize = keySize;
        this.nullKeyAllowed = nullKeyAllowed;
        this.valueProvider = selectValueProvider(valueSerializer);

        fileId = addFile(atomicOperation, getFullName());

        if (nullKeyAllowed) {
          final OSebTreeNode<K, V> nullNode = createNode(atomicOperation).beginCreate();
          try {
            nullNode.create(true);
          } finally {
            releaseNode(atomicOperation, nullNode.endWrite());
          }
        }

        final OSebTreeNode<K, V> rootNode = createNode(atomicOperation).beginCreate();
        try {
          rootNode.create(true);
        } finally {
          releaseNode(atomicOperation, rootNode.endWrite());
        }

        opened = true;
        endSuccessfulAtomicOperation();
      } catch (Exception e) {
        throw endFailedAtomicOperation(e);
      }
    } finally {
      end(statistic);
    }
  }

  public void open(OBinarySerializer<K> keySerializer, OType[] keyTypes, int keySize, boolean nullKeyAllowed,
      OBinarySerializer<V> valueSerializer) {

    assert !opened;

    final OSessionStoragePerformanceStatistic statistic = start();
    try {
      this.keySerializer = keySerializer;
      this.keyProvider = selectKeyProvider(keySerializer, keyTypes);
      this.keyTypes = keyTypes;
      this.keySize = keySize;
      this.nullKeyAllowed = nullKeyAllowed;
      this.valueProvider = selectValueProvider(valueSerializer);

      fileId = openFile(atomicOperation(), getFullName());
    } catch (IOException e) {
      throw error("Exception during opening of SebTree " + getName(), e);
    } finally {
      end(statistic);
    }
  }

  public void close() {
    assert opened; // TODO
  }

  public void reset() {
    assert opened;

    final OSessionStoragePerformanceStatistic statistic = start();
    try {
      final OAtomicOperation atomicOperation = startAtomicOperation("reset", true);
      try {
        truncateFile(atomicOperation, fileId);

        if (nullKeyAllowed) {
          final OSebTreeNode<K, V> nullNode = createNode(atomicOperation).beginCreate();
          try {
            nullNode.create(true);
          } finally {
            releaseNode(atomicOperation, nullNode.endWrite());
          }
        }

        final OSebTreeNode<K, V> rootNode = createNode(atomicOperation).beginCreate();
        try {
          rootNode.create(true);
        } finally {
          releaseNode(atomicOperation, rootNode.endWrite());
        }

        endSuccessfulAtomicOperation();
      } catch (Exception e) {
        throw endFailedAtomicOperation(e);
      }
    } finally {
      end(statistic);
    }
  }

  public void delete() {
    // TODO
  }

  @Override
  public long size() {
    final OSessionStoragePerformanceStatistic statistic = start();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        return size(atomicOperation());
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      end(statistic);
    }
  }

  @Override
  public boolean contains(K key) {
    final OSessionStoragePerformanceStatistic statistic = start();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        return contains(atomicOperation(), internalKey(key));
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      end(statistic);
    }
  }

  @Override
  public V get(K key) {
    final OSessionStoragePerformanceStatistic statistic = start();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        return get(atomicOperation(), internalKey(key));
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      end(statistic);
    }
  }

  @Override
  public V get(K key, OModifiableBoolean found) {
    final OSessionStoragePerformanceStatistic statistic = start();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        return get(atomicOperation(), internalKey(key), found);
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      end(statistic);
    }
  }

  @Override
  public boolean put(K key, V value) {
    final OSessionStoragePerformanceStatistic statistic = start();
    try {
      startAtomicOperation("put", true);
      try {
        final boolean result = putValue(atomicOperation(), internalKey(key), value);
        endSuccessfulAtomicOperation();
        return result;
      } catch (Exception e) {
        throw endFailedAtomicOperation(e);
      }
    } finally {
      end(statistic);
    }
  }

  @Override
  public boolean remove(K key) {
    final OSessionStoragePerformanceStatistic statistic = start();
    try {
      startAtomicOperation("remove", true);
      try {
        final boolean result = remove(atomicOperation(), internalKey(key));
        endSuccessfulAtomicOperation();
        return result;
      } catch (Exception e) {
        throw endFailedAtomicOperation(e);
      }
    } finally {
      end(statistic);
    }
  }

  @Override
  public K beginningKey() {
    return null; // TODO
  }

  @Override
  public K endKey() {
    return null; // TODO
  }

  @Override
  public OKeyValueCursor<K, V> range(K beginningKey, K endKey, OCursor.Beginning beginning, OCursor.End end,
      OCursor.Direction direction) {
    final OSessionStoragePerformanceStatistic statistic = start();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        return new Cursor<>(atomicOperation(), this, true, true, beginningKey, endKey, beginning, end, direction);
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      end(statistic);
    }
  }

  @Override
  public OKeyCursor<K> keyRange(K beginningKey, K endKey, OCursor.Beginning beginning, OCursor.End end,
      OCursor.Direction direction) {
    final OSessionStoragePerformanceStatistic statistic = start();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        return new Cursor<>(atomicOperation(), this, true, false, beginningKey, endKey, beginning, end, direction);
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      end(statistic);
    }
  }

  @Override
  public OValueCursor<V> valueRange(K beginningKey, K endKey, OCursor.Beginning beginning, OCursor.End end,
      OCursor.Direction direction) {
    final OSessionStoragePerformanceStatistic statistic = start();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        return new Cursor<>(atomicOperation(), this, false, true, beginningKey, endKey, beginning, end, direction);
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      end(statistic);
    }
  }

  /* internal */ void dump() throws IOException {
    final OSebTreeNode<K, V> root = getRootNode(null).beginRead();
    try {
      System.out.println("Size: " + root.getTreeSize());
      dump(root, 0);
    } finally {
      releaseNode(null, root.endRead());
    }
  }

  private OEncoder.Provider<K> selectKeyProvider(OBinarySerializer<K> keySerializer, OType[] keyTypes) {
    return OEncoder.runtime().getProviderForKeySerializer(keySerializer, keyTypes, OEncoder.Size.PreferFixed);
  }

  private OEncoder.Provider<V> selectValueProvider(OBinarySerializer<V> valueSerializer) {
    return OEncoder.runtime().getProviderForValueSerializer(valueSerializer, OEncoder.Size.PreferVariable);
  }

  private OSessionStoragePerformanceStatistic start() {
    final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();
    if (statistic != null)
      statistic.startComponentOperation(getFullName(), OSessionStoragePerformanceStatistic.ComponentType.INDEX);
    return statistic;
  }

  private void end(OSessionStoragePerformanceStatistic statistic) {
    if (statistic != null)
      statistic.completeComponentOperation();
  }

  private OSebTreeNode<K, V> createNode(OAtomicOperation atomicOperation) throws IOException {
    final OCacheEntry page = addPage(atomicOperation, fileId);
    return new OSebTreeNode<>(page, keyProvider, valueProvider);
  }

  private Creation<K, V> createNode(OAtomicOperation atomicOperation, List<OSebTreeNode<K, V>> path, int rank,
      OSebTreeNode<K, V> node, K key) throws IOException {

    final OSebTreeNode<K, V> parent = path.get(path.size() - rank - 2).beginWrite();
    try {
      final int searchIndex = parent.indexOf(key);
      final OSebTreeNode.Marker marker = parent.nearestMarker(searchIndex);

      if (marker.blockPagesUsed < BLOCK_SIZE) {
        final boolean repositionMarker = updateMarkersOnCreate(atomicOperation, parent, marker, searchIndex);

        return new Creation<>(node, getNode(atomicOperation, marker.blockIndex + marker.blockPagesUsed),
            repositionMarker ? marker.blockIndex : 0, repositionMarker ? marker.blockPagesUsed + 1 : 0);
      }

      return splitBlock(atomicOperation, path, rank, node, parent, marker, searchIndex);
    } finally {
      parent.endWrite();
    }
  }

  private boolean updateMarkersOnCreate(OAtomicOperation atomicOperation, OSebTreeNode<K, V> parent, OSebTreeNode.Marker marker,
      int keyIndex) throws IOException {

    final boolean repositionMarker = OSebTreeNode.isPreceding(keyIndex, marker.index);
    if (repositionMarker)
      parent.updateMarker(marker.index, 0, 0); // TODO: seem like this never happens
    else
      parent.updateMarker(marker.index, marker.blockPagesUsed + 1);

    if ((parent.isContinuedFrom() && marker.index == parent.leftMostMarkerIndex())) {
      long siblingPointer = parent.getLeftSibling();
      while (true) {
        final OSebTreeNode<K, V> siblingParent = getNode(atomicOperation, siblingPointer).beginWrite();
        try {
          final int markerIndex = siblingParent.rightMostMarkerIndex();
          siblingParent.updateMarker(markerIndex, marker.blockPagesUsed + 1);
          if (!(siblingParent.isContinuedFrom() && markerIndex == siblingParent.leftMostMarkerIndex()))
            break;

          siblingPointer = siblingParent.getLeftSibling();
        } finally {
          releaseNode(atomicOperation, siblingParent.endWrite());
        }
      }
    }

    if ((parent.isContinuedTo() && marker.index == parent.rightMostMarkerIndex())) {
      long siblingPointer = parent.getRightSibling();
      while (true) {
        final OSebTreeNode<K, V> siblingParent = getNode(atomicOperation, siblingPointer).beginWrite();
        try {
          final int markerIndex = siblingParent.leftMostMarkerIndex();
          siblingParent.updateMarker(markerIndex, marker.blockPagesUsed + 1);
          if (!(siblingParent.isContinuedTo() && markerIndex == siblingParent.rightMostMarkerIndex()))
            break;

          siblingPointer = siblingParent.getRightSibling();
        } finally {
          releaseNode(atomicOperation, siblingParent.endWrite());
        }
      }
    }

    return repositionMarker;
  }

  private Creation<K, V> splitBlock(OAtomicOperation atomicOperation, List<OSebTreeNode<K, V>> path, int rank,
      OSebTreeNode<K, V> node, OSebTreeNode<K, V> parent, OSebTreeNode.Marker marker, int keyIndex) throws IOException {

    final Block block = collectBlockInformation(atomicOperation, parent, marker, keyIndex);
    final long[] oldBlockPointers = block.pointers;
    final long[] newBlockPointers = new long[BLOCK_SIZE];
    final long newBlock = allocateBlock(atomicOperation);

    // move the right part to the new block

    OSebTreeNode<K, V> nodeReplacement = node;

    for (int i = 0; i < BLOCK_HALF; ++i) {
      final long oldPageIndex = oldBlockPointers[BLOCK_HALF + i];
      final long newPageIndex = newBlockPointers[BLOCK_HALF + i] = newBlock + i;

      final OSebTreeNode<K, V> oldNode =
          oldPageIndex == node.getPointer() ? node : getNode(atomicOperation, oldPageIndex).beginCreate();
      try {
        final OSebTreeNode<K, V> newNode = getNode(atomicOperation, newPageIndex).beginCreate();
        try {
          newNode.cloneFrom(oldNode);
        } finally {
          if (node == oldNode) {
            nodeReplacement = newNode;
            path.set(path.size() - rank - 1, nodeReplacement);
            nodeReplacement.endWrite().beginWrite(); // reinitialize the node with a new data
          } else
            releaseNode(atomicOperation, newNode.endWrite());
        }
      } finally {
        if (oldNode != node || nodeReplacement != node)
          releaseNode(atomicOperation, oldNode.endWrite());
      }
    }

    // "collapse-left"

    final boolean[] usedPagesMap = new boolean[BLOCK_HALF];
    for (int i = 0; i < BLOCK_HALF; ++i) {
      final int index = (int) (oldBlockPointers[i] - marker.blockIndex);
      if (index < BLOCK_HALF) {
        usedPagesMap[index] = true;
        newBlockPointers[i] = oldBlockPointers[i];
      }
    }

    int lastFree = 0;
    for (int i = 0; i < BLOCK_HALF; ++i) {
      if (newBlockPointers[i] == 0) {
        final long oldPageIndex = oldBlockPointers[i];

        for (int j = lastFree; ; ++j)
          if (!usedPagesMap[j]) {
            final long newPageIndex = newBlockPointers[i] = marker.blockIndex + j;

            final OSebTreeNode<K, V> oldNode =
                oldPageIndex == node.getPointer() ? node : getNode(atomicOperation, oldPageIndex).beginCreate();
            try {
              final OSebTreeNode<K, V> newNode = getNode(atomicOperation, newPageIndex).beginCreate();
              try {
                newNode.cloneFrom(oldNode);
              } finally {
                if (node == oldNode) {
                  nodeReplacement = newNode;
                  path.set(path.size() - rank - 1, nodeReplacement);
                  nodeReplacement.endWrite().beginWrite(); // reinitialize the node with a new data
                } else
                  releaseNode(atomicOperation, newNode.endWrite());
              }
            } finally {
              if (oldNode != node || nodeReplacement != node)
                releaseNode(atomicOperation, oldNode.endWrite());
            }

            lastFree = j + 1;
            break;
          }
      }
    }

    relinkNodes(atomicOperation, oldBlockPointers, newBlockPointers, nodeReplacement);

    final boolean repositionMarker = updateMarkersOnSplit(atomicOperation, newBlockPointers, oldBlockPointers, marker.index,
        block.targetNodeIndex, block.parents, parent, block.markerIndexes, keyIndex, newBlock);

    final boolean targetInNewBlock = block.targetNodeIndex >= BLOCK_HALF;
    final OSebTreeNode<K, V> newNode = getNode(atomicOperation,
        targetInNewBlock ? newBlock + BLOCK_HALF : marker.blockIndex + BLOCK_HALF);
    if (repositionMarker)
      return new Creation<>(nodeReplacement, newNode, targetInNewBlock ? newBlock : marker.blockIndex, BLOCK_HALF + 1);
    else
      return new Creation<>(nodeReplacement, newNode, 0, 0);
  }

  private Block collectBlockInformation(OAtomicOperation atomicOperation, OSebTreeNode<K, V> node, OSebTreeNode.Marker marker,
      int keyIndex) throws IOException {

    final List<Long> pointers = new ArrayList<>(BLOCK_SIZE /* exact size */);
    final List<Integer> markerIndexes = new ArrayList<>(BLOCK_SIZE /* just a guess */);
    final List<Long> parents = new ArrayList<>(BLOCK_SIZE /* just a guess */);

    final long targetPointer = node.pointerAt(keyIndex);
    pointers.add(node.pointerAt(marker.index));
    markerIndexes.add(marker.index);
    parents.add(node.getPointer());
    for (int i = marker.index + 1; i < node.getSize(); ++i) {
      if (node.markerBlockIndexAt(i) != 0)
        break;
      pointers.add(node.pointerAt(i));
    }

    if (node.isContinuedFrom() && marker.index == node.leftMostMarkerIndex()) {
      long siblingPointer = node.getLeftSibling();
      while (true) {
        final OSebTreeNode<K, V> sibling = getNode(atomicOperation, siblingPointer).beginRead();
        try {
          final int markerIndex = sibling.rightMostMarkerIndex();

          pointers.add(0, sibling.pointerAt(markerIndex));
          markerIndexes.add(0, markerIndex);
          parents.add(0, sibling.getPointer());
          int added = 1;
          for (int j = markerIndex + 1; j < sibling.getSize(); ++j) {
            if (sibling.markerBlockIndexAt(j) != 0)
              break;
            pointers.add(added++, sibling.pointerAt(j));
          }

          if (!(sibling.isContinuedFrom() && markerIndex == sibling.leftMostMarkerIndex()))
            break;

          siblingPointer = sibling.getLeftSibling();
        } finally {
          releaseNode(atomicOperation, sibling.endRead());
        }
      }
    }

    if (node.isContinuedTo() && marker.index == node.rightMostMarkerIndex()) {
      long siblingPointer = node.getRightSibling();
      while (true) {
        final OSebTreeNode<K, V> sibling = getNode(atomicOperation, siblingPointer).beginRead();
        try {
          final int markerIndex = sibling.leftMostMarkerIndex();

          pointers.add(sibling.pointerAt(markerIndex));
          markerIndexes.add(markerIndex);
          parents.add(sibling.getPointer());
          for (int j = markerIndex + 1; j < sibling.getSize(); ++j) {
            if (sibling.markerBlockIndexAt(j) != 0)
              break;
            pointers.add(sibling.pointerAt(j));
          }

          if (!(sibling.isContinuedTo() && markerIndex == sibling.rightMostMarkerIndex()))
            break;

          siblingPointer = sibling.getRightSibling();
        } finally {
          releaseNode(atomicOperation, sibling.endRead());
        }
      }
    }

    assert pointers.size() == BLOCK_SIZE;
    assert parents.size() == markerIndexes.size();
    return new Block(pointers, parents, markerIndexes, targetPointer);
  }

  private boolean updateMarkersOnSplit(OAtomicOperation atomicOperation, long[] newPointers, long[] oldPointers,
      int targetMarkerIndex, int targetNodeIndex, long[] parents, OSebTreeNode<K, V> targetParent, int[] markerIndexes,
      int keyIndex, long newBlock) throws IOException {

    final boolean repositionMarker = OSebTreeNode.isPreceding(keyIndex, targetMarkerIndex);
    final boolean targetInNewBlock = targetNodeIndex >= BLOCK_HALF;

    int pointer = 0;
    for (int i = 0; i < parents.length; ++i) {
      final int markerIndex = markerIndexes[i];
      final long parentPointer = parents[i];
      final boolean target = parentPointer == targetParent.getPointer();

      final OSebTreeNode<K, V> node = target ? targetParent : getNode(atomicOperation, parentPointer).beginWrite();
      try {
        if (target && repositionMarker)
          node.updateMarker(markerIndex, 0, 0); // TODO: seem like this never happens
        else if (pointer >= BLOCK_HALF)
          node.updateMarker(markerIndex, newBlock, targetInNewBlock ? BLOCK_HALF + 1 : BLOCK_HALF);
        else
          node.updateMarker(markerIndex, !targetInNewBlock ? BLOCK_HALF + 1 : BLOCK_HALF);

        int j = markerIndex;
        do {
          if (pointer == BLOCK_HALF) { // insert new middle marker if needed
            if (j != markerIndex) // it's needed
              node.updateMarker(j, newBlock, targetInNewBlock ? BLOCK_HALF + 1 : BLOCK_HALF);
            else if (j == -1) // first marker in the node, the marker itself is already updated
              node.setContinuedFrom(false);
          }

          if (oldPointers[pointer] != newPointers[pointer])
            node.updatePointer(j, newPointers[pointer]);

          ++j;
          ++pointer;
        } while (j < node.getSize() && node.markerBlockIndexAt(j) == 0);

        if (pointer == BLOCK_HALF) // next node will have new middle marker at the beginning
          node.setContinuedTo(false);

      } finally {
        if (!target)
          releaseNode(atomicOperation, node.endWrite());
      }
    }

    return repositionMarker;
  }

  private void relinkNodes(OAtomicOperation atomicOperation, long[] oldPointers, long[] newPointers, OSebTreeNode<K, V> targetNode)
      throws IOException {

    for (int i = 0; i < BLOCK_SIZE; ++i) {
      final long oldPointer = oldPointers[i];
      final long newPointer = newPointers[i];

      if ((i == 0 && oldPointer != newPointer) || (i > 0 && oldPointers[i - 1] != newPointers[i - 1]) || (i < BLOCK_SIZE - 1
          && oldPointers[i + 1] != newPointers[i + 1]) || (i == BLOCK_SIZE - 1 && oldPointer != newPointer)) {
        final OSebTreeNode<K, V> node =
            newPointer == targetNode.getPointer() ? targetNode : getNode(atomicOperation, newPointer).beginWrite();
        try {

          if (i == 0 && oldPointer != newPointer && node.getLeftSibling() != 0) {
            final OSebTreeNode<K, V> sibling = getNode(atomicOperation, node.getLeftSibling()).beginWrite();
            try {
              sibling.setRightSibling(newPointer);
            } finally {
              releaseNode(atomicOperation, sibling.endWrite());
            }
          }

          if (i > 0 && oldPointers[i - 1] != newPointers[i - 1])
            node.setLeftSibling(newPointers[i - 1]);

          if (i < BLOCK_SIZE - 1 && oldPointers[i + 1] != newPointers[i + 1])
            node.setRightSibling(newPointers[i + 1]);

          if (i == BLOCK_SIZE - 1 && oldPointer != newPointer && node.getRightSibling() != 0) {
            final OSebTreeNode<K, V> sibling = getNode(atomicOperation, node.getRightSibling()).beginWrite();
            try {
              sibling.setLeftSibling(newPointer);
            } finally {
              releaseNode(atomicOperation, sibling.endWrite());
            }
          }

        } finally {
          if (node != targetNode)
            releaseNode(atomicOperation, node.endWrite());
        }
      }
    }
  }

  private OSebTreeNode<K, V> getNode(OAtomicOperation atomicOperation, long pageIndex) throws IOException {
    final OCacheEntry page;

    //    if (!nullKeyAllowed && pageIndex > 0 || nullKeyAllowed && pageIndex > 1) {
    //      // preload to the block end
    //      final int preload = (int) (BLOCK_SIZE - (pageIndex - 1 - (nullKeyAllowed ? 1 : 0)) % BLOCK_SIZE);
    //      page = loadPage(atomicOperation, fileId, pageIndex, false, preload);
    //    } else
    page = loadPage(atomicOperation, fileId, pageIndex, false);

    return new OSebTreeNode<>(page, keyProvider, valueProvider);
  }

  private void releaseNode(OAtomicOperation atomicOperation, OSebTreeNode<K, V> node) {
    releasePage(atomicOperation, node.getPage());
  }

  private long getRootPageIndex() {
    return nullKeyAllowed ? 1 : 0;
  }

  private long getNullPageIndex() {
    assert nullKeyAllowed;
    return 0;
  }

  private OSebTreeNode<K, V> getRootNode(OAtomicOperation atomicOperation) throws IOException {
    return getNode(atomicOperation, getRootPageIndex());
  }

  private OSebTreeNode<K, V> getNullNode(OAtomicOperation atomicOperation) throws IOException {
    return getNode(atomicOperation, getNullPageIndex());
  }

  private OSebTreeNode<K, V> findLeaf(OAtomicOperation atomicOperation, K key) throws IOException {
    if (key == null)
      return getNullNode(atomicOperation);

    long nodePage = getRootPageIndex();
    while (true) {
      final OSebTreeNode<K, V> node = getNode(atomicOperation, nodePage).beginRead();

      final boolean leaf;
      try {
        leaf = node.isLeaf();
      } catch (Exception e) {
        releaseNode(atomicOperation, node.endRead());
        throw e;
      }

      if (leaf)
        return node.endRead();

      try {
        nodePage = node.pointerAt(node.indexOf(key));
      } finally {
        releaseNode(atomicOperation, node.endRead());
      }
    }
  }

  private OSebTreeNode<K, V> findLeafWithPath(OAtomicOperation atomicOperation, K key, List<OSebTreeNode<K, V>> path)
      throws IOException {
    if (key == null)
      return getNullNode(atomicOperation);

    long nodePage = getRootPageIndex();
    while (true) {
      final OSebTreeNode<K, V> node = getNode(atomicOperation, nodePage).beginRead();
      path.add(node);

      final boolean leaf;
      try {
        leaf = node.isLeaf();
      } catch (Exception e) {
        node.endRead();
        throw e;
      }

      if (leaf)
        return node.endRead();

      try {
        nodePage = node.pointerAt(node.indexOf(key));
      } finally {
        node.endRead();
      }
    }
  }

  private int indexOfKeyInLeaf(OSebTreeNode<K, V> leaf, K key) {
    return key == null ? leaf.getSize() == 1 ? 0 : -1 : leaf.indexOf(key);
  }

  private long size(OAtomicOperation atomicOperation) {
    try {
      final OSebTreeNode<K, V> rootNode = getRootNode(atomicOperation).beginRead();
      try {
        return rootNode.getTreeSize();
      } finally {
        releaseNode(atomicOperation, rootNode.endRead());
      }
    } catch (IOException e) {
      throw error("Error during retrieving the size of SebTree " + getName(), e);
    }
  }

  private boolean contains(OAtomicOperation atomicOperation, K key) {
    final boolean nullKey = key == null;
    if (nullKey)
      checkNullKeyAllowed();

    try {
      final OSebTreeNode<K, V> leaf = findLeaf(atomicOperation, key);

      leaf.beginRead();
      try {
        return nullKey ? leaf.getSize() == 1 : leaf.indexOf(key) >= 0;
      } finally {
        releaseNode(atomicOperation, leaf.endRead());
      }
    } catch (IOException e) {
      throw error("Error during key lookup in SebTree " + getName(), e);
    }
  }

  private V get(OAtomicOperation atomicOperation, K key) {
    final boolean nullKey = key == null;
    if (nullKey)
      checkNullKeyAllowed();

    try {
      final OSebTreeNode<K, V> leaf = findLeaf(atomicOperation, key);

      leaf.beginRead();
      try {
        final int keyIndex = nullKey ? 0 : leaf.indexOf(key);
        final boolean keyFound = nullKey ? leaf.getSize() == 1 : keyIndex >= 0;
        return keyFound ? leaf.valueAt(keyIndex) : null;
      } finally {
        releaseNode(atomicOperation, leaf.endRead());
      }
    } catch (IOException e) {
      throw error("Error during key lookup in SebTree " + getName(), e);
    }
  }

  private V get(OAtomicOperation atomicOperation, K key, OModifiableBoolean found) {
    final boolean nullKey = key == null;
    if (nullKey)
      checkNullKeyAllowed();

    try {
      final OSebTreeNode<K, V> leaf = findLeaf(atomicOperation, key);

      leaf.beginRead();
      try {
        final int keyIndex = nullKey ? 0 : leaf.indexOf(key);
        final boolean keyFound = nullKey ? leaf.getSize() == 1 : keyIndex >= 0;
        found.setValue(keyFound);
        return keyFound ? leaf.valueAt(keyIndex) : null;
      } finally {
        releaseNode(atomicOperation, leaf.endRead());
      }
    } catch (IOException e) {
      throw error("Error during key lookup in SebTree " + getName(), e);
    }
  }

  private boolean putValue(OAtomicOperation atomicOperation, K key, V value) {
    if (key == null)
      checkNullKeyAllowed();

    try {
      final List<OSebTreeNode<K, V>> path = new ArrayList<>(16);

      OSebTreeNode<K, V> leaf = findLeafWithPath(atomicOperation, key, path).beginWrite();
      try {
        int keyIndex = indexOfKeyInLeaf(leaf, key);

        if (!tryPutValue(atomicOperation, leaf, keyIndex, key, value)) {
          final Splitting<K, V> splitting = split(atomicOperation, leaf, key, keyIndex, path, 0, 0);
          leaf = splitting.node;
          keyIndex = splitting.keyIndex;

          if (!tryPutValue(atomicOperation, leaf, keyIndex, key, value))
            throw new OSebTreeException("Split failed.", this);
        }

        return OSebTreeNode.isInsertionPoint(keyIndex);
      } finally {
        leaf.endWrite();

        for (OSebTreeNode<K, V> node : path)
          releaseNode(atomicOperation, node);
      }

    } catch (IOException e) {
      throw error("Error during put in SebTree " + getName(), e);
    }
  }

  private boolean tryPutValue(OAtomicOperation atomicOperation, OSebTreeNode<K, V> leaf, int keyIndex, K key, V value)
      throws IOException {
    final boolean keyExists = !OSebTreeNode.isInsertionPoint(keyIndex);

    final int keySize = leaf.getKeyEncoder().exactSize(key);
    final int valueSize = leaf.getValueEncoder().exactSize(value);
    final int fullEntrySize = leaf.fullEntrySize(keySize, valueSize);
    leaf.checkEntrySize(fullEntrySize, this);

    final int currentKeySize = keyExists ? leaf.keySizeAt(keyIndex) : 0;
    final int currentValueSize = keyExists ? leaf.valueSizeAt(keyIndex) : 0;
    final int currentFullEntrySize = keyExists ? leaf.fullEntrySize(currentKeySize, currentValueSize) : 0;
    final int sizeDelta = fullEntrySize - currentFullEntrySize;
    final boolean entryFits = leaf.deltaFits(sizeDelta);

    if (entryFits) {
      if (keyExists)
        leaf.updateValue(keyIndex, value, valueSize, currentValueSize);
      else {
        leaf.insertValue(keyIndex, key, keySize, value, valueSize);
        updateTreeSize(atomicOperation, +1);
      }
    }

    return entryFits;
  }

  private Splitting<K, V> split(OAtomicOperation atomicOperation, OSebTreeNode<K, V> node, K key, int keyIndex,
      List<OSebTreeNode<K, V>> path, int rank, long blockIndex) throws IOException {
    if (isRoot(node))
      return splitRoot(atomicOperation, node, key, keyIndex, path, blockIndex);
    else
      return splitNonRoot(atomicOperation, path, node, key, keyIndex, rank, blockIndex);
  }

  private Splitting<K, V> splitRoot(OAtomicOperation atomicOperation, OSebTreeNode<K, V> node, K key, int keyIndex,
      List<OSebTreeNode<K, V>> path, long blockIndex) throws IOException {
    if (node.isLeaf())
      return splitLeafRoot(atomicOperation, node, key, keyIndex, path);
    else
      return splitNonLeafRoot(atomicOperation, node, key, keyIndex, path, blockIndex);
  }

  private Splitting<K, V> splitNonRoot(OAtomicOperation atomicOperation, List<OSebTreeNode<K, V>> path, OSebTreeNode<K, V> node,
      K key, int keyIndex, int rank, long blockIndex) throws IOException {
    if (node.isLeaf())
      return splitLeafNonRoot(atomicOperation, path, node, key, keyIndex);
    else
      return splitNonLeafNonRoot(atomicOperation, path, node, key, keyIndex, rank, blockIndex);
  }

  private Splitting<K, V> splitLeafRoot(OAtomicOperation atomicOperation, OSebTreeNode<K, V> node, K key, int keyIndex,
      List<OSebTreeNode<K, V>> path) throws IOException {

    final long blockIndex = allocateBlock(atomicOperation);

    OSebTreeNode<K, V> newKeyNode = null;
    final int newKeyIndex;

    final OSebTreeNode<K, V> leftNode = getNode(atomicOperation, blockIndex).beginCreate();
    try {
      leftNode.create(true);
      final OSebTreeNode<K, V> rightNode = getNode(atomicOperation, blockIndex + 1).beginCreate();
      try {
        rightNode.create(true);

        final int entriesToMove = node.countEntriesToMoveUntilHalfFree();
        final int halfIndex = node.getSize() - entriesToMove;

        final K separator;
        final int rightNodeSize;
        if (OSebTreeNode.isInsertionPoint(keyIndex)) {
          final int index = OSebTreeNode.toIndex(keyIndex);
          if (index <= halfIndex) {
            newKeyNode = leftNode;
            newKeyIndex = keyIndex;
            separator = node.keyAt(halfIndex);
            rightNodeSize = entriesToMove;
          } else {
            newKeyNode = rightNode;
            newKeyIndex = OSebTreeNode.toInsertionPoint(index - halfIndex - 1);
            separator = newKeyIndex == -1 ? key : node.keyAt(halfIndex + 1);
            rightNodeSize = entriesToMove - 1;
          }
        } else {
          if (keyIndex <= halfIndex) {
            newKeyNode = leftNode;
            newKeyIndex = keyIndex;
            separator = keyIndex == halfIndex ? node.keyAt(halfIndex + 1) : node.keyAt(halfIndex);
            rightNodeSize = keyIndex == halfIndex ? entriesToMove - 1 : entriesToMove;
          } else {
            newKeyNode = rightNode;
            newKeyIndex = keyIndex - halfIndex - 1;
            separator = node.keyAt(halfIndex + 1);
            rightNodeSize = entriesToMove - 1;
          }
        }

        node.moveTailTo(rightNode, rightNodeSize);
        node.moveTailTo(leftNode, node.getSize());

        leftNode.setRightSibling(rightNode.getPointer());
        rightNode.setLeftSibling(leftNode.getPointer());

        node.convertToNonLeaf();
        node.setLeftPointer(leftNode.getPointer());

        final int separatorSize = node.getKeyEncoder().exactSize(separator);
        final int fullSeparatorSize = node.fullEntrySize(separatorSize, node.getPointerEncoder().maximumSize());
        node.checkEntrySize(fullSeparatorSize, this);
        node.insertPointer(0, separator, separatorSize, rightNode.getPointer(), 0, 0);

        node.updateMarker(-1, blockIndex, 2);
      } finally {
        if (newKeyNode != rightNode)
          releaseNode(atomicOperation, rightNode.endWrite());
      }
    } finally {
      if (newKeyNode != leftNode)
        releaseNode(atomicOperation, leftNode.endWrite());
    }

    node.endWrite();

    path.add(newKeyNode);
    return new Splitting<>(newKeyNode, newKeyIndex);
  }

  private Splitting<K, V> splitNonLeafRoot(OAtomicOperation atomicOperation, OSebTreeNode<K, V> node, K key, int keyIndex,
      List<OSebTreeNode<K, V>> path, long blockIndex) throws IOException {

    final long newBlockIndex = allocateBlock(atomicOperation);

    OSebTreeNode<K, V> newKeyNode = null;
    final int newKeyIndex;

    final OSebTreeNode<K, V> leftNode = getNode(atomicOperation, newBlockIndex).beginCreate();
    try {
      leftNode.create(false);
      final OSebTreeNode<K, V> rightNode = getNode(atomicOperation, newBlockIndex + 1).beginCreate();
      try {
        rightNode.create(false);

        final int entriesToMove = node.countEntriesToMoveUntilHalfFree();
        final int halfIndex = node.getSize() - entriesToMove;

        final K separator;
        final int rightNodeSize;
        assert OSebTreeNode.isInsertionPoint(keyIndex);
        final int index = OSebTreeNode.toIndex(keyIndex);
        if (index <= halfIndex) {
          newKeyNode = entriesToMove > 1 ? leftNode : rightNode;
          newKeyIndex = entriesToMove > 1 ? keyIndex : Integer.MIN_VALUE;
          separator = entriesToMove > 1 ? node.keyAt(halfIndex) : key;
          rightNodeSize = entriesToMove > 1 ? entriesToMove - 1 : entriesToMove;
        } else {
          newKeyNode = rightNode;
          newKeyIndex = OSebTreeNode.toInsertionPoint(index - halfIndex - 1);
          separator = node.keyAt(halfIndex);
          rightNodeSize = entriesToMove - 1;
        }

        final int separatorSize = node.getKeyEncoder().exactSize(separator);

        node.moveTailTo(rightNode, rightNodeSize);
        if (separator != key) {
          rightNode.setLeftPointer(node.pointerAt(node.getSize() - 1));

          OSebTreeNode.Marker marker = node.markerAt(node.getSize() - 1);
          if (marker.blockIndex == 0) {
            marker = node.markerAt(node.rightMostMarkerIndex());
            leftNode.setContinuedTo(true);
            rightNode.setContinuedFrom(true);
          }

          rightNode.updateMarker(-1, marker.blockIndex, marker.blockPagesUsed);

          node.delete(node.getSize() - 1, separatorSize, node.getPointerEncoder().maximumSize());
        } else if (blockIndex == 0) {
          leftNode.setContinuedTo(true);
          rightNode.setContinuedFrom(true);
          // marker itself will be set in the insertPointer that triggered that split
        }

        node.moveTailTo(leftNode, node.getSize());

        leftNode.setRightSibling(rightNode.getPointer());
        rightNode.setLeftSibling(leftNode.getPointer());

        leftNode.setLeftPointer(node.getLeftPointer());
        node.setLeftPointer(leftNode.getPointer());

        final OSebTreeNode.Marker marker = node.markerAt(-1);
        leftNode.updateMarker(-1, marker.blockIndex, marker.blockPagesUsed);

        final int fullSeparatorSize = node.fullEntrySize(separatorSize, node.getPointerEncoder().maximumSize());
        node.checkEntrySize(fullSeparatorSize, this);
        node.insertPointer(0, separator, separatorSize, rightNode.getPointer(), 0, 0);

        node.updateMarker(-1, newBlockIndex, 2);
      } finally {
        if (newKeyNode != rightNode)
          releaseNode(atomicOperation, rightNode.endWrite());
      }
    } finally {
      if (newKeyNode != leftNode)
        releaseNode(atomicOperation, leftNode.endWrite());
    }

    node.endWrite();

    path.add(1, newKeyNode);
    return new Splitting<>(newKeyNode, newKeyIndex);
  }

  private Splitting<K, V> splitLeafNonRoot(OAtomicOperation atomicOperation, List<OSebTreeNode<K, V>> path, OSebTreeNode<K, V> node,
      K key, int keyIndex) throws IOException {

    OSebTreeNode<K, V> newKeyNode = null;
    final int newKeyIndex;

    final Creation<K, V> creation = createNode(atomicOperation, path, 0, node, key);
    node = creation.oldNode;
    final OSebTreeNode<K, V> rightNode = creation.newNode.beginCreate();
    try {
      rightNode.create(true);

      final int entriesToMove = node.countEntriesToMoveUntilHalfFree();
      final int halfIndex = node.getSize() - entriesToMove;

      final K separator;
      final int rightNodeSize;
      if (OSebTreeNode.isInsertionPoint(keyIndex)) {
        final int index = OSebTreeNode.toIndex(keyIndex);
        if (index <= halfIndex) {
          newKeyNode = node;
          newKeyIndex = keyIndex;
          separator = node.keyAt(halfIndex);
          rightNodeSize = entriesToMove;
        } else {
          newKeyNode = rightNode;
          newKeyIndex = OSebTreeNode.toInsertionPoint(index - halfIndex - 1);
          separator = newKeyIndex == -1 ? key : node.keyAt(halfIndex + 1);
          rightNodeSize = entriesToMove - 1;
        }
      } else {
        if (keyIndex <= halfIndex) {
          newKeyNode = node;
          newKeyIndex = keyIndex;
          separator = keyIndex == halfIndex ? node.keyAt(halfIndex + 1) : node.keyAt(halfIndex);
          rightNodeSize = keyIndex == halfIndex ? entriesToMove - 1 : entriesToMove;
        } else {
          newKeyNode = rightNode;
          newKeyIndex = keyIndex - halfIndex - 1;
          separator = node.keyAt(halfIndex + 1);
          rightNodeSize = entriesToMove - 1;
        }
      }

      node.moveTailTo(rightNode, rightNodeSize);

      final long rightSiblingPointer = node.getRightSibling();
      if (rightSiblingPointer != 0) {
        final OSebTreeNode<K, V> oldRightSibling = getNode(atomicOperation, rightSiblingPointer).beginWrite();
        try {
          oldRightSibling.setLeftSibling(rightNode.getPointer());
        } finally {
          releaseNode(atomicOperation, oldRightSibling.endWrite());
        }
        rightNode.setRightSibling(rightSiblingPointer);
      }
      node.setRightSibling(rightNode.getPointer());
      rightNode.setLeftSibling(node.getPointer());

      insertPointer(atomicOperation, path.get(path.size() - 2), separator, rightNode.getPointer(), path, 1, creation.blockIndex,
          creation.blockPagesUsed);
    } finally {
      if (newKeyNode != rightNode)
        releaseNode(atomicOperation, rightNode.endWrite());
    }

    if (newKeyNode != node)
      releaseNode(atomicOperation, node.endWrite());

    path.set(path.size() - 1, newKeyNode);
    return new Splitting<>(newKeyNode, newKeyIndex);
  }

  private Splitting<K, V> splitNonLeafNonRoot(OAtomicOperation atomicOperation, List<OSebTreeNode<K, V>> path,
      OSebTreeNode<K, V> node, K key, int keyIndex, int rank, long blockIndex) throws IOException {

    OSebTreeNode<K, V> newKeyNode = null;
    final int newKeyIndex;

    final Creation<K, V> creation = createNode(atomicOperation, path, rank, node, key);
    node = creation.oldNode;
    final OSebTreeNode<K, V> rightNode = creation.newNode.beginCreate();
    try {
      rightNode.create(false);

      final int entriesToMove = node.countEntriesToMoveUntilHalfFree();
      final int halfIndex = node.getSize() - entriesToMove;

      final K separator;
      final int rightNodeSize;
      assert OSebTreeNode.isInsertionPoint(keyIndex);
      final int index = OSebTreeNode.toIndex(keyIndex);
      if (index <= halfIndex) {
        newKeyNode = entriesToMove > 1 ? node : rightNode;
        newKeyIndex = entriesToMove > 1 ? keyIndex : Integer.MIN_VALUE;
        separator = entriesToMove > 1 ? node.keyAt(halfIndex) : key;
        rightNodeSize = entriesToMove > 1 ? entriesToMove - 1 : entriesToMove;
      } else {
        newKeyNode = rightNode;
        newKeyIndex = OSebTreeNode.toInsertionPoint(index - halfIndex - 1);
        separator = node.keyAt(halfIndex);
        rightNodeSize = entriesToMove - 1;
      }

      node.moveTailTo(rightNode, rightNodeSize);
      if (separator != key) {
        rightNode.setLeftPointer(node.pointerAt(node.getSize() - 1));

        OSebTreeNode.Marker marker = node.markerAt(node.getSize() - 1);
        if (marker.blockIndex == 0) {
          marker = node.markerAt(node.rightMostMarkerIndex());
          rightNode.setContinuedTo(node.isContinuedTo());
          node.setContinuedTo(true);
          rightNode.setContinuedFrom(true);
        } else {
          rightNode.setContinuedTo(node.isContinuedTo());
          node.setContinuedTo(false);
        }
        rightNode.updateMarker(-1, marker.blockIndex, marker.blockPagesUsed);

        node.delete(node.getSize() - 1, node.getKeyEncoder().exactSize(separator), node.getPointerEncoder().maximumSize());
      } else if (blockIndex == 0) {
        node.setContinuedTo(true);
        rightNode.setContinuedFrom(true);
        // marker itself will be set in the insertPointer that triggered that split
      } else {
        node.setContinuedTo(false);
        rightNode.setContinuedFrom(false);
      }

      final long rightSiblingPointer = node.getRightSibling();
      if (rightSiblingPointer != 0) {
        final OSebTreeNode<K, V> oldRightSibling = getNode(atomicOperation, rightSiblingPointer).beginWrite();
        try {
          oldRightSibling.setLeftSibling(rightNode.getPointer());
        } finally {
          releaseNode(atomicOperation, oldRightSibling.endWrite());
        }
        rightNode.setRightSibling(rightSiblingPointer);
      }
      node.setRightSibling(rightNode.getPointer());
      rightNode.setLeftSibling(node.getPointer());

      insertPointer(atomicOperation, path.get(path.size() - rank - 2), separator, rightNode.getPointer(), path, rank + 1,
          creation.blockIndex, creation.blockPagesUsed);
    } finally {
      if (newKeyNode != rightNode)
        releaseNode(atomicOperation, rightNode.endWrite());
    }

    if (newKeyNode != node)
      releaseNode(atomicOperation, node.endWrite());

    path.set(path.size() - rank - 1, newKeyNode);
    return new Splitting<>(newKeyNode, newKeyIndex);
  }

  private void insertPointer(OAtomicOperation atomicOperation, OSebTreeNode<K, V> node, K key, long pointer,
      List<OSebTreeNode<K, V>> path, int rank, long blockIndex, int blockPagesUsed) throws IOException {

    node.beginWrite();
    try {
      int keyIndex = node.indexOf(key);

      if (!tryInsertPointer(node, keyIndex, key, pointer, blockIndex, blockPagesUsed)) {
        final Splitting<K, V> splitting = split(atomicOperation, node, key, keyIndex, path, rank, blockIndex);
        node = splitting.node;
        keyIndex = splitting.keyIndex;

        if (keyIndex == Integer.MIN_VALUE) {
          node.setLeftPointer(pointer);
          node.updateMarker(-1, blockIndex, blockPagesUsed);
        } else if (!tryInsertPointer(node, keyIndex, key, pointer, blockIndex, blockPagesUsed))
          throw new OSebTreeException("Split failed.", this);
      }
    } finally {
      node.endWrite();
    }
  }

  private boolean tryInsertPointer(OSebTreeNode<K, V> node, int keyIndex, K key, long pointer, long blockIndex, int blockPagesUsed)
      throws IOException {
    final int keySize = node.getKeyEncoder().exactSize(key);
    final int pointerSize = node.getPointerEncoder().maximumSize();
    final int fullEntrySize = node.fullEntrySize(keySize, pointerSize);
    node.checkEntrySize(fullEntrySize, this);
    final boolean entryFits = node.deltaFits(fullEntrySize);

    if (entryFits) {
      final int index = OSebTreeNode.isInsertionPoint(keyIndex) ? OSebTreeNode.toIndex(keyIndex) : keyIndex;
      node.insertPointer(index, key, keySize, pointer, blockIndex, blockPagesUsed);
    }

    return entryFits;
  }

  private boolean remove(OAtomicOperation atomicOperation, K key) {
    final boolean nullKey = key == null;
    if (nullKey)
      checkNullKeyAllowed();

    try {
      final OSebTreeNode<K, V> leaf = findLeaf(atomicOperation, key);

      leaf.beginWrite();
      try {
        final int keyIndex = nullKey ? 0 : leaf.indexOf(key);
        final boolean keyFound = nullKey ? leaf.getSize() == 1 : keyIndex >= 0;

        if (!keyFound)
          return false;

        final int keySize = leaf.keySizeAt(keyIndex);
        final int valueSize = leaf.valueSizeAt(keyIndex);

        leaf.delete(keyIndex, keySize, valueSize);
        updateTreeSize(atomicOperation, -1);

        // TODO: borrow?

        return true;
      } finally {
        releaseNode(atomicOperation, leaf.endWrite());
      }
    } catch (IOException e) {
      throw error("Error during key removal in SebTree " + getName(), e);
    }
  }

  private boolean isRoot(OSebTreeNode<K, V> node) {
    return node.getPointer() == getRootPageIndex();
  }

  private OAtomicOperation atomicOperation() {
    return atomicOperationsManager.getCurrentOperation();
  }

  private OAtomicOperation startAtomicOperation(String operation, boolean nonTx) {
    final OAtomicOperation atomicOperation;
    try {
      atomicOperation = startAtomicOperation(nonTx);
    } catch (IOException e) {
      throw error("Error during SebTree " + operation, e);
    }

    currentOperation = operation;
    return atomicOperation;
  }

  private void endSuccessfulAtomicOperation() {
    try {
      endAtomicOperation(false, null);
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error during commit of the SebTree atomic operation.", e);
    }
    currentOperation = null;
  }

  private RuntimeException endFailedAtomicOperation(Exception exception) {
    try {
      endAtomicOperation(true, exception);
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error during rollback of the SebTree atomic operation.", e);
    }

    final String operation = currentOperation;
    currentOperation = null;

    if (exception instanceof RuntimeException)
      return (RuntimeException) exception;
    else
      return error("Error during SebTree " + operation, exception);
  }

  private RuntimeException error(String message, Exception exception) {
    return OException.wrapException(new OSebTreeException(message, this), exception);
  }

  private void checkNullKeyAllowed() {
    if (!nullKeyAllowed)
      throw new OSebTreeException("Null keys are not allowed by SebTree " + getName(), this);
  }

  private K internalKey(K key) {
    return key == null ? null : keySerializer.preprocess(key, (Object[]) keyTypes);
  }

  private void updateTreeSize(OAtomicOperation atomicOperation, long delta) throws IOException {
    final OSebTreeNode<K, V> root = getRootNode(atomicOperation).beginWrite();
    try {
      root.setTreeSize(root.getTreeSize() + delta);
    } finally {
      releaseNode(atomicOperation, root.endWrite());
    }
  }

  private long allocateBlock(OAtomicOperation atomicOperation) throws IOException {
    final long firstPage = createNode(atomicOperation).beginCreate().createDummy().endWrite().getPointer();

    for (int i = 0; i < BLOCK_SIZE - 1; ++i)
      createNode(atomicOperation).beginCreate().createDummy().endWrite();

    return firstPage;
  }

  private void dump(OSebTreeNode<K, V> node, int level) throws IOException {
    node.dump(level);

    if (!node.isLeaf())
      for (int i = -1; i < node.getSize(); ++i) {
        final OSebTreeNode<K, V> child = getNode(null, node.pointerAt(i)).beginRead();
        try {
          dump(child, level + 1);
        } finally {
          releaseNode(null, child.endRead());
        }
      }
  }

  private static class Splitting<K, V> {
    public final OSebTreeNode<K, V> node;
    public final int                keyIndex;

    public Splitting(OSebTreeNode<K, V> node, int keyIndex) {
      this.node = node;
      this.keyIndex = keyIndex;
    }
  }

  private static class Creation<K, V> {
    public final OSebTreeNode<K, V> oldNode;
    public final OSebTreeNode<K, V> newNode;
    public final long               blockIndex;
    public final int                blockPagesUsed;

    private Creation(OSebTreeNode<K, V> oldNode, OSebTreeNode<K, V> newNode, long blockIndex, int blockPagesUsed) {
      this.oldNode = oldNode;
      this.newNode = newNode;
      this.blockIndex = blockIndex;
      this.blockPagesUsed = blockPagesUsed;
    }
  }

  private static class Block {
    public final int    targetNodeIndex;
    public final long[] pointers;
    public final long[] parents;
    public final int[]  markerIndexes;

    private Block(List<Long> pointers, List<Long> parents, List<Integer> markerIndexes, long targetPointer) {
      this.pointers = new long[pointers.size()];

      int targetNodeIndex = -1;
      for (int i = 0; i < pointers.size(); ++i) {
        final long pointer = pointers.get(i);
        this.pointers[i] = pointer;
        if (pointer == targetPointer)
          targetNodeIndex = i;
      }

      assert targetNodeIndex != -1;
      this.targetNodeIndex = targetNodeIndex;

      this.parents = new long[parents.size()];
      for (int i = 0; i < parents.size(); ++i)
        this.parents[i] = parents.get(i);

      this.markerIndexes = new int[markerIndexes.size()];
      for (int i = 0; i < markerIndexes.size(); ++i)
        this.markerIndexes[i] = markerIndexes.get(i);
    }
  }

  private static class Cursor<K, V> implements OKeyValueCursor<K, V> {

    private final OSebTree<K, V> tree;
    private final boolean        fetchKeys;
    private final boolean        fetchValues;
    private final K              beginningKey;
    private final K              endKey;
    private final Beginning      beginning;
    private final End            end;
    private final Direction      direction;

    private long leafPointer;
    private int  index;

    private long endLeafPointer;
    private int  endIndex;

    public Cursor(OAtomicOperation atomicOperation, OSebTree<K, V> tree, boolean fetchKeys, boolean fetchValues, K beginningKey,
        K endKey, OCursor.Beginning beginning, OCursor.End end, OCursor.Direction direction) {

      this.tree = tree;
      this.fetchKeys = fetchKeys;
      this.fetchValues = fetchValues;
      this.beginningKey = beginningKey;
      this.endKey = endKey;
      this.beginning = beginning;
      this.end = end;
      this.direction = direction;

      try {
        final OSebTreeNode<K, V> leaf = tree.findLeaf(atomicOperation, beginningKey).beginRead();
        try {
          leafPointer = leaf.getPointer();
          index = leaf.indexOf(beginningKey);

          if (OSebTreeNode.isInsertionPoint(index))
            index = OSebTreeNode.toIndex(index);
          else {
            if (beginning == Beginning.Exclusive)
              index += direction == Direction.Forward ? +1 : -1;
          }
        } finally {
          tree.releaseNode(atomicOperation, leaf.endRead());
        }

        if (!fetchKeys && end != End.Open) {
          final OSebTreeNode<K, V> endLeaf = tree.findLeaf(atomicOperation, endKey).beginRead();
          try {
            endLeafPointer = endLeaf.getPointer();
            endIndex = endLeaf.indexOf(endKey);

            if (OSebTreeNode.isInsertionPoint(endIndex))
              endIndex = OSebTreeNode.toIndex(endIndex);
            else {
              if (end == End.Exclusive)
                endIndex += direction == Direction.Forward ? -1 : +1;
            }
          } finally {
            tree.releaseNode(atomicOperation, endLeaf.endRead());
          }
        } else
          endLeafPointer = -1;

      } catch (IOException e) {
        throw tree.error("Error while constructing cursor for SebTree " + tree.getName(), e);
      }
    }

    @Override
    public boolean next() {

      switch (direction) {
      case Forward:
        break;

      case Reverse:
        break;
      }

      return false;
    }

    @Override
    public K key() {
      return null;
    }

    @Override
    public V value() {
      return null;
    }

  }

}
