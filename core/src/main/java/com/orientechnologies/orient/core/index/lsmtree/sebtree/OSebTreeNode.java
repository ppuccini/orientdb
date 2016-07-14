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

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.index.lsmtree.encoders.OEncoder;
import com.orientechnologies.orient.core.index.lsmtree.encoders.OPageIndexEncoder;
import com.orientechnologies.orient.core.index.lsmtree.encoders.OPagePositionEncoder;
import com.orientechnologies.orient.core.index.lsmtree.encoders.impl.OEncoderDurablePage;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;

/**
 * @author Sergey Sitnikov
 */
public class OSebTreeNode<K, V> extends OEncoderDurablePage {

  private static final int FREE_DATA_POSITION_OFFSET = NEXT_FREE_POSITION;
  private static final int FLAGS_OFFSET              = FREE_DATA_POSITION_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int SIZE_OFFSET               = FLAGS_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int TREE_SIZE_OFFSET          = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int LEFT_POINTER_OFFSET       = TREE_SIZE_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int LEFT_MARKER_BLOCK_OFFSET  = LEFT_POINTER_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int LEFT_MARKER_USAGE_OFFSET  = LEFT_MARKER_BLOCK_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int LEFT_SIBLING_OFFSET       = LEFT_MARKER_USAGE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int RIGHT_SIBLING_OFFSET      = LEFT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int RECORDS_OFFSET            = RIGHT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;

  private static final int HALF_SIZE = (MAX_PAGE_SIZE_BYTES - RECORDS_OFFSET) / 2;
  /* internal */ static final int MAX_ENTRY_SIZE = HALF_SIZE;

  private static final int CLONE_BUFFER_SIZE = 4 * 1024;

  private static final byte LEAF_FLAG_MASK           = 0b0000_0001;
  private static final byte CONTINUED_FROM_FLAG_MASK = 0b0000_0010;
  private static final byte CONTINUED_TO_FLAG_MASK   = 0b0000_0100;
  private static final byte ENCODERS_VERSION_MASK    = 0b0111_1000;
  private static final byte ENCODERS_VERSION_SHIFT   = 3;
  private static final byte EXTENSION_FLAG_MASK      = (byte) 0b1000_0000;

  private static final int FREE_DATA_POSITION_FIELD = 1;
  private static final int FLAGS_FIELD              = 2;
  private static final int SIZE_FIELD               = 4;
  private static final int TREE_SIZE_FIELD          = 8;

  private final OEncoder.Provider<K> keyProvider;
  private final OEncoder.Provider<V> valueProvider;

  private OEncoder<K>          keyEncoder;
  private OEncoder<V>          valueEncoder;
  private OPagePositionEncoder positionEncoder;
  private OPageIndexEncoder    pointerEncoder;

  private boolean keysInlined;
  private boolean valuesInlined;
  private int     recordSize;
  private int     markerSize;

  private int loadedFields = 0;
  private int dirtyFields  = 0;

  private int  freeDataPosition;
  private byte flags;
  private int  size;
  private long treeSize;

  public static boolean isInsertionPoint(int searchIndex) {
    return searchIndex < 0;
  }

  public static int toIndex(int insertionPoint) {
    return -insertionPoint - 1;
  }

  public static int toInsertionPoint(int index) {
    return -(index + 1);
  }

  public static int toMinusOneBasedIndex(int searchIndex) {
    if (isInsertionPoint(searchIndex)) {
      final int index = toIndex(searchIndex);
      return index == 0 ? -1 : index - 1;
    } else
      return searchIndex;
  }

  public static boolean isPreceding(int leftIndex, int rightIndex) {
    return toMinusOneBasedIndex(rightIndex) - toMinusOneBasedIndex(leftIndex) == 1;
  }

  public OSebTreeNode(OCacheEntry page, OEncoder.Provider<K> keyProvider, OEncoder.Provider<V> valueProvider) {
    super(page);
    this.keyProvider = keyProvider;
    this.valueProvider = valueProvider;
  }

  public OSebTreeNode<K, V> beginRead() {
    cacheEntry.acquireSharedLock();

    flags = getByteValue(FLAGS_OFFSET);
    size = getIntValue(SIZE_OFFSET);

    initialize(false);
    return this;
  }

  public OSebTreeNode<K, V> endRead() {
    assert dirtyFields == 0;

    loadedFields = 0;

    cacheEntry.releaseSharedLock();
    return this;
  }

  public OSebTreeNode<K, V> beginWrite() {
    cacheEntry.acquireExclusiveLock();

    flags = getByteValue(FLAGS_OFFSET);
    size = getIntValue(SIZE_OFFSET);

    initialize(false);
    return this;
  }

  public OSebTreeNode<K, V> endWrite() {
    if (dirtyFields != 0) {
      if (dirty(FREE_DATA_POSITION_FIELD))
        setIntValue(FREE_DATA_POSITION_OFFSET, freeDataPosition);
      if (dirty(FLAGS_FIELD))
        setByteValue(FLAGS_OFFSET, flags);
      if (dirty(SIZE_FIELD))
        setIntValue(SIZE_OFFSET, size);
      if (dirty(TREE_SIZE_FIELD))
        setLongValue(TREE_SIZE_OFFSET, treeSize);
    }

    loadedFields = 0;
    dirtyFields = 0;

    cacheEntry.releaseExclusiveLock();
    return this;
  }

  public OSebTreeNode<K, V> beginCreate() {
    cacheEntry.acquireExclusiveLock();
    return this;
  }

  public void create(boolean leaf) {
    setFreeDataPosition(MAX_PAGE_SIZE_BYTES);
    setLeaf(leaf);
    setContinuedFrom(false);
    setContinuedTo(false);
    setEncodersVersion(OSebTree.ENCODERS_VERSION);
    setFlag(EXTENSION_FLAG_MASK, false);
    setSize(0);
    setTreeSize(0);
    setLeftSibling(0);
    setRightSibling(0);

    initialize(true);
  }

  public OSebTreeNode<K, V> createDummy() {
    setFreeDataPosition(MAX_PAGE_SIZE_BYTES);
    return this;
  }

  public long getPointer() {
    return pointer.getPageIndex();
  }

  public int indexOf(K key) {
    return binarySearch(key);
  }

  public long pointerAt(int keyIndex) {
    if (isInsertionPoint(keyIndex)) {
      final int index = toIndex(keyIndex);
      return index == 0 ? getLeftPointer() : getPointer(index - 1);
    } else
      return getPointer(keyIndex);
  }

  public V valueAt(int index) {
    return getValue(index);
  }

  public K keyAt(int index) {
    return getKey(index);
  }

  public Marker markerAt(int index) {
    navigateToMarker(index);
    return new Marker(index, pointerEncoder.decodeLong(this), positionEncoder.decodeInteger(this));
  }

  public long markerBlockIndexAt(int index) {
    navigateToMarker(index);
    return pointerEncoder.decodeLong(this);
  }

  public Marker nearestMarker(int searchIndex) {
    int index = toMinusOneBasedIndex(searchIndex);
    while (true) {
      navigateToMarker(index);

      final long blockIndex = pointerEncoder.decodeLong(this);
      if (blockIndex != 0)
        return new Marker(index, blockIndex, positionEncoder.decodeInteger(this));

      --index;
      assert index >= -1;
    }
  }

  public void updateMarker(int index, long blockIndex, int blockPagesUsed) {
    assert index >= -1 && index < getSize();

    navigateToMarker(index);
    pointerEncoder.encodeLong(blockIndex, this);
    positionEncoder.encodeInteger(blockPagesUsed, this);
  }

  public void updateMarker(int index, int blockPagesUsed) {
    navigateToMarker(index);
    seek(pointerEncoder.maximumSize());
    positionEncoder.encodeInteger(blockPagesUsed, this);
  }

  public void updatePointer(int index, long pointer) {
    if (index == -1)
      setLeftPointer(pointer);
    else {
      setPosition(recordValuePosition(index));
      pointerEncoder.encodeLong(pointer, this);
    }
  }

  public int leftMostMarkerIndex() {
    return -1;
  }

  public int rightMostMarkerIndex() {
    for (int i = getSize() - 1; i >= 0; --i) {
      navigateToMarker(i);
      final long blockIndex = pointerEncoder.decodeLong(this);
      if (blockIndex != 0)
        return i;
    }

    return leftMostMarkerIndex();
  }

  public int keySizeAt(int index) {
    return getKeySize(index);
  }

  public int valueSizeAt(int index) {
    return getValueSize(index);
  }

  public int fullEntrySize(int keySize, int valueSize) {
    return getEntrySize(keySize, valueSize);
  }

  public void checkEntrySize(int entrySize, OSebTree<K, V> tree) {
    if (entrySize > MAX_ENTRY_SIZE)
      throw new OSebTreeException("Too large entry size " + entrySize + ", maximum possible size is " + MAX_ENTRY_SIZE, tree);
  }

  public boolean deltaFits(int sizeDelta) {
    return sizeDelta <= getFreeBytes();
  }

  public void updateValue(int index, V value, int valueSize, int currentValueSize) {
    setValue(index, value, valueSize, currentValueSize);
  }

  public void insertValue(int index, K key, int keySize, V value, int valueSize) {
    addKey(toIndex(index), key, keySize, value, valueSize);
  }

  public void insertPointer(int index, K key, int keySize, long pointer, long blockIndex, int blockPagesUsed) {
    addKey(index, key, keySize, pointer, blockIndex, blockPagesUsed);
  }

  public void moveTailTo(OSebTreeNode<K, V> destination, int length) {
    if (length == 0)
      return;

    if (isLeaf())
      leafMoveTailTo(destination, length);
    else
      nonLeafMoveTailTo(destination, length);
  }

  public int countEntriesToMoveUntilHalfFree() {
    final int size = getSize();
    final boolean leaf = isLeaf();

    int entriesToMove = 0;
    int bytesFree = getFreeBytes();
    for (int i = size - 1; size >= 0; --i) {
      if (bytesFree >= HALF_SIZE)
        break;

      navigateToKey(i);
      final int keySize = keyEncoder.exactSizeInStream(this);

      final int valueSize;
      if (leaf) {
        navigateToValue(i);
        valueSize = valueEncoder.exactSizeInStream(this);
      } else
        valueSize = pointerEncoder.maximumSize();

      final int fullSize = fullEntrySize(keySize, valueSize);

      bytesFree += fullSize;
      ++entriesToMove;
    }

    return entriesToMove;
  }

  public void cloneFrom(OSebTreeNode<K, V> node) {
    this.setPosition(0);
    node.setPosition(0);

    for (int i = 0; i < MAX_PAGE_SIZE_BYTES / CLONE_BUFFER_SIZE; ++i)
      this.write(node.read(CLONE_BUFFER_SIZE));
  }

  public void convertToNonLeaf() {
    setFreeDataPosition(MAX_PAGE_SIZE_BYTES);
    setLeaf(false);
    setContinuedFrom(false);
    setContinuedTo(false);
    setEncodersVersion(OSebTree.ENCODERS_VERSION);
    setFlag(EXTENSION_FLAG_MASK, false);
    setSize(0);

    initialize(true);
  }

  public void delete(int index, int keySize, int valueSize) {
    removeKey(index, keySize, valueSize);
  }

  public int getFreeDataPosition() {
    if (absent(FREE_DATA_POSITION_FIELD)) {
      freeDataPosition = getIntValue(FREE_DATA_POSITION_OFFSET);
      loaded(FREE_DATA_POSITION_FIELD);
    }
    return freeDataPosition;
  }

  public void setFreeDataPosition(int value) {
    changed(FREE_DATA_POSITION_FIELD);
    freeDataPosition = value;
  }

  public int getSize() {
    return size;
  }

  public void setSize(int value) {
    changed(SIZE_FIELD);
    size = value;
  }

  public long getTreeSize() {
    if (absent(TREE_SIZE_FIELD)) {
      treeSize = getLongValue(TREE_SIZE_OFFSET);
      loaded(TREE_SIZE_FIELD);
    }
    return treeSize;
  }

  public void setTreeSize(long value) {
    changed(TREE_SIZE_FIELD);
    treeSize = value;
  }

  public byte getFlags() {
    return flags;
  }

  public void setFlags(byte value) {
    changed(FLAGS_FIELD);
    flags = value;
  }

  public void setFlag(byte mask, boolean value) {
    if (value)
      setFlags((byte) (getFlags() | mask));
    else
      setFlags((byte) (getFlags() & ~mask));
  }

  public boolean getFlag(byte mask) {
    return (getFlags() & mask) != 0;
  }

  public boolean isLeaf() {
    return getFlag(LEAF_FLAG_MASK);
  }

  public void setLeaf(boolean value) {
    setFlag(LEAF_FLAG_MASK, value);
  }

  public boolean isContinuedFrom() {
    return getFlag(CONTINUED_FROM_FLAG_MASK);
  }

  public void setContinuedFrom(boolean value) {
    setFlag(CONTINUED_FROM_FLAG_MASK, value);
  }

  public boolean isContinuedTo() {
    return getFlag(CONTINUED_TO_FLAG_MASK);
  }

  public void setContinuedTo(boolean value) {
    setFlag(CONTINUED_TO_FLAG_MASK, value);
  }

  public int getEncodersVersion() {
    return (getFlags() & ENCODERS_VERSION_MASK) >>> ENCODERS_VERSION_SHIFT;
  }

  public void setEncodersVersion(int value) {
    setFlags((byte) ((value << ENCODERS_VERSION_SHIFT & ENCODERS_VERSION_MASK) | (getFlags() & ~ENCODERS_VERSION_MASK)));
  }

  public long getLeftPointer() {
    assert !isLeaf();
    return getLongValue(LEFT_POINTER_OFFSET);
  }

  public void setLeftPointer(long pointer) {
    assert !isLeaf();
    setLongValue(LEFT_POINTER_OFFSET, pointer);
  }

  public long getLeftSibling() {
    return getLongValue(LEFT_SIBLING_OFFSET);
  }

  public void setLeftSibling(long pointer) {
    setLongValue(LEFT_SIBLING_OFFSET, pointer);
  }

  public long getRightSibling() {
    return getLongValue(RIGHT_SIBLING_OFFSET);
  }

  public void setRightSibling(long pointer) {
    setLongValue(RIGHT_SIBLING_OFFSET, pointer);
  }

  public OEncoder<K> getKeyEncoder() {
    return keyEncoder;
  }

  public OEncoder<V> getValueEncoder() {
    return valueEncoder;
  }

  public OPageIndexEncoder getPointerEncoder() {
    return pointerEncoder;
  }

  @Override
  public String toString() {
    return isLeaf() ? "Leaf " : "Int. " + getPointer();
  }

  /* internal */ OCacheEntry getPage() {
    return cacheEntry;
  }

  private int binarySearch(K key) {
    int low = 0;
    int high = getSize() - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      K midVal = getKey(mid);

      final int order = compare(key, midVal);
      if (order > 0)
        low = mid + 1;
      else if (order < 0)
        high = mid - 1;
      else
        return mid; // key found
    }
    return -(low + 1);  // key not found.
  }

  private K getKey(int index) {
    navigateToKey(index);
    return keyEncoder.decode(this);
  }

  private void navigateToKey(int index) {
    setPosition(recordKeyPosition(index));

    if (!keysInlined)
      setPosition(positionEncoder.decodeInteger(this));
  }

  private V getValue(int index) {
    navigateToValue(index);
    return valueEncoder.decode(this);
  }

  private void navigateToValue(int index) {
    setPosition(recordValuePosition(index));

    if (!valuesInlined)
      setPosition(positionEncoder.decodeInteger(this));
  }

  private void navigateToMarker(int index) {
    setPosition(recordMarkerPosition(index));
  }

  private long getPointer(int index) {
    setPosition(recordValuePosition(index));
    return pointerEncoder.decodeLong(this);
  }

  private int getKeySize(int index) {
    if (keysInlined)
      return keyEncoder.maximumSize();
    else {
      navigateToKey(index);
      return keyEncoder.exactSizeInStream(this);
    }
  }

  private int getValueSize(int index) {
    if (valuesInlined)
      return valueEncoder.maximumSize();
    else {
      navigateToValue(index);
      return valueEncoder.exactSizeInStream(this);
    }
  }

  private int getEntrySize(int keySize, int valueSize) {
    int size = keySize + valueSize;

    if (!keysInlined)
      size += positionEncoder.maximumSize();
    if (isLeaf()) {
      if (!valuesInlined)
        size += positionEncoder.maximumSize();
    } else {
      size += markerSize;
    }

    return size;
  }

  private void setValue(int index, V value, int valueSize, int currentValueSize) {
    navigateToValue(index);

    if (!valuesInlined) {
      if (currentValueSize != valueSize) {
        int dataPosition = deleteData(getFreeDataPosition(), getPosition(), currentValueSize);
        dataPosition = allocateData(dataPosition, valueSize);

        setPosition(recordValuePosition(index));
        positionEncoder.encodeInteger(dataPosition, this);

        setFreeDataPosition(dataPosition);
        setPosition(dataPosition);
      }
    }

    valueEncoder.encode(value, this);
  }

  private void addKey(int index, K key, int keySize, V value, int valueSize) {
    allocateRecord(index);
    if (keysInlined)
      keyEncoder.encode(key, this);
    else {
      final int dataPosition = allocateData(getFreeDataPosition(), keySize);
      positionEncoder.encodeInteger(dataPosition, this);

      setPosition(dataPosition);
      keyEncoder.encode(key, this);

      setFreeDataPosition(dataPosition);
    }

    setPosition(recordValuePosition(index));
    if (valuesInlined)
      valueEncoder.encode(value, this);
    else {
      final int dataPosition = allocateData(getFreeDataPosition(), valueSize);
      positionEncoder.encodeInteger(dataPosition, this);

      setPosition(dataPosition);
      valueEncoder.encode(value, this);

      setFreeDataPosition(dataPosition);
    }

    setSize(getSize() + 1);
  }

  private void addKey(int index, K key, int keySize, long pointer, long blockIndex, int blockPagesUsed) {
    allocateRecord(index);

    if (keysInlined)
      keyEncoder.encode(key, this);
    else {
      final int dataPosition = allocateData(getFreeDataPosition(), keySize);
      positionEncoder.encodeInteger(dataPosition, this);

      setPosition(dataPosition);
      keyEncoder.encode(key, this);

      setFreeDataPosition(dataPosition);
    }

    setPosition(recordValuePosition(index));
    pointerEncoder.encodeLong(pointer, this);
    pointerEncoder.encodeLong(blockIndex, this);
    positionEncoder.encodeInteger(blockPagesUsed, this);

    setSize(getSize() + 1);
  }

  private void removeKey(int index, int keySize, int valueSize) {
    if (!keysInlined) {
      setPosition(recordKeyPosition(index));
      final int keyDataPosition = positionEncoder.decodeInteger(this);
      setFreeDataPosition(deleteData(getFreeDataPosition(), keyDataPosition, keySize));
    }

    if (isLeaf() && !valuesInlined) {
      setPosition(recordValuePosition(index));
      final int valueDataPosition = positionEncoder.decodeInteger(this);
      setFreeDataPosition(deleteData(getFreeDataPosition(), valueDataPosition, valueSize));
    }

    deleteRecord(index);

    setSize(getSize() - 1);
  }

  private int allocateData(int freePosition, int length) {
    return freePosition - length;
  }

  private int deleteData(int freePosition, int position, int length) {
    if (position > freePosition) { // not the last one from the end of the page
      moveData(freePosition, freePosition + length, position - freePosition);

      final boolean leaf = isLeaf();

      setPosition(RECORDS_OFFSET);
      final int size = getSize();
      for (int i = 0; i < size; ++i) {
        if (keysInlined)
          seek(keyEncoder.maximumSize());
        else {
          final int keyPosition = getPosition();
          final int keyDataPosition = positionEncoder.decodeInteger(this);
          if (keyDataPosition < position) {
            setPosition(keyPosition);
            positionEncoder.encodeInteger(keyDataPosition + length, this);
          }
        }

        if (!leaf)
          seek(pointerEncoder.maximumSize() + markerSize);
        else if (valuesInlined)
          seek(valueEncoder.maximumSize());
        else {
          final int valuePosition = getPosition();
          final int valueDataPosition = positionEncoder.decodeInteger(this);
          if (valueDataPosition < position) {
            setPosition(valuePosition);
            positionEncoder.encodeInteger(valueDataPosition + length, this);
          }
        }
      }
    }

    return freePosition + length;
  }

  private void allocateRecord(int index) {
    final int recordPosition = recordKeyPosition(index);

    if (index < getSize())
      moveData(recordPosition, recordPosition + recordSize, (getSize() - index) * recordSize);

    setPosition(recordPosition);
  }

  private void deleteRecord(int index) {
    final int recordPosition = recordKeyPosition(index);

    if (index < getSize() - 1)
      moveData(recordPosition + recordSize, recordPosition, (getSize() - index - 1) * recordSize);
  }

  @SuppressWarnings("unchecked")
  private void leafMoveTailTo(OSebTreeNode<K, V> destination, int length) {
    final int size = getSize();
    final int remaining = size - length;

    for (int i = 0; i < length; ++i) {
      final int index = remaining + i;

      navigateToKey(index);
      final int keyStart = getPosition();
      final K key = keyEncoder.decode(this);
      final int keySize = getPosition() - keyStart;

      navigateToValue(index);
      final int valueStart = getPosition();
      final V value = valueEncoder.decode(this);
      final int valueSize = getPosition() - valueStart;

      destination.addKey(i, key, keySize, value, valueSize);
    }

    final int[] keySizes = new int[remaining];
    final int[] valueSizes = new int[remaining];
    final K[] keys = (K[]) new Object[remaining];
    final V[] values = (V[]) new Object[remaining];

    for (int i = 0; i < remaining; ++i) {
      navigateToKey(i);
      final int keyStart = getPosition();
      final K key = keyEncoder.decode(this);
      keys[i] = key;
      keySizes[i] = getPosition() - keyStart;

      navigateToValue(i);
      final int valueStart = getPosition();
      final V value = valueEncoder.decode(this);
      values[i] = value;
      valueSizes[i] = getPosition() - valueStart;
    }

    clear();
    for (int i = 0; i < remaining; ++i)
      addKey(i, keys[i], keySizes[i], values[i], valueSizes[i]);
  }

  @SuppressWarnings("unchecked")
  private void nonLeafMoveTailTo(OSebTreeNode<K, V> destination, int length) {
    final int size = getSize();
    final int remaining = size - length;

    for (int i = 0; i < length; ++i) {
      final int index = remaining + i;

      navigateToKey(index);
      final int keyStart = getPosition();
      final K key = keyEncoder.decode(this);
      final int keySize = getPosition() - keyStart;

      setPosition(recordValuePosition(index));
      destination.addKey(i, key, keySize, pointerEncoder.decodeLong(this), pointerEncoder.decodeLong(this),
          positionEncoder.decodeInteger(this));
    }

    final int[] keySizes = new int[remaining];
    final K[] keys = (K[]) new Object[remaining];
    final V[] values = (V[]) new Object[remaining];
    final long[] pointers = new long[remaining];
    final long[] markerPointers = new long[remaining];
    final int[] markersUsages = new int[remaining];

    for (int i = 0; i < remaining; ++i) {
      navigateToKey(i);
      final int keyStart = getPosition();
      final K key = keyEncoder.decode(this);
      keys[i] = key;
      keySizes[i] = getPosition() - keyStart;

      setPosition(recordValuePosition(i));
      pointers[i] = pointerEncoder.decodeLong(this);
      markerPointers[i] = pointerEncoder.decodeLong(this);
      markersUsages[i] = positionEncoder.decodeInteger(this);
    }

    clear();
    for (int i = 0; i < remaining; ++i)
      addKey(i, keys[i], keySizes[i], pointers[i], markerPointers[i], markersUsages[i]);
  }

  private void clear() {
    setSize(0);
    setFreeDataPosition(MAX_PAGE_SIZE_BYTES);
  }

  private int getFreeBytes() {
    return getFreeDataPosition() - getSize() * recordSize - RECORDS_OFFSET;
  }

  private int compare(K a, K b) {
    return ODefaultComparator.INSTANCE.compare(a, b);
  }

  private int recordKeyPosition(int index) {
    return RECORDS_OFFSET + index * recordSize;
  }

  private int recordValuePosition(int index) {
    return recordKeyPosition(index) + (keysInlined ? keyEncoder.maximumSize() : positionEncoder.maximumSize());
  }

  private int recordMarkerPosition(int index) {
    return index == -1 ? LEFT_MARKER_BLOCK_OFFSET : recordValuePosition(index) + pointerEncoder.maximumSize();
  }

  private void initialize(boolean force) {
    if (keyEncoder != null && !force)
      return;

    keyEncoder = keyProvider.getEncoder(getEncodersVersion());
    valueEncoder = valueProvider.getEncoder(getEncodersVersion());

    positionEncoder = OEncoder.runtime().getProvider(OPagePositionEncoder.class, OEncoder.Size.PreferFixed)
        .getEncoder(getEncodersVersion());
    pointerEncoder = OEncoder.runtime().getProvider(OPageIndexEncoder.class, OEncoder.Size.PreferFixed)
        .getEncoder(getEncodersVersion());

    keysInlined = keyEncoder.isOfBoundSize() && keyEncoder.maximumSize() <= OSebTree.INLINE_KEYS_SIZE_THRESHOLD;
    valuesInlined = valueEncoder.isOfBoundSize() && valueEncoder.maximumSize() <= OSebTree.INLINE_VALUES_SIZE_THRESHOLD;

    recordSize = keysInlined ? keyEncoder.maximumSize() : positionEncoder.maximumSize();
    if (isLeaf())
      recordSize += valuesInlined ? valueEncoder.maximumSize() : positionEncoder.maximumSize();
    else {
      markerSize = pointerEncoder.maximumSize() + positionEncoder.maximumSize();
      recordSize += pointerEncoder.maximumSize() + markerSize;
    }
  }

  private boolean absent(int field) {
    return (loadedFields & field) == 0;
  }

  private void loaded(int field) {
    loadedFields |= field;
  }

  private boolean dirty(int field) {
    return (dirtyFields & field) != 0;
  }

  private void changed(int field) {
    dirtyFields |= field;
    loadedFields |= field;
  }

  @SuppressWarnings("unchecked")
  public void dump(int level) {
    for (int i = 0; i < level; ++i)
      System.out.print('\t');
    System.out.print(isLeaf() ? "Leaf " : "Int. ");
    System.out.print(getPointer() + ": ");

    if (isContinuedFrom())
      System.out.print("... ");

    if (getLeftSibling() != 0)
      System.out.print("<-" + getLeftSibling() + " ");

    for (int i = -1; i < getSize(); ++i) {
      if (isLeaf()) {
        if (i > -1) {
          K key = keyAt(i);
          if (key instanceof String)
            key = (K) ((String) key).substring(0, 3);
          V value = valueAt(i);
          if (value instanceof String)
            value = (V) ((String) value).substring(0, 3);
          System.out.print(key + " " + value + ", ");
        }
      } else {
        final Marker marker = markerAt(i);
        if (marker.blockIndex != 0)
          System.out.print("M(" + marker.blockIndex + ", " + marker.blockPagesUsed + "), ");

        final long pointer = pointerAt(i);
        if (i == -1)
          System.out.print("P(" + pointer + "), ");
        else {
          K key = keyAt(i);
          if (key instanceof String)
            key = (K) ((String) key).substring(0, 3);
          System.out.print(key + " P(" + pointer + "), ");
        }
      }
    }

    if (getRightSibling() != 0)
      System.out.print(getRightSibling() + "-> ");

    if (isContinuedTo())
      System.out.print("...");

    System.out.println();
  }

  public static class Marker {

    public final int  index;
    public final long blockIndex;
    public final int  blockPagesUsed;

    public Marker(int index, long blockIndex, int blockPagesUsed) {
      this.index = index;
      this.blockIndex = blockIndex;
      this.blockPagesUsed = blockPagesUsed;
    }

    @Override
    public String toString() {
      return Long.toString(blockIndex) + ":" + blockPagesUsed + " at " + index;
    }

  }

}
