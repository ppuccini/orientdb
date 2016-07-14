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

package com.orientechnologies.orient.core.index.lsmtree.encoders.impl;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.index.lsmtree.encoders.OEncoder;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;

import java.nio.ByteBuffer;

/**
 * @author Sergey Sitnikov
 */
public class OEncoderDurablePage extends ODurablePage implements OEncoder.Stream, OSerializerEncoderStream {
  private int position = 0;

  public OEncoderDurablePage(OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  @Override
  public int getPosition() {
    return position;
  }

  @Override
  public void setPosition(int position) {
    this.position = position;
  }

  @Override
  public byte read() {
    return getByteValue(position++);
  }

  @Override
  public void write(byte value) {
    setByteValue(position++, value);
  }

  @Override
  public byte[] read(int length) {
    final byte[] bytes = getBinaryValue(position, length);
    position += length;
    return bytes;
  }

  @Override
  public void write(byte[] bytes) {
    setBinaryValue(position, bytes);
    position += bytes.length;
  }

  @Override
  public int readInteger() {
    final int value = getIntValue(position);
    position += OIntegerSerializer.INT_SIZE;
    return value;
  }

  @Override
  public void writeInteger(int value) {
    setIntValue(position, value);
    position += OIntegerSerializer.INT_SIZE;
  }

  @Override
  public long readLong() {
    final long value = getLongValue(position);
    position += OLongSerializer.LONG_SIZE;
    return value;
  }

  @Override
  public void writeLong(long value) {
    setLongValue(position, value);
    position += OLongSerializer.LONG_SIZE;
  }

  @Override
  public ByteBuffer getReadByteBuffer() {
    return cacheEntry.getCachePointer().getSharedBuffer();
  }

  @Override
  public OWALChanges getWALChanges() {
    return changes;
  }
}
