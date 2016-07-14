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

package com.orientechnologies.orient.core.index.lsmtree.encoders;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.index.lsmtree.encoders.impl.OEncodersRuntime;
import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * @author Sergey Sitnikov
 */
public interface OEncoder<V> {

  int UNBOUND_SIZE = -1;

  static OEncoder.Runtime runtime() {
    return OEncodersRuntime.INSTANCE;
  }

  int version();

  int minimumSize();

  int maximumSize();

  int exactSize(V value);

  void encode(V value, OEncoder.Stream stream);

  int exactSizeInStream(OEncoder.Stream stream);

  V decode(OEncoder.Stream stream);

  default boolean isOfFixedSize() {
    return minimumSize() == maximumSize() && maximumSize() != UNBOUND_SIZE;
  }

  default boolean isOfBoundSize() {
    return maximumSize() != UNBOUND_SIZE;
  }

  interface Runtime {
    <T> OEncoder.Provider<T> getProvider(Class<? extends OEncoder<T>> encoderClass, OEncoder.Size size);

    <K> OEncoder.Provider<K> getProviderForKeySerializer(OBinarySerializer<K> keyEncoder, OType[] keyTypes, OEncoder.Size size);

    <V> OEncoder.Provider<V> getProviderForValueSerializer(OBinarySerializer<V> valueEncoder, OEncoder.Size size);
  }

  interface Provider<V> {
    <E extends OEncoder<V>> E getEncoder(int version);
  }

  enum Size {
    Auto,

    PreferFixed,

    PreferVariable
  }

  interface Stream {

    int getPosition();

    void setPosition(int position);

    byte read();

    void write(byte value);

    byte[] read(int length);

    void write(byte[] bytes);

    default int readInteger() {
      int value = read() & 0xFF;
      value |= (read() & 0xFF) << 8;
      value |= (read() & 0xFF) << 16;
      value |= (read() & 0xFF) << 24;
      return value;
    }

    default void writeInteger(int value) {
      write((byte) value);
      write((byte) (value >>> 8));
      write((byte) (value >>> 16));
      write((byte) (value >>> 24));
    }

    default long readLong() {
      long value = read() & 0xFFL;
      value |= (read() & 0xFFL) << 8;
      value |= (read() & 0xFFL) << 16;
      value |= (read() & 0xFFL) << 24;
      value |= (read() & 0xFFL) << 32;
      value |= (read() & 0xFFL) << 40;
      value |= (read() & 0xFFL) << 48;
      value |= (read() & 0xFFL) << 56;
      return value;
    }

    default void writeLong(long value) {
      write((byte) value);
      write((byte) (value >>> 8));
      write((byte) (value >>> 16));
      write((byte) (value >>> 24));
      write((byte) (value >>> 32));
      write((byte) (value >>> 40));
      write((byte) (value >>> 48));
      write((byte) (value >>> 56));
    }

    default void seek(int offset) {
      setPosition(getPosition() + offset);
    }

  }

}
