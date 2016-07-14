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

import com.orientechnologies.orient.core.index.lsmtree.encoders.OEncoder;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Sergey Sitnikov
 */
public class Base128IntegerTest {

  // @formatter:off
  final Map<Integer, byte[]> UNSIGNED_INTEGER_TEST_VECTORS = new LinkedHashMap<Integer,byte[]>() {{
    put(0,                  bytes(0b1000_0000));
    put(1,                  bytes(0b1000_0001));
    put(10,                 bytes(0b1000_1010));
    put(64,                 bytes(0b1100_0000));
    put(127,                bytes(0b1111_1111));
    put(128,                bytes(0b0000_0000, 0b1000_0001));
    put(150,                bytes(0b0001_0110, 0b1000_0001));
    put(300,                bytes(0b0010_1100, 0b1000_0010));
    put(11111,              bytes(0b0110_0111, 0b1101_0110));
    put(16383,              bytes(0b0111_1111, 0b1111_1111));
    put(16384,              bytes(0b0000_0000, 0b0000_0000, 0b1000_0001));
    put(Integer.MAX_VALUE,  bytes(0b0111_1111, 0b0111_1111, 0b0111_1111, 0b0111_1111, 0b1000_0111));
  }};
  // @formatter:on

  @Test
  public void testUnsignedInteger() {
    for (Map.Entry<Integer, byte[]> vector : UNSIGNED_INTEGER_TEST_VECTORS.entrySet()) {
      final int expectedValue = vector.getKey();
      final byte[] expectedBytes = vector.getValue();

      final int actualLength = OBase128IntegerUtils.sizeOfUnsigned(expectedValue);
      final EncoderArrayStream actualBytes = new EncoderArrayStream(actualLength);
      OBase128IntegerUtils.writeUnsigned(expectedValue, actualBytes);
      Assert.assertArrayEquals(Integer.toString(expectedValue), expectedBytes, actualBytes.array);

      actualBytes.setPosition(0);
      final int actualValue = OBase128IntegerUtils.readUnsignedInteger(actualBytes);
      Assert.assertEquals(expectedValue, actualValue);

      actualBytes.setPosition(0);
      Assert.assertEquals(Integer.toString(expectedValue), expectedBytes.length,
          OBase128IntegerUtils.lengthOfStoredInteger(actualBytes));
    }
  }

  private static byte[] bytes(int... bytes) {
    final byte[] result = new byte[bytes.length];
    for (int i = 0; i < bytes.length; ++i)
      result[i] = (byte) bytes[i];
    return result;
  }

  private static class EncoderArrayStream implements OEncoder.Stream {

    private final byte[] array;
    private int position = 0;

    public EncoderArrayStream(int size) {
      array = new byte[size];
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
      return array[position++];
    }

    @Override
    public void write(byte value) {
      array[position++] = value;
    }

    @Override
    public byte[] read(int length) {
      final byte[] bytes = new byte[length];
      System.arraycopy(array, position, bytes, 0, length);
      position += length;
      return bytes;
    }

    @Override
    public void write(byte[] bytes) {
      System.arraycopy(bytes, 0, array, position, bytes.length);
      position += bytes.length;
    }
  }
}
