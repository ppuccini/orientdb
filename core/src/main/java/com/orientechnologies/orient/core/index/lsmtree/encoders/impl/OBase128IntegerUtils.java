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

/**
 * <a href=https://en.wikipedia.org/wiki/Variable-length_quantity>Variable-length quantity<a/>
 *
 * @author Sergey Sitnikov
 */
public class OBase128IntegerUtils {
  private static final int MAX_LONG_LENGTH  = (64 + 6) / 7;
  private static final int MAX_INT_LENGTH   = (32 + 6) / 7;
  private static final int MAX_SHORT_LENGTH = (16 + 6) / 7;
  private static final int MAX_BYTE_LENGTH  = (8 + 6) / 7;

  private OBase128IntegerUtils() {
  }

  public static int sizeOfUnsigned(long value) {
    return value == 0 ? 1 : (63 - Long.numberOfLeadingZeros(value) + 7) / 7;
  }

  public static int sizeOfSigned(long value) {
    return sizeOfUnsigned(zigZagEncode(value));
  }

  public static void writeUnsigned(long value, OEncoder.Stream stream) {
    while (value > 127) {
      stream.write((byte) (value & 0b0111_1111));
      value >>>= 7;
    }
    stream.write((byte) (value | 0b1000_0000));
  }

  public static void writeSigned(long value, OEncoder.Stream stream) {
    writeUnsigned(zigZagEncode(value), stream);
  }

  public static int readSignedInteger(OEncoder.Stream stream) {
    return (int) readSigned(stream, MAX_INT_LENGTH);
  }

  public static int readUnsignedInteger(OEncoder.Stream stream) {
    return (int) readUnsigned(stream, MAX_INT_LENGTH);
  }

  public static int lengthOfStoredInteger(OEncoder.Stream stream) {
    return storedValueLength(stream, MAX_INT_LENGTH);
  }

  private static long readUnsigned(OEncoder.Stream stream, int maxLength) {
    long value = 0;

    int bits = 0;
    byte octet;
    while ((octet = stream.read()) >= 0) {
      value |= (octet << bits);
      bits += 7;
    }
    value |= (octet & 0b0111_1111) << bits;

    if (bits > maxLength * 8)
      throw new IllegalStateException(
          "Too long 7-bit encoded integer of length " + bits / 8 + ", length must be not longer than " + maxLength);

    return value;
  }

  private static long readSigned(OEncoder.Stream stream, int maxLength) {
    return zigZagDecode(readUnsigned(stream, maxLength));
  }

  private static int storedValueLength(OEncoder.Stream stream, int maxLength) {
    int length = 1;
    while (stream.read() >= 0)
      ++length;

    if (length > maxLength)
      throw new IllegalStateException(
          "Too long 7-bit encoded integer of length " + length + ", length must be not longer than " + maxLength);

    return length;
  }

  private static long zigZagEncode(long value) {
    return value >> 63 ^ value << 1;
  }

  private static long zigZagDecode(long value) {
    return value >>> 1 ^ -(value & 1);
  }
}
