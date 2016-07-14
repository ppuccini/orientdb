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
import com.orientechnologies.orient.core.index.lsmtree.encoders.OIntegerEncoder;

/**
 * @author Sergey Sitnikov
 */
public class OVariableIntegerEncoder_V0 implements OIntegerEncoder {

  private static final int MIN_SIZE = OBase128IntegerUtils.sizeOfSigned(0);
  private static final int MAX_SIZE = Math
      .max(OBase128IntegerUtils.sizeOfSigned(Integer.MIN_VALUE), OBase128IntegerUtils.sizeOfSigned(Integer.MAX_VALUE));

  @Override
  public int version() {
    return 0;
  }

  @Override
  public int minimumSize() {
    return MIN_SIZE;
  }

  @Override
  public int maximumSize() {
    return MAX_SIZE;
  }

  @Override
  public int exactSize(Integer value) {
    return exactSizeInteger(value);
  }

  @Override
  public void encode(Integer value, OEncoder.Stream stream) {
    encodeInteger(value, stream);
  }

  @Override
  public int exactSizeInStream(OEncoder.Stream stream) {
    return OBase128IntegerUtils.lengthOfStoredInteger(stream);
  }

  @Override
  public Integer decode(OEncoder.Stream stream) {
    return decodeInteger(stream);
  }

  @Override
  public int exactSizeInteger(int value) {
    return OBase128IntegerUtils.sizeOfSigned(value);
  }

  @Override
  public void encodeInteger(int value, Stream stream) {
    OBase128IntegerUtils.writeSigned(value, stream);
  }

  @Override
  public int decodeInteger(Stream stream) {
    return OBase128IntegerUtils.readSignedInteger(stream);
  }

}
