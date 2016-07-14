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
import com.orientechnologies.orient.core.index.lsmtree.encoders.OLongEncoder;

/**
 * @author Sergey Sitnikov
 */
public class OFixedLongEncoder_V0 implements OLongEncoder {

  @Override
  public int version() {
    return 0;
  }

  @Override
  public int minimumSize() {
    return 8;
  }

  @Override
  public int maximumSize() {
    return 8;
  }

  @Override
  public int exactSize(Long value) {
    return 8;
  }

  @Override
  public void encode(Long value, OEncoder.Stream stream) {
    encodeLong(value, stream);
  }

  @Override
  public int exactSizeInStream(OEncoder.Stream stream) {
    return 8;
  }

  @Override
  public Long decode(OEncoder.Stream stream) {
    return decodeLong(stream);
  }

  @Override
  public int exactSizeLong(long value) {
    return 8;
  }

  @Override
  public void encodeLong(long value, Stream stream) {
    stream.writeLong(value);
  }

  @Override
  public long decodeLong(Stream stream) {
    return stream.readLong();
  }

}
