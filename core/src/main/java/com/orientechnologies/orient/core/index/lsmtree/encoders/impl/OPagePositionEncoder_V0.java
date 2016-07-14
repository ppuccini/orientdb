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
import com.orientechnologies.orient.core.index.lsmtree.encoders.OPagePositionEncoder;

/**
 * @author Sergey Sitnikov
 */
public class OPagePositionEncoder_V0 implements OPagePositionEncoder {

  private final OIntegerEncoder actualEncoder;

  public OPagePositionEncoder_V0(OEncoder.Runtime runtime) {
    actualEncoder = runtime.getProvider(OIntegerEncoder.class, OEncoder.Size.Auto).getEncoder(version());
  }

  @Override
  public int version() {
    return 0;
  }

  @Override
  public int minimumSize() {
    return actualEncoder.minimumSize();
  }

  @Override
  public int maximumSize() {
    return actualEncoder.maximumSize();
  }

  @Override
  public int exactSize(Integer value) {
    return actualEncoder.exactSize(value);
  }

  @Override
  public void encode(Integer value, Stream stream) {
    actualEncoder.encode(value, stream);
  }

  @Override
  public int exactSizeInStream(Stream stream) {
    return actualEncoder.exactSizeInStream(stream);
  }

  @Override
  public Integer decode(Stream stream) {
    return actualEncoder.decode(stream);
  }

  @Override
  public int exactSizeInteger(int value) {
    return actualEncoder.exactSizeInteger(value);
  }

  @Override
  public void encodeInteger(int value, Stream stream) {
    actualEncoder.encodeInteger(value, stream);
  }

  @Override
  public int decodeInteger(Stream stream) {
    return actualEncoder.decodeInteger(stream);
  }

}
