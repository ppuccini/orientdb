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
import com.orientechnologies.orient.core.index.lsmtree.encoders.OStringEncoder;
import com.orientechnologies.orient.core.index.lsmtree.encoders.OUnsignedIntegerEncoder;

import java.nio.charset.Charset;

/**
 * @author Sergey Sitnikov
 */
public class OStringEncoder_V0 implements OStringEncoder {

  private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");

  private final OIntegerEncoder lengthEncoder;

  public OStringEncoder_V0(OEncoder.Runtime runtime) {
    lengthEncoder = runtime.getProvider(OUnsignedIntegerEncoder.class, Size.Auto).getEncoder(version());
  }

  @Override
  public int version() {
    return 0;
  }

  @Override
  public int minimumSize() {
    return lengthEncoder.minimumSize();
  }

  @Override
  public int maximumSize() {
    return OEncoder.UNBOUND_SIZE;
  }

  @Override
  public int exactSize(String value) {
    // TODO: optimize
    final byte[] utf8Bytes = value.getBytes(UTF8_CHARSET);
    return lengthEncoder.exactSizeInteger(utf8Bytes.length) + utf8Bytes.length;
  }

  @Override
  public void encode(String value, OEncoder.Stream stream) {
    // TODO: optimize?
    final byte[] utf8Bytes = value.getBytes();
    lengthEncoder.encodeInteger(utf8Bytes.length, stream);
    stream.write(utf8Bytes);
  }

  @Override
  public int exactSizeInStream(OEncoder.Stream stream) {
    final int start = stream.getPosition();
    final int length = lengthEncoder.decodeInteger(stream);
    return stream.getPosition() - start + length;
  }

  @Override
  public String decode(OEncoder.Stream stream) {
    final int length = lengthEncoder.decodeInteger(stream);
    return new String(stream.read(length), UTF8_CHARSET);
  }
}
