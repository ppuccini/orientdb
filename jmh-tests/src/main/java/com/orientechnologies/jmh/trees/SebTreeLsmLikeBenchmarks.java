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

package com.orientechnologies.jmh.trees;

import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.lsmtree.sebtree.OSebTree;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import org.openjdk.jmh.annotations.*;

import java.util.Random;

@State(Scope.Thread)
public class SebTreeLsmLikeBenchmarks {

  private ODatabaseDocumentTx db;
  private OSebTree<String, String> tree = null;
  private Random random;

  private long keyIndex  = 0;
  private long fileIndex = 0;

  @Setup
  public void setup() {
    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = "./jmh-tests/target";

    db = new ODatabaseDocumentTx("plocal:" + buildDirectory + "/SebTreeLsmLikeBenchmarks");
    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }

    db.create();
    random = new Random(57);
  }

  @TearDown
  public void tearDown() {
    db.drop();
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void randomInsert() {
    ++keyIndex;
    getTree().put(randomString(64), randomString(128));
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void sequentialInsert() {
    getTree().put(nextString(keyIndex++), randomString(128));
  }

  private OSebTree<String, String> getTree() {
    if (tree == null || tree.isFull()) {
      System.out.println("\n" + ++fileIndex + " seb tree created");
      System.out.println(keyIndex + " keys inserted");
      tree = new OSebTree<>((OAbstractPaginatedStorage) db.getStorage(), "seb-tree" + fileIndex, ".seb", true);
      tree.create(OStringSerializer.INSTANCE, null, 1, false, OStringSerializer.INSTANCE);
    }

    return tree;
  }

  private String randomString(int maxLength) {
    final int length = random.nextInt(maxLength);
    final StringBuilder builder = new StringBuilder(length);
    for (int i = 0; i < length; ++i)
      builder.append((char) ('a' + random.nextInt('z' - 'a')));
    return builder.toString();
  }

  private String nextString(long keyIndex) {
    final StringBuilder builder = new StringBuilder();

    do {
      builder.append((char) ('a' + keyIndex % ('z' - 'a')));
      keyIndex /= 'z' - 'a';
    } while (keyIndex > 0);

    return builder.reverse().toString();
  }

}
