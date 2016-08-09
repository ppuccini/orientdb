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
import com.orientechnologies.orient.core.index.sbtree.local.OSBTree;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import org.openjdk.jmh.annotations.*;

import java.util.Random;

@State(Scope.Thread)
public class SbTreeBenchmarks {

  private ODatabaseDocumentTx     db;
  private OSBTree<String, String> tree;
  private Random                  random;

  private long keyIndex = 0;
  private long inserted = 0;

  @Setup
  public void setup() {
    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = "./jmh-tests/target";

    db = new ODatabaseDocumentTx("plocal:" + buildDirectory + "/SbTreeBenchmarks");
    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }

    db.create();

    tree = new OSBTree<>("sb-tree", ".sb", true, ".null", (OAbstractPaginatedStorage) db.getStorage());
    tree.create(OStringSerializer.INSTANCE, OStringSerializer.INSTANCE, null, 1, false);

    random = new Random(57);
  }

  @TearDown
  public void tearDown() {
    tree.delete();
    db.drop();
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void basicThings() {
    tree.put("key", "value");
    tree.get("key");
    tree.put("key", "new value");
    tree.put("key2", "value2");
    tree.remove("key");
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void randomInsert() {
    if (++inserted % 100000 == 0)
      System.out.println("\nInserted: " + inserted);

    tree.put(randomString(64), randomString(128));
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void sequentialInsert() {
    tree.put(nextString(keyIndex++), randomString(128));
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
