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

package com.orientechnologies.orient.core.index.lsmtree.sebtree;

import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import org.junit.*;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

/**
 * @author Sergey Sitnikov
 */
public class StringSebTreeTest {

  private static final int RANDOMIZED_TESTS_ITERATIONS = 20000;

  private static final int LARGE = OSebTreeNode.MAX_ENTRY_SIZE / 2 - 50;
  private static final int SMALL = LARGE / 2;

  private static ODatabaseDocumentTx      db;
  private static OSebTree<String, String> tree;

  @BeforeClass
  public static void beforeClass() {
    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = ".";

    db = new ODatabaseDocumentTx("plocal:" + buildDirectory + "/StringSebTreeTest");
    System.out.println(db.getURL());
    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }

    db.create();

    tree = new OSebTree<>((OAbstractPaginatedStorage) db.getStorage(), "tree", ".seb", true);
    tree.create(OStringSerializer.INSTANCE, null, 1, false, OStringSerializer.INSTANCE);
  }

  @AfterClass
  public static void afterClass() {
    tree.delete();
    db.drop();
  }

  @After
  public void afterMethod() {
    tree.reset();
  }

  @Test
  public void testBasicOperations() {
    final OModifiableBoolean found = new OModifiableBoolean();

    Assert.assertEquals(0, tree.size());

    found.setValue(true);
    Assert.assertEquals(null, tree.get("nonexistent key", found));
    Assert.assertFalse(found.getValue());

    Assert.assertTrue(tree.put("key1", "value1"));
    Assert.assertEquals(1, tree.size());
    Assert.assertTrue(tree.contains("key1"));
    Assert.assertEquals("value1", tree.get("key1"));

    found.setValue(false);
    Assert.assertEquals("value1", tree.get("key1", found));
    Assert.assertTrue(found.getValue());

    Assert.assertFalse(tree.put("key1", "new value1"));
    Assert.assertTrue(tree.contains("key1"));
    Assert.assertEquals(1, tree.size());
    Assert.assertEquals("new value1", tree.get("key1"));

    Assert.assertTrue(tree.put("key2", "value2"));
    Assert.assertTrue(tree.contains("key2"));
    Assert.assertEquals(2, tree.size());
    Assert.assertEquals("value2", tree.get("key2"));
    Assert.assertEquals("new value1", tree.get("key1"));

    Assert.assertFalse(tree.remove("nonexistent key"));

    Assert.assertTrue(tree.remove("key1"));
    Assert.assertEquals(1, tree.size());
    Assert.assertEquals("value2", tree.get("key2"));
  }

  @Test
  public void testDensePutRemove() {
    final long seed = System.currentTimeMillis();
    final Random random = new Random(seed);
    System.out.println("StringSebTreeTest.testDensePutRemove seed: " + seed);

    final int count = (int) Math.sqrt(RANDOMIZED_TESTS_ITERATIONS);
    final Map<String, String> expected = new TreeMap<>();
    for (int i = 0; i < RANDOMIZED_TESTS_ITERATIONS; ++i) {
      switch (random.nextInt(2)) {
      case 0: {
        final String key = "key" + random.nextInt(count);
        final String value = "value" + random.nextInt(count);
        Assert.assertEquals(expected.put(key, value) == null, tree.put(key, value));
      }
      break;
      case 1: {
        final String key = "key" + random.nextInt(count);
        Assert.assertEquals(expected.remove(key) != null, tree.remove(key));
      }
      break;
      }
    }

    for (int i = 0; i < count; ++i) {
      final String key = "key" + i;
      Assert.assertEquals(expected.get(key), tree.get(key));
    }

    Assert.assertEquals(expected.size(), tree.size());
  }

  @Test
  public void testLargeDensePutRemove() throws IOException {
    final int SIZE = OSebTreeNode.MAX_ENTRY_SIZE / 4 - 50;
    final String keyPostfix = dup('k', SIZE);
    final String valuePostfix = dup('v', SIZE);

    final long seed = System.currentTimeMillis();
    final Random random = new Random(seed);
    System.out.println("StringSebTreeTest.testLargeDensePutRemove seed: " + seed);

    final int count = RANDOMIZED_TESTS_ITERATIONS / 2; //(int) Math.sqrt(RANDOMIZED_TESTS_ITERATIONS);
    final Map<String, String> expected = new TreeMap<>();

    try {
      for (int i = 0; i < RANDOMIZED_TESTS_ITERATIONS; ++i) {
        switch (random.nextInt(3)) {

        case 0: {
          final int keyId = random.nextInt(count);
          final int valueId = random.nextInt(count);
          final String key = keyId + keyPostfix;
          final String value = valueId + valuePostfix;
//          System.out.println("put: " + keyId + " -> " + valueId);
          Assert.assertEquals(expected.put(key, value) == null, tree.put(key, value));

//          tree.dump();
        }
        break;

        case 1: {
          final int keyId = random.nextInt(count);
          final String key = keyId + keyPostfix;
//          System.out.println("delete: " + keyId);
          Assert.assertEquals(expected.remove(key) != null, tree.remove(key));

//          tree.dump();
        }
        break;

        case 2: {
          if (!expected.isEmpty()) {
            final String key = expected.keySet().toArray(new String[0])[random.nextInt(expected.size())];
            final String value = expected.get(key);
            final String newValue = value.length() * 2 < (OSebTreeNode.MAX_ENTRY_SIZE / 2 - SIZE) - 100 ?
                value + value :
                value.substring(0, value.length() / 2);
            Assert.assertEquals(expected.put(key, newValue) == null, tree.put(key, newValue));

//            tree.dump();
          }
        }
        break;

        }
      }

      for (int i = 0; i < count; ++i) {
        final String key = i + keyPostfix;
        Assert.assertEquals(key, expected.get(key), tree.get(key));
      }

      Assert.assertEquals(expected.size(), tree.size());

    } finally {
      tree.dump();

      System.out.println("Size: " + expected.size());
      for (Map.Entry<String, String> e : expected.entrySet()) {
        System.out.println(e.getKey().substring(0, 3) + " -> " + e.getValue().substring(0, 3));
      }
    }
  }

  @Test
  public void testLeafRootSplit() {
    final String A = dup('A', LARGE);
    final String B = dup('B', LARGE);
    final String C = dup('C', LARGE);
    final String a = dup('a', SMALL);
    final String b = dup('b', SMALL);
    final String c = dup('c', SMALL);

    tree.put("a", A);
    tree.put("b", B);
    tree.put("c", C);
    Assert.assertTrue(tree.contains("a"));
    Assert.assertTrue(tree.contains("b"));
    Assert.assertTrue(tree.contains("c"));
    tree.reset();

    tree.put("b", B);
    tree.put("a", A);
    tree.put("c", C);
    Assert.assertTrue(tree.contains("a"));
    Assert.assertTrue(tree.contains("b"));
    Assert.assertTrue(tree.contains("c"));
    tree.reset();

    tree.put("b", B);
    tree.put("c", C);
    tree.put("a", A);
    Assert.assertTrue(tree.contains("a"));
    Assert.assertTrue(tree.contains("b"));
    Assert.assertTrue(tree.contains("c"));
    tree.reset();

    tree.put("c", C);
    tree.put("a", A);
    tree.put("b", B);
    Assert.assertTrue(tree.contains("a"));
    Assert.assertTrue(tree.contains("b"));
    Assert.assertTrue(tree.contains("c"));
    tree.reset();

    tree.put("a", a);
    tree.put("b", b);
    tree.put("c", c);
    tree.put("c", C);
    Assert.assertTrue(tree.contains("a"));
    Assert.assertTrue(tree.contains("b"));
    Assert.assertTrue(tree.contains("c"));
    tree.reset();

    tree.put("a", a);
    tree.put("b", b);
    tree.put("c", c);
    tree.put("b", B);
    Assert.assertTrue(tree.contains("a"));
    Assert.assertTrue(tree.contains("b"));
    Assert.assertTrue(tree.contains("c"));
    tree.reset();

    tree.put("a", a);
    tree.put("b", b);
    tree.put("c", c);
    tree.put("a", A);
    Assert.assertTrue(tree.contains("a"));
    Assert.assertTrue(tree.contains("b"));
    Assert.assertTrue(tree.contains("c"));
    tree.reset();
  }

  @Test
  public void testNonLeafSplit() {
    final String A = dup('A', LARGE);
    final String B = dup('B', LARGE);
    final String X = dup('X', LARGE);
    final String Y = dup('Y', LARGE);
    final String Z = dup('Z', LARGE);
    final String x = dup('x', SMALL);
    final String y = dup('y', SMALL);
    final String z = dup('z', SMALL);

    makeNonLeafRootTree();
    tree.put(A, "A");
    tree.put(X, "X");
    tree.put(Y, "Y");
    tree.put(Z, "Z");
    tree.put(B, "B");
    Assert.assertTrue(tree.contains("a"));
    Assert.assertTrue(tree.contains("b"));
    Assert.assertTrue(tree.contains("c"));
    Assert.assertTrue(tree.contains(A));
    Assert.assertTrue(tree.contains(X));
    Assert.assertTrue(tree.contains(Y));
    Assert.assertTrue(tree.contains(Z));
  }

  private static String dup(char char_, int length) {
    final StringBuilder builder = new StringBuilder(length);
    for (int i = 0; i < length; ++i)
      builder.append(char_);
    return builder.toString();
  }

  private static void makeNonLeafRootTree() {
    tree.reset();

    final String A = dup('A', LARGE);
    final String B = dup('B', LARGE);
    final String C = dup('C', LARGE);

    tree.put("a", A);
    tree.put("b", B);
    tree.put("c", C);
  }

}
