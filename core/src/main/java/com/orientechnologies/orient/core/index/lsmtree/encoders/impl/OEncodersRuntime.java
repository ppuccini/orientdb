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

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.index.lsmtree.encoders.*;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Sergey Sitnikov
 */
public class OEncodersRuntime implements OEncoder.Runtime {

  public static final OEncodersRuntime INSTANCE = new OEncodersRuntime();

  private final OEncoder.Runtime fixedSizeView    = new View(OEncoder.Size.PreferFixed);
  private final OEncoder.Runtime variableSizeView = new View(OEncoder.Size.PreferVariable);

  private final Map<Class<?>, Provider> fixedSizeSerializerClassToProvider = new IdentityHashMap<>();
  private final Map<Class<?>, Provider> fixedSizeEncoderClassToProvider    = new IdentityHashMap<>();

  private final Map<Class<?>, Provider> variableSizeSerializerClassToProvider = new IdentityHashMap<>();
  private final Map<Class<?>, Provider> variableSizeEncoderClassToProvider    = new IdentityHashMap<>();

  private final List<Mapping> mappings = new ArrayList<>();

  private OEncodersRuntime() {
    registerV0();

    compile();
  }

  private void registerV0() {
    // @formatter:off
    registerMapping(0, OIntegerSerializer.class,   OIntegerEncoder.class,          OFixedIntegerEncoder_V0.class);
    registerMapping(0, OIntegerSerializer.class,   OIntegerEncoder.class,          OVariableIntegerEncoder_V0.class);
    registerMapping(0, null,                       OUnsignedIntegerEncoder.class,  OFixedIntegerEncoder_V0.class);
    registerMapping(0, null,                       OUnsignedIntegerEncoder.class,  OVariableUnsignedIntegerEncoder_V0.class);
    registerMapping(0, OLongSerializer.class,      OLongEncoder.class,             OFixedLongEncoder_V0.class);
    registerMapping(0, OStringSerializer.class,    OStringEncoder.class,           OStringEncoder_V0.class);

    registerMapping(0, null,                       OPageIndexEncoder.class,        OPageIndexEncoder_V0.class);
    registerMapping(0, null,                       OPagePositionEncoder.class,     OPagePositionEncoder_V0.class);
    // @formatter:on
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> OEncoder.Provider<T> getProvider(Class<? extends OEncoder<T>> encoderClass, OEncoder.Size size) {
    switch (size) {
    case Auto:
    case PreferVariable:
      return variableSizeEncoderClassToProvider.get(encoderClass);
    case PreferFixed:
      return fixedSizeEncoderClassToProvider.get(encoderClass);
    default:
      throw new IllegalStateException("Unexpected preferred size.");
    }
  }

  @Override
  public <K> OEncoder.Provider<K> getProviderForKeySerializer(OBinarySerializer<K> keyEncoder, OType[] keyTypes,
      OEncoder.Size size) {

    // TODO: better composite key support

    final OEncoder.Provider provider;
    switch (size) {
    case Auto:
    case PreferVariable:
      provider = variableSizeSerializerClassToProvider.get(keyEncoder.getClass());
      break;
    case PreferFixed:
      provider = fixedSizeSerializerClassToProvider.get(keyEncoder.getClass());
      break;
    default:
      throw new IllegalStateException("Unexpected preferred size.");
    }

    return provider == null ? new SerializerWrapper(keyEncoder, keyTypes) : provider;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <V> OEncoder.Provider<V> getProviderForValueSerializer(OBinarySerializer<V> valueEncoder, OEncoder.Size size) {
    final OEncoder.Provider provider;
    switch (size) {
    case Auto:
    case PreferVariable:
      provider = variableSizeSerializerClassToProvider.get(valueEncoder.getClass());
      break;
    case PreferFixed:
      provider = fixedSizeSerializerClassToProvider.get(valueEncoder.getClass());
      break;
    default:
      throw new IllegalStateException("Unexpected preferred size.");
    }

    return provider == null ? new SerializerWrapper(valueEncoder) : provider;
  }

  @SuppressWarnings("unchecked")
  private <V, C extends OEncoder<V>> void registerMapping(int version, Class<? extends OBinarySerializer<V>> serializerClass,
      Class<C> encoderClass, Class<? extends C> encoderVersionClass) {
    mappings.add(new Mapping(version, serializerClass, encoderClass, encoderVersionClass));
  }

  private void compile() {
    fixedSizeSerializerClassToProvider.clear();
    fixedSizeEncoderClassToProvider.clear();
    variableSizeSerializerClassToProvider.clear();
    variableSizeEncoderClassToProvider.clear();

    // find maximum version

    int maxVersion = Integer.MIN_VALUE;
    for (Mapping mapping : mappings)
      if (mapping.version > maxVersion)
        maxVersion = mapping.version;
    if (maxVersion == Integer.MIN_VALUE)
      return;

    // set provider for each version there provider is set in mapping

    for (Mapping mapping : mappings) {
      Provider fixedSizeProvider = fixedSizeEncoderClassToProvider.get(mapping.encoderClass);
      Provider variableSizeProvider = variableSizeEncoderClassToProvider.get(mapping.encoderClass);

      if (fixedSizeProvider == null) {
        fixedSizeProvider = new Provider(maxVersion);
        if (mapping.serializerClass != null)
          fixedSizeSerializerClassToProvider.put(mapping.serializerClass, fixedSizeProvider);
        fixedSizeEncoderClassToProvider.put(mapping.encoderClass, fixedSizeProvider);
      }

      if (variableSizeProvider == null) {
        variableSizeProvider = new Provider(maxVersion);
        if (mapping.serializerClass != null)
          variableSizeSerializerClassToProvider.put(mapping.serializerClass, variableSizeProvider);
        variableSizeEncoderClassToProvider.put(mapping.encoderClass, variableSizeProvider);
      }

      final OEncoder fixedSizeVersion = mapping.getFixedSizeVersion(fixedSizeView);
      final OEncoder variableSizeVersion = mapping.getVariableSizeVersion(variableSizeView);

      if (fixedSizeVersion.isOfFixedSize())
        fixedSizeProvider.updateVersion(fixedSizeVersion);
      else {
        final OEncoder existingFixedSizeVersion = fixedSizeProvider.versions[fixedSizeVersion.version()];
        if (existingFixedSizeVersion == null || !existingFixedSizeVersion.isOfFixedSize())
          // peek the most compact one, it's not of a fixed size, anyway
          fixedSizeProvider.updateVersion(
              fixedSizeVersion.minimumSize() <= variableSizeVersion.minimumSize() ? fixedSizeVersion : variableSizeVersion);
      }

      if (!variableSizeVersion.isOfFixedSize())
        variableSizeProvider.updateVersion(variableSizeVersion);
      else {
        final OEncoder existingVariableSizeVersion = variableSizeProvider.versions[variableSizeVersion.version()];
        if (existingVariableSizeVersion == null || existingVariableSizeVersion.isOfFixedSize())
          // peek the most compact one, it's not of a variable size, anyway
          variableSizeProvider.updateVersion(
              variableSizeVersion.minimumSize() <= fixedSizeVersion.minimumSize() ? variableSizeVersion : fixedSizeVersion);
      }
    }
  }

  private static class Mapping {
    public final int      version;
    public final Class<?> serializerClass;
    public final Class<?> encoderClass;

    private final Class<?> encoderVersionClass;

    private OEncoder fixedSizeVersion    = null;
    private OEncoder variableSizeVersion = null;

    public Mapping(int version, Class<?> serializerClass, Class<?> encoderClass, Class<?> encoderVersionClass) {
      this.version = version;
      this.serializerClass = serializerClass;
      this.encoderClass = encoderClass;
      this.encoderVersionClass = encoderVersionClass;
    }

    public OEncoder getFixedSizeVersion(OEncoder.Runtime runtime) {
      try {
        if (fixedSizeVersion == null) {
          try {
            final Constructor<?> constructor = encoderVersionClass.getConstructor(OEncoder.Runtime.class);
            fixedSizeVersion = (OEncoder) constructor.newInstance(runtime);
          } catch (NoSuchMethodException e) {
            fixedSizeVersion = (OEncoder) encoderVersionClass.newInstance();
          }
        }

        assert fixedSizeVersion.version() == version;
        return fixedSizeVersion;
      } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
        throw new RuntimeException("Unable to create encoder instance.", e);
      }
    }

    public OEncoder getVariableSizeVersion(OEncoder.Runtime runtime) {
      try {
        if (variableSizeVersion == null) {
          try {
            final Constructor<?> constructor = encoderVersionClass.getConstructor(OEncoder.Runtime.class);
            variableSizeVersion = (OEncoder) constructor.newInstance(runtime);
          } catch (NoSuchMethodException e) {
            variableSizeVersion = (OEncoder) encoderVersionClass.newInstance();
          }
        }

        assert variableSizeVersion.version() == version;
        return variableSizeVersion;
      } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
        throw new RuntimeException("Unable to create encoder instance.", e);
      }
    }
  }

  private static class Provider<V> implements OEncoder.Provider<V> {
    public final OEncoder[] versions;

    public Provider(int maxVersion) {
      this.versions = new OEncoder[maxVersion + 1];
    }

    public void updateVersion(OEncoder encoderVersion) {
      for (int i = encoderVersion.version(); i < versions.length; ++i)
        versions[i] = encoderVersion;
    }

    @SuppressWarnings("unchecked")
    @Override
    public OEncoder<V> getEncoder(int version) {
      return versions[version];
    }
  }

  private class View implements OEncoder.Runtime {

    private final OEncoder.Size autoSize;

    public View(OEncoder.Size autoSize) {
      this.autoSize = autoSize;
    }

    @Override
    public <T> OEncoder.Provider<T> getProvider(Class<? extends OEncoder<T>> encoderClass, OEncoder.Size size) {
      return OEncodersRuntime.this.getProvider(encoderClass, size == OEncoder.Size.Auto ? autoSize : size);
    }

    @Override
    public <K> OEncoder.Provider<K> getProviderForKeySerializer(OBinarySerializer<K> keyEncoder, OType[] keyTypes,
        OEncoder.Size size) {
      return OEncodersRuntime.this.getProviderForKeySerializer(keyEncoder, keyTypes, size == OEncoder.Size.Auto ? autoSize : size);
    }

    @Override
    public <V> OEncoder.Provider<V> getProviderForValueSerializer(OBinarySerializer<V> valueEncoder, OEncoder.Size size) {
      return OEncodersRuntime.this.getProviderForValueSerializer(valueEncoder, size == OEncoder.Size.Auto ? autoSize : size);
    }
  }

  private static class SerializerWrapper implements OEncoder, OEncoder.Provider {
    private final OBinarySerializer serializer;
    private final Object[]          hints;

    public SerializerWrapper(OBinarySerializer serializer, Object... hints) {
      this.serializer = serializer;
      this.hints = hints;
    }

    @Override
    public int version() {
      return 0;
    }

    @Override
    public int minimumSize() {
      return serializer.isFixedLength() ? serializer.getFixedLength() : UNBOUND_SIZE;
    }

    @Override
    public int maximumSize() {
      return serializer.isFixedLength() ? serializer.getFixedLength() : UNBOUND_SIZE;
    }

    @SuppressWarnings("unchecked")
    @Override
    public int exactSize(Object value) {
      return serializer.getObjectSize(value, hints);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void encode(Object value, Stream stream) {
      final byte[] bytes = new byte[exactSize(value)];
      serializer.serializeNativeObject(value, bytes, 0, hints);
      stream.write(bytes);
    }

    @Override
    public int exactSizeInStream(Stream stream) {
      final OSerializerEncoderStream serializerStream = (OSerializerEncoderStream) stream;
      final ByteBuffer buffer = serializerStream.getReadByteBuffer();
      final OWALChanges changes = serializerStream.getWALChanges();

      if (changes == null) {
        buffer.position(stream.getPosition());
        return serializer.getObjectSizeInByteBuffer(buffer);
      } else
        return serializer.getObjectSizeInByteBuffer(buffer, changes, stream.getPosition());
    }

    @Override
    public Object decode(Stream stream) {
      final OSerializerEncoderStream serializerStream = (OSerializerEncoderStream) stream;
      final ByteBuffer buffer = serializerStream.getReadByteBuffer();
      final OWALChanges changes = serializerStream.getWALChanges();

      final int size = exactSizeInStream(stream);
      final Object value;
      if (changes == null) {
        buffer.position(stream.getPosition());
        value = serializer.deserializeFromByteBufferObject(buffer);
      } else
        value = serializer.deserializeFromByteBufferObject(buffer, changes, stream.getPosition());

      stream.seek(size);
      return value;
    }

    @Override
    public OEncoder getEncoder(int version) {
      return this;
    }
  }
}
