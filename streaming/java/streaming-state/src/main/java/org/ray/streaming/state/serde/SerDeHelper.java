package org.ray.streaming.state.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import de.javakaffee.kryoserializers.guava.ArrayListMultimapSerializer;
import de.javakaffee.kryoserializers.guava.HashMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableListSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableSetSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableSortedSetSerializer;
import de.javakaffee.kryoserializers.guava.LinkedHashMultimapSerializer;
import de.javakaffee.kryoserializers.guava.LinkedListMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ReverseListSerializer;
import de.javakaffee.kryoserializers.guava.TreeMultimapSerializer;
import de.javakaffee.kryoserializers.guava.UnmodifiableNavigableSetSerializer;
import java.io.ByteArrayOutputStream;
import java.util.List;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * kryo wrapper.
 * Created by eagle on 2019/8/15.
 */
public class SerDeHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(SerDeHelper.class);
  private static List<String> needRegisterClasses;

  private static ThreadLocal<Kryo> local = new ThreadLocal<Kryo>() {
    @Override
    protected Kryo initialValue() {
      Kryo kryo = new Kryo();
      Kryo.DefaultInstantiatorStrategy is = new Kryo.DefaultInstantiatorStrategy();
      is.setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
      kryo.setInstantiatorStrategy(is);
      kryo.getFieldSerializerConfig().setOptimizedGenerics(true);

      ArrayListMultimapSerializer.registerSerializers(kryo);
      HashMultimapSerializer.registerSerializers(kryo);
      ImmutableListSerializer.registerSerializers(kryo);
      ImmutableMapSerializer.registerSerializers(kryo);
      ImmutableMultimapSerializer.registerSerializers(kryo);
      ImmutableSetSerializer.registerSerializers(kryo);
      ImmutableSortedSetSerializer.registerSerializers(kryo);
      LinkedHashMultimapSerializer.registerSerializers(kryo);
      LinkedListMultimapSerializer.registerSerializers(kryo);
      ReverseListSerializer.registerSerializers(kryo);
      TreeMultimapSerializer.registerSerializers(kryo);
      UnmodifiableNavigableSetSerializer.registerSerializers(kryo);

      ClassLoader tcl = Thread.currentThread().getContextClassLoader();
      if (tcl != null) {
        kryo.setClassLoader(tcl);
      }
      if (needRegisterClasses != null && needRegisterClasses.size() != 0) {
        for (String clazz : needRegisterClasses) {
          try {
            String[] clazzToId = clazz.split(":");
            int registerId = Integer.parseInt(clazzToId[1]);
            LOGGER.info("register class:{} id:{}", clazz, registerId);
            kryo.register(Class.forName(clazzToId[0], false,
                Thread.currentThread().getContextClassLoader()), registerId);
          } catch (ClassNotFoundException e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e);
          }
        }
      }

      return kryo;
    }
  };

  public static synchronized void init(List<String> needRegisterClasses) {
    if (SerDeHelper.needRegisterClasses == null) {
      SerDeHelper.needRegisterClasses = needRegisterClasses;
    }
  }

  public static byte[] object2Byte(Object o) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream(4096 * 5);
    Output output = new Output(outputStream);
    try {
      local.get().writeClassAndObject(output, o);
      output.flush();
    } finally {
      output.clear();
      output.close();
    }
    return outputStream.toByteArray();
  }

  /**
   * str may be modified and then returned to its original state during deserialization
   */
  public static Object byte2Object(byte[] str) {
    Input input = new Input(str);
    return local.get().readClassAndObject(input);
  }

  public static byte[] object2ByteWithoutClass(Object o) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream(4096 * 5);
    Output output = new Output(outputStream);
    try {
      local.get().writeObject(output, o);
      output.flush();
    } finally {
      output.clear();
      output.close();
    }
    return outputStream.toByteArray();
  }

  public static Object byte2ObjectWithoutClass(byte[] str, Class clazz) {
    Input input = new Input(str);
    return local.get().readObject(input, clazz);
  }

  public static void clean() {
    local.remove();
  }
}
