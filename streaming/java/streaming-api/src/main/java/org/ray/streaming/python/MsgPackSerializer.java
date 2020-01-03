package org.ray.streaming.python;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.FloatValue;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;

class MsgPackSerializer {

  byte[] serialize(Object obj) {
    MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
    serialize(obj, packer);
    return packer.toByteArray();
  }

  private void serialize(Object obj, MessageBufferPacker packer) {
    try {
      if (obj == null) {
        packer.packNil();
      } else {
        Class<?> clz = obj.getClass();
        if (clz == Boolean.class) {
          packer.packBoolean((Boolean) obj);
        } else if (clz == Integer.class) {
          packer.packInt((Integer) obj);
        } else if (clz == Long.class) {
          packer.packLong((Long) obj);
        } else if (clz == Double.class) {
          packer.packDouble((Double) obj);
        } else if (clz == String.class) {
          packer.packString((String) obj);
        } else if (obj instanceof Collection) {
          Collection collection = (Collection) (obj);
          packer.packArrayHeader(collection.size());
          for (Object o : collection) {
            serialize(o, packer);
          }
        } else if (obj instanceof Map) {
          Map map = (Map) (obj);
          packer.packMapHeader(map.size());
          for (Object o : map.entrySet()) {
            Map.Entry e = (Map.Entry) o;
            serialize(e.getKey(), packer);
            serialize(e.getValue(), packer);
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Serialize error for object " + obj, e);
    }
  }

  Object deserialize(byte[] bytes) {
    MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes);
    try {
      return convert(unpacker.unpackValue());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Object convert(Value value) {
    switch (value.getValueType()) {
      case NIL:
        return null;
      case BOOLEAN:
        return value.asBooleanValue().getBoolean();
      case INTEGER:
        IntegerValue iv = value.asIntegerValue();
        if (iv.isInIntRange()) {
          return iv.toInt();
        } else if (iv.isInLongRange()) {
          return iv.toLong();
        } else {
          return iv.toBigInteger();
        }
      case FLOAT:
        FloatValue fv = value.asFloatValue();
        return fv.toDouble();
      case STRING:
        return value.asStringValue().asString();
      case BINARY:
        return value.asBinaryValue().asByteArray();
      case ARRAY:
        ArrayValue arrayValue = value.asArrayValue();
        List<Object> list = new ArrayList<>(arrayValue.size());
        for (Value elem : arrayValue) {
          list.add(convert(elem));
        }
        return list;
      case MAP:
        MapValue mapValue = value.asMapValue();
        Map<Object, Object> map = new HashMap<>();
        for (Map.Entry<Value, Value> entry : mapValue.entrySet()) {
          map.put(convert(entry.getKey()), convert(entry.getValue()));
        }
        return map;
      default:
        throw new UnsupportedOperationException("Unsupported type " + value.getValueType());
    }
  }
}