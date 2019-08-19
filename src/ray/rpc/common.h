
#ifndef RAY_RPC_COMMON_H
#define RAY_RPC_COMMON_H

#include "ray/protobuf/common.pb.h"

namespace ray {
namespace rpc {

template <typename T, typename F>
const std::string GenerateProtobufEnumName(T value, F name) {
  return name(value);
}

template <typename T, typename F>
const std::vector<std::string> GenerateProtobufEnumNames(T min_value, T max_value,
                                                         F name) {
  std::vector<std::string> enum_names;

  for (int i = static_cast<int>(min_value); i <= static_cast<int>(max_value); i++) {
    enum_names.push_back(name(static_cast<T>(i)));
  }
  return enum_names;
}

// Generate enum name string for `value` of type `T`. Internally this calls
// `T_name(value)` and function `T_name` is generated by protobuf compiler (see *.pb.h).
// For example we have an enum defined in protobuf:
//
// enum ServiceMessageType {
//  ConnectClient = 0;
//  DisconnectClient = 1;
// }
//
// GenerateEnumName(ServiceMessageType, ConnectClient) generates
// "ServiceMessageType_ConnectClient".
#define GenerateEnumName(T, value)                                     \
  GenerateProtobufEnumName<T, decltype(BOOST_PP_CAT(T, _Name))>(value, \
                                                                BOOST_PP_CAT(T, _Name))

// Generate enum name string for type `T`. For the above enum,
// GenerateEnumNames(ServiceMessageType) generates all the possible
// enum strings from "ServiceMessageType_MIN" to "ServiceMessageType_MAX".
#define GenerateEnumNames(T)                                      \
  GenerateProtobufEnumNames<T, decltype(BOOST_PP_CAT(T, _Name))>( \
      BOOST_PP_CAT(T, _MIN), BOOST_PP_CAT(T, _MAX), BOOST_PP_CAT(T, _Name))

}  // namespace rpc
}  // namespace ray

#endif