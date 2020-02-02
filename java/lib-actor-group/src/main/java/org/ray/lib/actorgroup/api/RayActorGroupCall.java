package org.ray.lib.actorgroup.api;

import java.util.List;
import org.ray.api.RayObject;
import org.ray.api.function.RayFunc2;
import org.ray.lib.actorgroup.api.options.ActorGroupCallOptions;

class RayActorGroupCall {

  public static <S, T0, R> RayObject<R> call(RayFunc2<S, T0, R> f, ActorGroup<S> group, T0 t0,
      ActorGroupCallOptions options) {
    Object[] args = new Object[]{t0};
    return RayActorGroup.internal().call(f, group, args, options);
  }

  public static <S, T0, R> List<RayObject<R>> broadcast(RayFunc2<S, T0, R> f, ActorGroup<S> group,
      T0 t0, ActorGroupCallOptions options) {
    return null;
  }
}
