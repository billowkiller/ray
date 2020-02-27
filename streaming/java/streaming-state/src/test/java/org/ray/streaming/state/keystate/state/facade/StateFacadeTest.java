package org.ray.streaming.state.keystate.state.facade;

import java.util.HashMap;
import org.ray.streaming.state.backend.KeyStateBackend;
import org.ray.streaming.state.backend.impl.MemoryStateBackend;
import org.ray.streaming.state.keystate.KeyGroup;

public class StateFacadeTest {

  protected KeyStateBackend keyStateBackend = new KeyStateBackend(1, new KeyGroup(1, 1),
    new MemoryStateBackend(new HashMap<String, String>()));
}
