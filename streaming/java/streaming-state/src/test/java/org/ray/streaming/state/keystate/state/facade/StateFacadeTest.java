package org.ray.streaming.state.keystate.state.facade;

import java.util.HashMap;
import org.ray.streaming.state.backend.KeyStateBackend;
import org.ray.streaming.state.backend.impl.MemoryStateBackend;
import org.ray.streaming.state.keystate.KeyGroup;

/**
 * Alipay.com Inc
 * Copyright (c) 2004-2017 All Rights Reserved.
 *
 * @author wutao on 2017/9/13.
 */
public class StateFacadeTest {

  protected KeyStateBackend keyStateBackend = new KeyStateBackend(1, new KeyGroup(1, 1),
    new MemoryStateBackend(new HashMap<String, String>()));
}
