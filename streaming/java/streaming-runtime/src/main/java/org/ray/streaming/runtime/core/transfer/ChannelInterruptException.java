package org.ray.streaming.runtime.core.transfer;

public class ChannelInterruptException extends RuntimeException {
  public ChannelInterruptException() {
    super();
  }

  public ChannelInterruptException(String message) {
    super(message);
  }
}
