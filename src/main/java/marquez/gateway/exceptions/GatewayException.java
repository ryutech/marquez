package marquez.gateway.exceptions;

import javax.annotation.Nullable;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class GatewayException extends Exception {
  private static final long serialVersionUID = 1L;

  public GatewayException(@Nullable final String message, @Nullable final Throwable cause) {
    super(message, cause);
  }
}
