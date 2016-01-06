package org.apache.usergrid.java.client.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.usergrid.java.client.response.UsergridResponse;

import javax.ws.rs.*;
import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.ClientResponseContext;
import javax.ws.rs.client.ClientResponseFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;
import java.io.IOException;

@Provider
public class ErrorResponseFilter implements ClientResponseFilter {

  private static ObjectMapper _MAPPER = new ObjectMapper();

  @Override
  public final void filter(final ClientRequestContext requestContext,
                           final ClientResponseContext responseContext) throws IOException {

    if (responseContext.getStatus() != Response.Status.OK.getStatusCode()) {
      if (responseContext.hasEntity()) {
        // get the "real" error message

        int responseCode = responseContext.getStatus();

        UsergridResponse error = _MAPPER.readValue(responseContext.getEntityStream(), UsergridResponse.class);

        String message = String.format("HTTP %s: (%s) %s", responseCode, error.getError(), error.getErrorDescription());

        Response.Status status = Response.Status.fromStatusCode(responseContext.getStatus());
        WebApplicationException webAppException;

        switch (status) {
          case BAD_REQUEST:
            webAppException = new BadRequestException(message);
            break;
          case UNAUTHORIZED:
            webAppException = new NotAuthorizedException(message);
            break;
          case FORBIDDEN:
            webAppException = new ForbiddenException(message);
            break;
          case NOT_FOUND:
            webAppException = new NotFoundException(message);
            break;
          case METHOD_NOT_ALLOWED:
            webAppException = new NotAllowedException(message);
            break;
          case NOT_ACCEPTABLE:
            webAppException = new NotAcceptableException(message);
            break;
          case UNSUPPORTED_MEDIA_TYPE:
            webAppException = new NotSupportedException(message);
            break;
          case INTERNAL_SERVER_ERROR:
            webAppException = new InternalServerErrorException(message);
            break;
          case SERVICE_UNAVAILABLE:
            webAppException = new ServiceUnavailableException(message);
            break;
          default:
            webAppException = new WebApplicationException(message);
        }

        throw webAppException;
      }
    }
  }
}