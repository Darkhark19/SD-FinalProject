package tp1.impl.servers.rest.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Logger;

import org.glassfish.jersey.message.internal.ReaderWriter;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;

public class CustomLoggingFilter implements ContainerRequestFilter, ContainerResponseFilter {
	private static Logger Log = Logger.getLogger(CustomLoggingFilter.class.getName());

	@Override
	public void filter(ContainerRequestContext requestContext) throws IOException {
		StringBuilder sb = new StringBuilder();
		sb.append(" - Path: ").append(requestContext.getUriInfo().getPath());
		sb.append(" - Header: ").append(requestContext.getHeaders());
//		sb.append(" - Entity: ").append(getEntityBody(requestContext));
		sb.append("\n");
		Log.info("HTTP REQUEST : " + sb.toString());
	}

	String getEntityBody(ContainerRequestContext requestContext) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		InputStream in = requestContext.getEntityStream();

		final StringBuilder b = new StringBuilder();
		try {
			ReaderWriter.writeTo(in, out);

			byte[] requestEntity = out.toByteArray();
			if (requestEntity.length == 0) {
				b.append("").append("\n");
			} else {
				b.append(new String(requestEntity)).append("\n");
			}
			requestContext.setEntityStream(new ByteArrayInputStream(requestEntity));

		} catch (IOException ex) {
			// Handle logging error
		}
		return b.toString();
	}

	@Override
	public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
			throws IOException {

		StringBuilder sb = new StringBuilder();
		sb.append("Header: ").append(responseContext.getHeaders());
//		sb.append(" - Entity: ").append(Entity.entity(responseContext.getEntity(), MediaType.APPLICATION_JSON));
		sb.append("\n");
		Log.info("HTTP RESPONSE : " + responseContext.getStatus() + " " + sb.toString());
	}

}