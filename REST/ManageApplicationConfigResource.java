package com.AAA.XXX.rs;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.jcs.JCS;
import org.perf4j.aop.Profiled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.AAA.XXX.logger.WebConfigUtil;
import com.AAA.XXX.util.ResponseBuilder;

/**
 * Service to expose common runtime configuration services....
 * 
 * 
 */
@Path("/appConfig")
@Produces( { MediaType.TEXT_HTML })
@Component
public class ManageApplicationConfigResource {
	private static Logger logger = LoggerFactory.getLogger(ManageApplicationConfigResource.class);

	@GET
	@Path("{default}")
	public Response notFound(@Context HttpServletRequest servletRequest) {
		logger.debug("notFound method called");
		return ResponseBuilder.build(Response.Status.NOT_FOUND, "Request-URI is not found.");
	}

	/**
	 * HTML Page / UI for configuring Logger at runtime
	 * 
	 * @return
	 */
	@GET
	@Path("/logger")
	@Profiled(tag = "loggerUI", logFailuresSeparately = true)
	public String loggerUI() {
		try {
			return WebConfigUtil.loggerUI();
		} catch (IOException e) {
			logger.error("Error while loading logger UI ", e);
			return "Failed To load Logger Configuration UI";
		}
	}

	/**
	 * Actual Operation (of changing Logger) and logger report function for current logger configuration.
	 * 
	 * @param targetLogger
	 *        The logger which is targeted for change
	 * @param targetLogLevel
	 *        The new logging level for above logger
	 * @param logNameFilter
	 *        If too many logger are there filter the logger report with this string
	 * @param logNameFilterType
	 *        The string should be treated as "Contains" or starts with(default)
	 * @return
	 */
	@GET
	@Path("/loggerConfig")
	@Profiled(tag = "loggerConfig", logFailuresSeparately = true)
	public String loggerConfig(@DefaultValue("") @QueryParam("logger") String targetLogger,
			@DefaultValue("") @QueryParam("newLogLevel") String targetLogLevel,
			@DefaultValue("") @QueryParam("logNameFilter") String logNameFilter,
			@DefaultValue("") @QueryParam("logNameFilterType") String logNameFilterType) {
		return WebConfigUtil.loggerConfig(targetLogger, targetLogLevel, logNameFilter, logNameFilterType);
	}
	
	@GET
	@Path("/clearCache/{region}")
	@Profiled(tag = "clearCache", logFailuresSeparately = true)
	public Response clearPartnerCache(@PathParam("region") String region) {
		
		try {
			JCS cache= JCS.getInstance(region);
			cache.clear();
		} catch (Exception ex) {
			logger.error("Exception while clearing cache region : "+region, ex);
			return ResponseBuilder.build(Response.Status.INTERNAL_SERVER_ERROR, ex.getMessage());
		}
		return Response.ok(new GenericEntity<String>("Cache cleared successfully - Region: "+region) {}, MediaType.TEXT_HTML).build();
	}
	
}
