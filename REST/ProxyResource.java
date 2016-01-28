package com.BBB.AAA.rs;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

import org.apache.log4j.Logger;
import org.perf4j.aop.Profiled;
import org.springframework.stereotype.Component;

import com.BBB.AAA.service.ApplicationConfigService;
import com.BBB.AAA.util.Constants;
import com.BBB.AAA.util.URLUtils;
import com.sun.jersey.spi.inject.Inject;



@Path("/proxy")
@Produces({MediaType.APPLICATION_XML,MediaType.TEXT_XML})
@Component
public class ProxyResource {
	
	private static Logger _logger = Logger.getLogger(ProxyResource.class);
	
	@Inject("searchAppConfigService")
	private ApplicationConfigService appConfigService;;
	
	
	@GET
	@Path("/{proxy}/view.xml")
    @Produces({MediaType.APPLICATION_XML,MediaType.TEXT_XML})
    @Profiled(tag="proxy")
	public String getcontent( @PathParam("proxy") String proxy, @Context UriInfo uriinfo ) {
	    MultivaluedMap<String,String> params=uriinfo.getQueryParameters(false);
		String  urlcontent="";
		try {
			String strurl = appConfigService.getAppConfigValue(Constants.COMPONENT_NAME,"PROXY_URL_"+proxy.toUpperCase());
			if(strurl!=null && strurl.trim().length()>0){
				StringBuffer urlBuf=new StringBuffer(strurl);
				for(String key:params.keySet()){  
					
					urlBuf.append((urlBuf.toString().indexOf('?')>0)?'&':'?');
					urlBuf.append(key).append('=').append(params.getFirst(key));
				}
				
				urlcontent=URLUtils.getURLContent(urlBuf.toString(), appConfigService.getAppConfigValue(Constants.COMPONENT_NAME,Constants.PROXY_HOST ),
						appConfigService.getAppConfigValue(Constants.COMPONENT_NAME,Constants.PROXY_PORT ),
						appConfigService.getAppConfigValue(Constants.COMPONENT_NAME,Constants.PROXY_CONNECTION_TIMEOUT),
						appConfigService.getAppConfigValue(Constants.COMPONENT_NAME,Constants.PROXY_READ_TIMEOUT));
			}
		} catch (Exception e) {
			_logger.error("Error in getting the Proxy URL content",e);
		}
		return urlcontent;
    }
}
