package com.AAA.BBB.rs;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;

import com.sun.jersey.api.core.ResourceContext;
import com.sun.jersey.spi.resource.Singleton;

@Singleton
@Path("/")
@Produces(MediaType.TEXT_HTML)
public class DefaultResource {
	private static Logger _logger = Logger.getLogger(DefaultResource.class);
	
	@Context 
	protected ResourceContext resourceContext;
	
	public DefaultResource() {
	}
	
	@GET
	public Response defaultHtml(@Context HttpServletRequest request, @Context HttpServletResponse response){				
		SearchResource searchResource=resourceContext.getResource(SearchResource.class);
		return searchResource.allSearch(request, response);
    } 
	
	@POST
	public Response defaultHtmlPost(@Context HttpServletRequest request, @Context HttpServletResponse response){				
		return defaultHtml(request, response);
    }
	
	@GET
	@Path("{default}")
	public Response notFoundHtml(@Context HttpServletRequest request, @Context HttpServletResponse response){		
		SearchResource searchResource=resourceContext.getResource(SearchResource.class);
		return searchResource.allSearch(request, response);
    }	
	
	@POST
	@Path("{default}")
	public Response notFoundHtmlPost(@Context HttpServletRequest request, @Context HttpServletResponse response){		
		return notFoundHtml(request, response);
    }
	
	
	@GET
	@Path("/main.do")
	public Response main (@Context HttpServletRequest request, @Context HttpServletResponse response){	
		String output=request.getParameter("output");
		String target=request.getParameter("target");
		_logger.debug("target == "+target + ", output=" + output);
		SearchResource searchResource=resourceContext.getResource(SearchResource.class);
		if("partnercontent".equals(target)){
			if("json".equals(output)){
				return Response.ok(searchResource.partnerSearchJson(request), MediaType.APPLICATION_JSON).build();
			}
			return Response.ok(searchResource.partnerSearchXml(request), MediaType.APPLICATION_XML).build();
		}else if("mostpopular".equals(target)){
			if("json".equals(output)){
				return Response.ok(searchResource.mostPopularSearchJson(request), MediaType.APPLICATION_JSON).build();
			}
			return Response.ok(searchResource.mostPopularSearchXml(request), MediaType.APPLICATION_XML).build();
		}else{
			if("xml".equals(output)){
				return Response.ok(searchResource.newsSearchXml(request), MediaType.APPLICATION_XML).build();
			}else if("json".equals(output)){
				return Response.ok(searchResource.newsSearchJson(request), MediaType.APPLICATION_JSON).build();
			}else if("rss".equals(output)){
				return Response.ok(searchResource.newsSearchRss(request), MediaType.APPLICATION_XML).build();
			}
		}
		return searchResource.allSearch(request, response);
   }
	
	@POST
	@Path("/main.do")
	public Response mainPost (@Context HttpServletRequest request, @Context HttpServletResponse response){
		return main(request, response);
	}
	
	@GET
	@Path("/cmsSearch.do")
	public Response cmsSearch(@QueryParam("output") String output, @Context HttpServletRequest request){
		_logger.debug("cmsSearch.do output : " + output);
		SearchResource searchResource=resourceContext.getResource(SearchResource.class);
		if("xml".equals(output)){
			return Response.ok(searchResource.cmsSearchXml(request), MediaType.APPLICATION_XML).build();
		}else if("json".equals(output)){
			_logger.debug("json cmsSearch.do");
			return Response.ok(searchResource.cmsSearchJson(request), MediaType.APPLICATION_JSON).build();
		}else if("rss".equals(output)){
			return Response.ok(searchResource.cmsSearchRss(request), MediaType.APPLICATION_XML).build();
		}
		return Response.ok(searchResource.cmsSearchXml(request), MediaType.APPLICATION_XML).build();
    }
	
	
	@GET
	@Path("/breakingNews.do")
	public Response breakingNews(@QueryParam("output") String output, @Context HttpServletRequest request){	
		SearchResource searchResource=resourceContext.getResource(SearchResource.class);
		if("xml".equals(output)){
			return Response.ok(searchResource.breakingNewsXml(request), MediaType.APPLICATION_XML).build();
		}else if("json".equals(output)){
			return Response.ok(searchResource.breakingNewsJson(request), MediaType.APPLICATION_JSON).build();
		}else if("rss".equals(output)){
			return Response.ok(searchResource.breakingNewsRss(request), MediaType.APPLICATION_XML).build();
		}
		return Response.ok(searchResource.breakingNewsXml(request), MediaType.APPLICATION_XML).build();
    }
    
}