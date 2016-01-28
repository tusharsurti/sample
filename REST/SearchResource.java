package com.BBB.AAA.rs;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.xml.serialize.XMLSerializer;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.perf4j.aop.Profiled;

import com.BBB.AAA.acquiremedia.HttpCookie;
import com.BBB.AAA.businessobjects.CombinedSearchResponse;
import com.BBB.AAA.businessobjects.CustomCombinedSearchResponse;
import com.BBB.AAA.businessobjects.CustomSiteSearchResponse;
import com.BBB.AAA.businessobjects.PageContentSettings;
import com.BBB.AAA.businessobjects.PageSettings;
import com.BBB.AAA.businessobjects.RequestParam;
import com.BBB.AAA.businessobjects.RequestParams;
import com.BBB.AAA.businessobjects.SearchPageResults;
import com.BBB.AAA.businessobjects.SearchRequest;
import com.BBB.AAA.businessobjects.SearchResponse;
import com.BBB.AAA.businessobjects.SiteAsset;
import com.BBB.AAA.businessobjects.SiteSearchResponse;
import com.BBB.AAA.exception.ServiceException;
import com.BBB.AAA.service.ApplicationConfigService;
import com.BBB.AAA.service.CMSQuerySearchService;
import com.BBB.AAA.service.CMSSearchService;
import com.BBB.AAA.service.CombinedSearchService;
import com.BBB.AAA.service.DictionaryService;
import com.BBB.AAA.service.SiteSearchService;
import com.BBB.AAA.util.Constants;
import com.BBB.AAA.util.View;
import com.BBB.AAA.xss.XSSEncoder;
import com.sun.jersey.api.view.Viewable;


@Path("/search")
@Produces(MediaType.TEXT_HTML)
public class SearchResource  {
	private static Logger _logger = Logger.getLogger(SearchResource.class);
	private static final String USER_AGENT = "user-agent";
	private static final String USER_IP_ADDRESS = "HTTP_X_FORWARDED_FOR";

	private List<CMSSearchService> cmsSearchServiceList;
	
	private List<CMSSearchService> combinedCmsSearchServiceList;

	private List<CMSSearchService> breakingNewsServiceList;
	
	private List<CMSQuerySearchService> cmsQuerySearchServiceList;
	
	private List<SiteSearchService> siteSearchServiceList;
	
	private List<CombinedSearchService> combinedSearchServiceList;
	
	private List<SiteSearchService> partnerSearchServiceList;
	
	private DictionaryService dictionaryService;

	private JAXBContext jaxbContext;
	
	private ApplicationConfigService appConfigService;
	

	public SearchResource() {
	}
		
	
		
	@GET
	@Path("/all/view.html")
	@Profiled(tag="allsearch",logFailuresSeparately = true)
	public Response allSearch(@Context HttpServletRequest request, @Context HttpServletResponse response) {
		String path = View.getPath("allsearch", null);
		SearchRequest srequest =new SearchRequest(request);
		if(null!= srequest.getSource() &&  srequest.getSource().trim().length()>0){
			srequest.setType("news");
		}
		srequest.setKeywords(modifyKeywords(srequest.getKeywords()));
		srequest= stripSecialChars(srequest);
		setDefaultSiteSearchIndex(srequest,request);
		return Response.ok(new Viewable(path,prepareSearchResponse(srequest,request, response))).build();
	}
	
	private SearchRequest stripSecialChars(SearchRequest sr){
		sr.setByline(XSSEncoder.encode(sr.getByline()));
		sr.setCategories(XSSEncoder.encode(sr.getCategories()));
		sr.setCustomDateFormat(XSSEncoder.encode(sr.getCustomDateFormat()));
		sr.setCustompartnertransformExt(XSSEncoder.encode(sr.getCustompartnertransformExt()));
		sr.setCustomTimeZone(XSSEncoder.encode(sr.getCustomTimeZone()));
		sr.setCustomtransform(XSSEncoder.encode(sr.getCustomtransform()));
		sr.setCompanies(XSSEncoder.encode(sr.getCompanies()));
		sr.setGuestname(XSSEncoder.encode(sr.getGuestname()));
		
		sr.setIds(XSSEncoder.encode(sr.getIds()));
		sr.setIndustries(XSSEncoder.encode(sr.getIndustries()));
		sr.setKeywords(XSSEncoder.encode(sr.getKeywords()));
		sr.setLayout(XSSEncoder.encode(sr.getLayout()));
		sr.setNetwork(XSSEncoder.encode(sr.getNetwork()));
		sr.setPageContentNodes(XSSEncoder.encode(sr.getPageContentNodes()));
		sr.setPartnerId(XSSEncoder.encode(sr.getPartnerId()));
		sr.setPrefix(XSSEncoder.encode(sr.getPrefix()));
		sr.setPubfreq(XSSEncoder.encode(sr.getPubfreq()));
		sr.setQueries(XSSEncoder.encode(sr.getQueries()));
		sr.setQuery(XSSEncoder.encode(sr.getQuery()));
		
		sr.setSectors(XSSEncoder.encode(sr.getSectors()));
		sr.setShow(XSSEncoder.encode(sr.getShow()));
		sr.setSort(XSSEncoder.encode(sr.getSort()));
		sr.setSource(XSSEncoder.encode(sr.getSource()));
		sr.setTickersymbols(XSSEncoder.encode(sr.getTickersymbols()));
		sr.setTopics(XSSEncoder.encode(sr.getTopics()));
		sr.setType(XSSEncoder.encode(sr.getType()));
		
		return sr;
	}
	
	@GET
	@Path("/all/view.xml")
    @Produces({MediaType.APPLICATION_XML,MediaType.TEXT_XML})
    @Profiled(tag="allsearchxml",logFailuresSeparately = true)
	public CombinedSearchResponse allSearchXml(@Context HttpServletRequest request, @Context HttpServletResponse response) {
		return getCombinedSearchResults(new SearchRequest(request),true, request, response);	
    }
	
	@POST
	@Path("/all/view.xml")
    @Produces({MediaType.APPLICATION_XML,MediaType.TEXT_XML})
    @Profiled(tag="allsearchPostxml",logFailuresSeparately = true)
	public CombinedSearchResponse allSearchPostXml(@Context HttpServletRequest request, @Context HttpServletResponse response) {
		return allSearchXml(request, response);	
    }
    
    
    @GET
	@Path("/all/view.js")
    @Produces(MediaType.APPLICATION_JSON)
    @Profiled(tag="allsearchjson",logFailuresSeparately = true)
	public CombinedSearchResponse allSearchJson(@Context HttpServletRequest request, @Context HttpServletResponse response) {
		return getCombinedSearchResults(new SearchRequest(request),true, request, response);	
    }
    
    @GET
	@Path("/all/view.rss")
    @Produces({MediaType.APPLICATION_XML,MediaType.TEXT_XML})
    @Profiled(tag="allsearchrss",logFailuresSeparately = true)
	public String allSearchRss(@Context HttpServletRequest request, @Context HttpServletResponse response) {
		return applyRssTransform(getCombinedSearchResults(new SearchRequest(request),false, request, response));
    	
    }
    
	
	
	@GET
	@Path("/news/view.xml")
    @Produces({MediaType.APPLICATION_XML,MediaType.TEXT_XML})
    @Profiled(tag="newssearchxml",logFailuresSeparately = true)
    public SiteSearchResponse newsSearchXml(@Context HttpServletRequest request) {
		return getNewsSearchResults(request);	
    }
	
	@GET
	@Path("/news/view.rss")
	@Profiled(tag="newssearchrss",logFailuresSeparately = true)
    @Produces({MediaType.APPLICATION_XML,MediaType.TEXT_XML})
    public SiteSearchResponse newsSearchRss(@Context HttpServletRequest request) {
		CustomSiteSearchResponse response=(CustomSiteSearchResponse)getNewsSearchResults(request);
		addRssTransform(response);
		return response;	
    }
	
	
    @GET
	@Path("/news/view.js")
    @Produces(MediaType.APPLICATION_JSON)
    @Profiled(tag="newssearchjson",logFailuresSeparately = true)
	public SiteSearchResponse newsSearchJson(@Context HttpServletRequest request) {
    	return getNewsSearchResults(request);	
    }
    
    @GET
    @Path("/cms/view.xml")
    @Produces({MediaType.APPLICATION_XML,MediaType.TEXT_XML})
    @Profiled(tag="cmssearchxml",logFailuresSeparately = true)
	public SiteSearchResponse cmsSearchXml(@Context HttpServletRequest request) {
		return getCmsSearchResults(request);	
    }
    
    
    @GET
	@Path("/cms/view.rss")
    @Produces(MediaType.APPLICATION_XML)
    @Profiled(tag="cmssearchrss",logFailuresSeparately = true)
	public SiteSearchResponse cmsSearchRss(@Context HttpServletRequest request) {
    	CustomSiteSearchResponse response=(CustomSiteSearchResponse)getCmsSearchResults(request);
    	addRssTransform(response);
		return response;	
    }
    
    @GET
	@Path("/cms/view.js")
    @Produces(MediaType.APPLICATION_JSON)
    @Profiled(tag="cmssearchjson",logFailuresSeparately = true)
	public SiteSearchResponse cmsSearchJson(@Context HttpServletRequest request) {
		return getCmsSearchResults(request);	
    }
    
    @GET
    @Path("/cmsQuery/view.xml")
    @Produces({MediaType.APPLICATION_XML,MediaType.TEXT_XML})
    @Profiled(tag="cmsquerysearchxml",logFailuresSeparately = true)
	public SiteSearchResponse cmsQuerySearchXml(@Context HttpServletRequest request) {
		return getCmsQuerySearchResults(request);	
    }
    
    @GET
	@Path("/cmsQuery/view.js")
    @Produces(MediaType.APPLICATION_JSON)
    @Profiled(tag="cmsquerysearchjson",logFailuresSeparately = true)
	public SiteSearchResponse cmsQuerySearchJson(@Context HttpServletRequest request) {
		return getCmsQuerySearchResults(request);	
    }
    
    @GET
    @Path("/combinedcms/view.xml")
    @Produces({MediaType.APPLICATION_XML,MediaType.TEXT_XML})
    @Profiled(tag="allcmssearchxml",logFailuresSeparately = true)
	public CombinedSearchResponse combinedCmsSearchXml(@Context HttpServletRequest request) {
		return getCombinedCmsSearchResults(request);	
    }
    
    
    @GET
	@Path("/combinedcms/view.rss")
    @Produces(MediaType.APPLICATION_XML)
    @Profiled(tag="combinedcmssearchrss",logFailuresSeparately = true)
	public String combinedCmsSearchRss(@Context HttpServletRequest request) {
    	return applyRssTransform((CombinedSearchResponse)getCombinedCmsSearchResults(request));
			
    }
    
    @GET
	@Path("/combinedcms/view.js")
    @Produces(MediaType.APPLICATION_JSON)
    @Profiled(tag="cmssearchjson",logFailuresSeparately = true)
	public CombinedSearchResponse combinedCmsSearchJson(@Context HttpServletRequest request) {
		return getCombinedCmsSearchResults(request);	
    }
    
    @GET
	@Path("/breaking/view.xml")
    @Produces({MediaType.APPLICATION_XML,MediaType.TEXT_XML})
    @Profiled(tag="breakingnewsxml",logFailuresSeparately = true)
	public SiteSearchResponse breakingNewsXml(@Context HttpServletRequest request) {
		return getBreakingNews(request);	
    }
    
    @GET
	@Path("/breaking/view.rss")
    @Produces(MediaType.APPLICATION_XML)
    @Profiled(tag="breakingnewsrss",logFailuresSeparately = true)
	public SiteSearchResponse breakingNewsRss(@Context HttpServletRequest request) {
		return getBreakingNews(request);	
    }
    
    @GET
	@Path("/breaking/view.js")
    @Produces(MediaType.APPLICATION_JSON)
    @Profiled(tag="breakingnewsjson",logFailuresSeparately = true)
	public SiteSearchResponse breakingNewsJson(@Context HttpServletRequest request) {
		return getBreakingNews(request);	
    }
    
    @GET
	@Path("/partner/view.xml")
    @Produces({MediaType.APPLICATION_XML,MediaType.TEXT_XML})
    @Profiled(tag="partnersearchxml",logFailuresSeparately = true)
	public SiteSearchResponse partnerSearchXml(@Context HttpServletRequest request) {
		return getPartnerSearchResults(request);	
    }
    
    
    @GET
	@Path("/partner/view.js")
    @Produces(MediaType.APPLICATION_JSON)
    @Profiled(tag="partnersearchjson",logFailuresSeparately = true)
	public SiteSearchResponse partnerSearchJson(@Context HttpServletRequest request) {
		return getPartnerSearchResults(request);	
    }
	
	@GET
	@Path("/mostpopular/view.xml")
    @Produces({MediaType.APPLICATION_XML,MediaType.TEXT_XML})
    @Profiled(tag="mostpopularsearchxml",logFailuresSeparately = true)
    public SiteSearchResponse mostPopularSearchXml(@Context HttpServletRequest request) {
		return getMostPopularSearchResults(request);	
    }		
	
    @GET
	@Path("/mostpopular/view.js")
    @Produces(MediaType.APPLICATION_JSON)
    @Profiled(tag="mostpopularsearchjson",logFailuresSeparately = true)
	public SiteSearchResponse mostPopularSearchJson(@Context HttpServletRequest request) {
    	return getMostPopularSearchResults(request);	
    }   
	
	private SearchPageResults prepareSearchResponse(SearchRequest srequest, HttpServletRequest request, HttpServletResponse response){
		SearchPageResults results= new SearchPageResults();
		srequest.setCheckEmpty(true);
		results.setSearchResponse(getCombinedSearchResults(srequest,false, request, response));
		results.setRemoteIP(request.getHeader(USER_IP_ADDRESS));
		results.setUseragent(request.getHeader(USER_AGENT));
		try {
			results.setCommonResults(dictionaryService.getCommonResults(srequest.getKeywords()));
			
			if(srequest.getPubtime()==0 && "h".equalsIgnoreCase(srequest.getPubfreq())){
				 String str=appConfigService.getAppConfigValue(Constants.COMPONENT_NAME, "DEFAULT_PUBFREQ");
				 String defaultPubFreq=(str!=null && str.trim().length()>0)?str:"a";
				 str=appConfigService.getAppConfigValue(Constants.COMPONENT_NAME, "DEFAULT_PUBTIME");
				 Integer defaultPubtime=Integer.parseInt((str!=null && str.trim().length()>0)?str:"0");
				 srequest.setPubtime(defaultPubtime);srequest.setPubfreq(defaultPubFreq);
			}
			results.setSearchRequest(srequest);
			
		} catch (ServiceException e) {
			_logger.error("Error in setting common results");
		}
		return results;
	}
	
	private CombinedSearchResponse getCombinedSearchResults(SearchRequest request, boolean addCustomParams,
			HttpServletRequest httpRequest, HttpServletResponse httpResponse){
		CombinedSearchService combinedSearchService=
			new SearchServiceIdentifier<CombinedSearchService>(combinedSearchServiceList,request).getSearchService();
		CombinedSearchResponse response=null;
		if(combinedSearchService!=null){
			try{
				
				//extract if any relevant cookies are there to be sent...
				List<HttpCookie> reqCookieList = new ArrayList<HttpCookie>();
				
				//see what strategy should be employed for sticky session
				//is it server side session management or cookie
				String stickySessionStrategy = appConfigService.getAppConfigValue(Constants.COMPONENT_NAME,"STICKY_SESSION_STRATEGY");
				if(stickySessionStrategy != null){
					stickySessionStrategy = stickySessionStrategy.intern();
				}

				//checkout cookies with prefix "ss-"
				String cookiePrefix = appConfigService.getAppConfigValue(Constants.COMPONENT_NAME,"AM_COOKIE_PREFIX");

				if(("cookie").equals(stickySessionStrategy)){
					//we are using cookie base sticky session strategy
					//check if this request is having any cookie
					if(httpRequest!= null && httpRequest.getCookies()!= null){
						for(Cookie reqCookie : httpRequest.getCookies()){
							if(reqCookie.getName().startsWith(cookiePrefix)){
								HttpCookie newCookie = new HttpCookie(reqCookie.getName().substring(cookiePrefix.length()), reqCookie.getValue());
								newCookie.setVersion(0);
								reqCookieList.add(newCookie);
							}
						}
					}
					_logger.debug("Request Cookies:" + reqCookieList);
				}
				else if(("session").equals(stickySessionStrategy)){
					//AI Cache is employing sticky session strategy
					//so we will get session cookie back and will be able to retrieve value from session
					HttpSession session = httpRequest.getSession(true);
					List<HttpCookie> cookieList = (List<HttpCookie>)session.getAttribute(cookiePrefix);
					if(cookieList != null){
						reqCookieList = cookieList;
					}
					_logger.debug(new StringBuffer(100).append(" Session ID:").append(session.getId()).append(" Request Cookies:").append(reqCookieList).toString());
				}
				else if(("requestParam").equals(stickySessionStrategy)){
					//Get cookie detail for AM from URL parameter 
					String ssp = httpRequest.getParameter("ssp");
					reqCookieList = parseStickySessionParamter(ssp);
					_logger.debug(" Request Cookies (from param):" + reqCookieList);
				}
				
				response= combinedSearchService.search(request.getType(),
							request.getKeywords(),
							request.getSort(),
							request.getMinimumrelevance(),
							request.getCategories(),
							request.getCompanies(),
							request.getTopics(),
							request.getSectors(),
							request.getIndustries(),
							request.getTickersymbols(),
							request.getIds(),
							request.getSource(),
							request.getPubtime(),
							request.getPubfreq(),
							request.getAfter(),
							request.getAfterPrecision(),
							request.getBefore(),
							request.getBeforePrecision(),
							request.getQuery(),
							request.getPrefix(),
							request.getByline(),
							request.getLayout(),
							request.getShow(),
							request.getNetwork(),
							request.getGuestname(),
							new PageSettings(request.getPage(),request.getStartpage(),request.getPagesize()),
							new PageContentSettings(request.getMappagecontentsymbols(),
									request.getPageContentNodes(),
									request.getEnablepagecontentAAAlinks(), 
									request.getIncludepagecontent(),
									request.getIncludedbpagecontent()),
							request.isCheckEmpty(),
							request.getFullSearch(),
							request.isAndKeywords(),
							(reqCookieList.size()>0 ? reqCookieList : null),
							request.getPartnerId());
				
				//if response has any cookies to set pass it out to the consumer...
				if(response instanceof CustomCombinedSearchResponse){
					CustomCombinedSearchResponse ccResponse = (CustomCombinedSearchResponse) response;
					List<HttpCookie> resCookieList = ccResponse.getCookies();
					_logger.debug("Response Cookies " + resCookieList);
					if(("cookie").equals(stickySessionStrategy)){
						if(resCookieList != null && resCookieList.size() > 0){
							for(HttpCookie resCookie : resCookieList){
								httpResponse.addCookie(new Cookie(cookiePrefix + resCookie.getName(), resCookie.getValue()));
							}
						}
					}
					else if(("session").equals(stickySessionStrategy)){
						//AI Cache is employing sticky session strategy
						//so we will get session cookie back and will be able to retrieve value from session
						HttpSession session = httpRequest.getSession(true);
						session.setAttribute(cookiePrefix, resCookieList);
					}
					else if(("requestParam").equals(stickySessionStrategy)){
						if(resCookieList != null && resCookieList.size() > 0){
							String sspValue = buildStickySessionParamter(resCookieList);
							httpResponse.addCookie(new Cookie("ssp", sspValue));
						}
					}
					
				}
			
				if(addCustomParams){
					addCustomParams((CustomCombinedSearchResponse)response,request);
				}
				updateResponseWithRequestParameter(httpRequest, response);
				return response;
			}catch(ServiceException e){
				_logger.error("Error in fetching the combined search results",e);
			}
		}
		CombinedSearchResponse csr =new CombinedSearchResponse();
		updateResponseWithRequestParameter(httpRequest, csr);
		return csr;
	}

	/**
	 * join the cookies to be set as request parameter using ^ delimiter...
	 * 
	 * @param resCookieList
	 * @return
	 */
	private String buildStickySessionParamter(List<HttpCookie> resCookieList) {
		String sspValue = ""; 
		for(HttpCookie resCookie : resCookieList){
			sspValue += sspValue.length() > 0 ? "^" : "";
			sspValue += resCookie;
		}
		return sspValue;
	}
	
	/**
	 * parse the sticky session parameter built with {@link #buildStickySessionParamter(List)}
	 * 
	 * @param resCookieList
	 * @return
	 */
	private List<HttpCookie> parseStickySessionParamter(String reqSsp) {
		List<HttpCookie> cookieList = new ArrayList<HttpCookie>();
		if(reqSsp != null && reqSsp.length() > 0){
			String[] cookieStrs = StringUtils.split(reqSsp, "^");
			for(String cookie : cookieStrs){
				cookieList.addAll(HttpCookie.parse(cookie));
			}
		}
		return cookieList;
	}
	

	private SiteSearchResponse getNewsSearchResults(HttpServletRequest httpRequest){
		return getNewsSearchResults(new SearchRequest(httpRequest), httpRequest);
	}
	
	private SiteSearchResponse getNewsSearchResults(SearchRequest request, HttpServletRequest httpRequest){
		SiteSearchService siteSearchService=
			new SearchServiceIdentifier<SiteSearchService>(siteSearchServiceList,request).getSearchService();
		if(siteSearchService!=null){
			try{
				SiteSearchResponse response=getSplitIdResults(request);
				if(response==null){
				response= siteSearchService.search(request.getType(),
						    request.getKeywords(),
							request.getSort(),
							request.getMinimumrelevance(),
							request.getCategories(),
							request.getCompanies(),
							request.getTopics(),
							request.getSectors(),
							request.getIndustries(),
							request.getTickersymbols(),
							request.getIds(),
							request.getSource(),
							request.getPubtime(),
							request.getPubfreq(),
							request.getQuery(),
							request.getPrefix(),
							request.getByline(),
							request.getLayout(),
							new PageSettings(request.getPage(),request.getStartpage(),request.getPagesize()),
							new PageContentSettings(request.getMappagecontentsymbols(),
									request.getPageContentNodes(),
									request.getEnablepagecontentAAAlinks(), 
									request.getIncludepagecontent(),
									request.getIncludedbpagecontent(),
									request.getIncludeimages()),
							request.getFullSearch(),
							request.isAndKeywords(),
							request.getPartnerId());
				}
				addCustomParams((CustomSiteSearchResponse)response,request);
				updateResponseWithRequestParameter(httpRequest, response);
				return response;
			}catch(ServiceException e){
				_logger.error("Error in fetching the news search results",e);
			}
		}
		return null;
	}
	
	private SiteSearchResponse getSplitIdResults(SearchRequest request) throws ServiceException{
		int max_size=Constants.ID_REQUEST_SIZE;
		String ids =request.getIds();
		String id[]=ids.split(","); 
		if(id.length<=max_size){
			return null;
		}
		StringBuffer idBuffer=new StringBuffer();
		int total_count=1;
		List<SiteAsset> listAsset=new ArrayList<SiteAsset>();
		SiteSearchResponse response=null;
		SiteSearchService siteSearchService=
			new SearchServiceIdentifier<SiteSearchService>(siteSearchServiceList,request).getSearchService();
		for(int id_count=0;id_count<id.length;id_count++){
			idBuffer.append(id[id_count]).append(",");
			total_count++;
			
			if(total_count>max_size) {
				total_count=1;
				 response= siteSearchService.search(request.getType(),
						request.getKeywords(),
						request.getSort(),
						request.getMinimumrelevance(),
						request.getCategories(),
						request.getCompanies(),
						request.getTopics(),
						request.getSectors(),
						request.getIndustries(),
						request.getTickersymbols(),
						idBuffer.toString(),
						request.getSource(),
						request.getPubtime(),
						request.getPubfreq(),
						request.getQuery(),
						request.getPrefix(),
						request.getByline(),
						request.getLayout(),
						new PageSettings(1,request.getStartpage(),Constants.MAX_PAGE_SIZE),
						new PageContentSettings(request.getMappagecontentsymbols(),
								request.getPageContentNodes(),
								request.getEnablepagecontentAAAlinks(), 
								request.getIncludepagecontent(),
								request.getIncludedbpagecontent()),
						request.getFullSearch(),request.isAndKeywords(),
						request.getPartnerId()
								);
				 listAsset.addAll(response.getData().getSiteAsset());
				 idBuffer.delete(0, idBuffer.length());
				 
			}
			
		}
		if(total_count>1 && total_count<=max_size){
			response= siteSearchService.search(request.getType(),
					request.getKeywords(),
					request.getSort(),
					request.getMinimumrelevance(),
					request.getCategories(),
					request.getCompanies(),
					request.getTopics(),
					request.getSectors(),
					request.getIndustries(),
					request.getTickersymbols(),
					idBuffer.toString(),
					request.getSource(),
					request.getPubtime(),
					request.getPubfreq(),
					request.getQuery(),
					request.getPrefix(),
					request.getByline(),
					request.getLayout(),
					new PageSettings(1,request.getStartpage(),Constants.MAX_PAGE_SIZE),
					new PageContentSettings(request.getMappagecontentsymbols(),
							request.getPageContentNodes(),
							request.getEnablepagecontentAAAlinks(), 
							request.getIncludepagecontent(),
							request.getIncludedbpagecontent()),
					request.getFullSearch(),request.isAndKeywords(),
					request.getPartnerId());
			 listAsset.addAll(response.getData().getSiteAsset());
		}
		if(response!=null){
			SiteSearchResponse.Data data=new SiteSearchResponse.Data();
			data.setPage(1L);
			data.setPagesize((long)listAsset.size());
			data.setTotal((long)listAsset.size());
			data.setHasnext(false);
			data.setHasprev(false);
			data.getSiteAsset().addAll(listAsset);
			response.setData(data);
			return response;
		}else{
			return null;
		}
		
	}
	
	private SiteSearchResponse getCmsSearchResults(HttpServletRequest httpRequest){
		SearchRequest request = new SearchRequest(httpRequest);
		CMSSearchService cmsSearchService=
			new SearchServiceIdentifier<CMSSearchService>(cmsSearchServiceList,request).getSearchService();
		if(cmsSearchService!=null){
			try{
				SiteSearchResponse response= (SiteSearchResponse)cmsSearchService.search(request.getIds(), new PageContentSettings(request.getMappagecontentsymbols(),
																				request.getPageContentNodes(),
																				request.getEnablepagecontentAAAlinks(), 
																				request.getIncludepagecontent(),
																				request.getIncludedbpagecontent()),
																				request.getPartnerId());
				addCustomParams((CustomSiteSearchResponse)response,request);
				updateResponseWithRequestParameter(httpRequest, response);
				return response;
			}
			catch(FileNotFoundException fnfEx){
    			_logger.warn("getCmsSearchResults().FileNotFoundException for " + fnfEx.getMessage());
    		}
			catch(Exception e){
				_logger.error("Error in fetching the cms search results",e);
			}
		}
		return null;
	}
	
	private CombinedSearchResponse getCombinedCmsSearchResults(HttpServletRequest httpRequest){
		SearchRequest request = new SearchRequest(httpRequest);
		CombinedSearchResponse response=null;
		CMSSearchService combinedCmsSearchService=
			new SearchServiceIdentifier<CMSSearchService>(combinedCmsSearchServiceList,request).getSearchService();
		if(combinedCmsSearchService!=null){
			try{
				response= (CombinedSearchResponse)combinedCmsSearchService.search(request.getIds(), new PageContentSettings(request.getMappagecontentsymbols(),
																				request.getPageContentNodes(),
																				request.getEnablepagecontentAAAlinks(), 
																				request.getIncludepagecontent(),
																				request.getIncludedbpagecontent()),
																				request.getPartnerId());
				addCustomParams((CustomCombinedSearchResponse)response,request);
				updateResponseWithRequestParameter(httpRequest, response);
			}
			catch(FileNotFoundException fnfEx){
    			_logger.warn("getCombinedCmsSearchResults():FileNotFoundException for " + fnfEx.getMessage());
    		}
			catch(Exception e){
				_logger.error("Error in fetching the cms search results",e);
			}
		}
		return response;
	}
	
	private SiteSearchResponse getCmsQuerySearchResults(HttpServletRequest httpRequest){
		SearchRequest request = new SearchRequest(httpRequest);
		CMSQuerySearchService cmsQuerySearchService=
			new SearchServiceIdentifier<CMSQuerySearchService>(cmsQuerySearchServiceList,request).getSearchService();
		if(cmsQuerySearchService!=null){
			try{
				SiteSearchResponse response= (SiteSearchResponse)cmsQuerySearchService.search(request.getIds(),
																		request.getKeywords(),
																		request.getTickersymbols(),
																		request.getSource(), 
																		request.getPubtime(), 
																		request.getPubfreq(),
																		request.getQuery(),
																		request.getPrefix(),
																		request.getLayout(),
																		new PageSettings(request.getPage(),request.getStartpage(),request.getPagesize()),
																		new PageContentSettings(request.getMappagecontentsymbols(),
																				request.getPageContentNodes(),
																				request.getEnablepagecontentAAAlinks(), 
																				request.getIncludepagecontent(),
																				request.getIncludedbpagecontent()),
																				request.getPartnerId());
				addCustomParams((CustomSiteSearchResponse)response,request);
				updateResponseWithRequestParameter(httpRequest, response);
				return response;
			}catch(ServiceException e){
				_logger.error("Error in fetching the cms search results",e);
			}
		}
		return null;
	}
	
	private SiteSearchResponse getBreakingNews(HttpServletRequest httpRequest){
		SearchRequest request = new SearchRequest(httpRequest);
		CMSSearchService breakingNewsService=
			new SearchServiceIdentifier<CMSSearchService>(breakingNewsServiceList,request).getSearchService();
		if(breakingNewsService!=null){
			try{
				SiteSearchResponse response= (SiteSearchResponse)breakingNewsService.search(request.getIds(), new PageContentSettings(request.getMappagecontentsymbols(),
																				request.getPageContentNodes(),
																				request.getEnablepagecontentAAAlinks(), 
																				request.getIncludepagecontent(),
																				request.getIncludedbpagecontent()),
																				request.getPartnerId());
				addCustomParams((CustomSiteSearchResponse)response,request);
				updateResponseWithRequestParameter(httpRequest,response);
				return response;
			}
			catch(FileNotFoundException fnfEx){
    			_logger.warn("getBreakingNews().FileNotFoundException for " + fnfEx.getMessage());
    		}
			catch(Exception e){
				_logger.error("Error in fetching the cms search results",e);
			}
		}
		return null;
	}
	
	private SiteSearchResponse getPartnerSearchResults(HttpServletRequest httpRequest){
		SearchRequest request = new SearchRequest(httpRequest);
		SiteSearchService partnerSearchService=
			new SearchServiceIdentifier<SiteSearchService>(partnerSearchServiceList,request).getSearchService();
		if(partnerSearchService!=null){
			try{
				SiteSearchResponse response= partnerSearchService.search("partner",
						    request.getKeywords(),
							request.getSort(),
							request.getMinimumrelevance(),
							request.getCategories(),
							request.getCompanies(),
							request.getTopics(),
							request.getSectors(),
							request.getIndustries(),
							request.getTickersymbols(),
							request.getIds(),
							request.getSource(),
							request.getPubtime(),
							request.getPubfreq(),
							request.getQuery(),
							request.getPrefix(),
							request.getByline(),
							request.getLayout(),
							new PageSettings(request.getPage(),request.getStartpage(),request.getPagesize()),
							new PageContentSettings(request.getMappagecontentsymbols(),
									request.getPageContentNodes(),
									request.getEnablepagecontentAAAlinks(), 
									request.getIncludepagecontent(),
									request.getIncludedbpagecontent()),
							request.getFullSearch(),request.isAndKeywords(),
							request.getPartnerId());
				addCustomParams((CustomSiteSearchResponse)response,request);
				updateResponseWithRequestParameter(httpRequest, response);				
				return response;
			}catch(ServiceException e){
				_logger.error("Error in fetching the partner search results",e);
			}
		}
		return null;
	}
	
	private SiteSearchResponse getMostPopularSearchResults(HttpServletRequest httpRequest){
		try{		
			SearchRequest request = new SearchRequest(httpRequest);
			File site_mostpopular_xml = new File(appConfigService.getAppConfigValue(Constants.COMPONENT_NAME,"SITE_MOSTPOPULAR_XML"));
			SAXReader reader = new SAXReader();
			Document mostPopularDoc =  reader.read(site_mostpopular_xml);
			List<Element> ids = mostPopularDoc.selectNodes("/data/id");
			if(ids!=null){
				StringBuffer idBuffer = new StringBuffer();
				for(Element elem:ids){					
					idBuffer.append(elem.getText());
					idBuffer.append(",");
				}
								
				request.setIds(idBuffer.toString());
				return getNewsSearchResults(request, httpRequest);
			}
		}catch(Exception e){
				_logger.error("Error in fetching the most popular results",e);
		}
		return null;
	}
	
	private void addCustomParams(CustomSiteSearchResponse response, SearchRequest request){
		if(null!=response && null!=request){
			response.setCustomPartnerTransform(
					(request.getPartnertransformtype().equalsIgnoreCase("extended"))?request.getCustompartnertransformExt()
																					:request.getCustomtransform());
			response.setPageContentSettings(new PageContentSettings(request.getMappagecontentsymbols(),
																					request.getPageContentNodes(),
																					request.getEnablepagecontentAAAlinks(), 
																					request.getIncludepagecontent(),
																					request.getIncludedbpagecontent()));
			response.setCustomTimeZone(request.getCustomTimeZone());
			response.setCustomDateFormat(request.getCustomDateFormat());
		}
	}
	
	private void addCustomParams(CustomCombinedSearchResponse response, SearchRequest request){
		if(null!=response && null!=request){
			response.setCustomPartnerTransform(
					(request.getPartnertransformtype().equalsIgnoreCase("extended"))?request.getCustompartnertransformExt()
																					:request.getCustomtransform());
			
			response.setCustomTimeZone(request.getCustomTimeZone());
			response.setCustomDateFormat(request.getCustomDateFormat());
		}
	}
	
	private void addRssTransform(CustomSiteSearchResponse response){
		   try{
			   String rssTransform=Constants.getSiteRssXsl(appConfigService.getAppConfigValue(Constants.COMPONENT_NAME,"SITE_RSS_URL") ,
					appConfigService.getAppConfigValue(Constants.COMPONENT_NAME,Constants.PROXY_HOST ),
					appConfigService.getAppConfigValue(Constants.COMPONENT_NAME,Constants.PROXY_PORT ),
					appConfigService.getAppConfigValue(Constants.COMPONENT_NAME,Constants.PROXY_CONNECTION_TIMEOUT),
					appConfigService.getAppConfigValue(Constants.COMPONENT_NAME,Constants.PROXY_READ_TIMEOUT ));
				response.setCustomPartnerTransform(rssTransform);
		   }catch(Exception e){
			   _logger.error("Error in applying Rss transform",e);
		   }
	}
	
	protected String applyRssTransform(CombinedSearchResponse entity){
		String value=null;
		try{
			String rssTransform=Constants.getCombinedRssXsl(appConfigService.getAppConfigValue(Constants.COMPONENT_NAME,"COMBINED_RSS_URL") ,
					appConfigService.getAppConfigValue(Constants.COMPONENT_NAME,Constants.PROXY_HOST ),
					appConfigService.getAppConfigValue(Constants.COMPONENT_NAME,Constants.PROXY_PORT ),
					appConfigService.getAppConfigValue(Constants.COMPONENT_NAME,Constants.PROXY_CONNECTION_TIMEOUT),
					appConfigService.getAppConfigValue(Constants.COMPONENT_NAME,Constants.PROXY_READ_TIMEOUT ));
			if(rssTransform!=null && rssTransform.trim().length()>0){
				StreamSource xslt = new StreamSource(new StringReader( rssTransform));
				TransformerFactory transFactory	= TransformerFactory.newInstance();
				Transformer transformer	= transFactory.newTransformer(xslt);
	    		String modelXML=marshalModel(entity);
	    		StringWriter xmlWriter=new StringWriter();
	    		StreamResult dest	= new StreamResult(xmlWriter);
	    		StreamSource src	= new StreamSource(new StringReader(modelXML.toString()));
	    		transformer.transform(src, dest);
	    		value = xmlWriter.toString();
			
			}else{
				value = marshalModel(entity);
			}
		}catch(Exception e){
			_logger.error("Error in custom partner transform",e);
			return marshalModel(entity); 
		}
		return value;
	}
	
	protected String marshalModel(CombinedSearchResponse entity){
		Marshaller marshaller;
		StringWriter  strwriter=null;
		try {
			marshaller=jaxbContext.createMarshaller();
		    XMLSerializer serializer = new XMLSerializer();
			strwriter	= new StringWriter();
			serializer.setOutputCharStream(strwriter);
			marshaller.marshal(entity, serializer.asContentHandler());
		}
		catch(Exception e){
			_logger.error("Error",e);	
		}
		return strwriter.toString();
	}
	
	private class SearchServiceIdentifier<T> {
		private T searchService;
		public SearchServiceIdentifier(List<T> serviceList, SearchRequest request) {
			if (request.getSearchIndex()<0){
				try{
					int defaultSearchIndex=new Integer(appConfigService.getAppConfigValue(Constants.COMPONENT_NAME,"DEFAULT_SEARCH_INDEX"));
					_logger.debug("Setting the default search index :"+defaultSearchIndex);
					request.setSearchIndex(defaultSearchIndex);
				}catch (Exception e){
					_logger.error("Error in obtaining search Index",e);
					request.setSearchIndex(0);
				}
			}
			
			if(serviceList!=null && serviceList.size()>request.getSearchIndex())
				searchService=serviceList.get(request.getSearchIndex());
		}
		public  T getSearchService(){
			return searchService;
		}
	}
	
	public void setJaxbContext(JAXBContext jaxbContext) {
		this.jaxbContext = jaxbContext;
	}



	public void setAppConfigService(ApplicationConfigService appConfigService) {
		this.appConfigService = appConfigService;
	}
	
	public void setCombinedSearchServiceList(
			List<CombinedSearchService> combinedSearchServiceList) {
		this.combinedSearchServiceList = combinedSearchServiceList;
	}



	public void setCmsSearchServiceList(List<CMSSearchService> cmsSearchServiceList) {
		this.cmsSearchServiceList = cmsSearchServiceList;
	}
	
	public void setCmsQuerySearchServiceList(List<CMSQuerySearchService> cmsQuerySearchServiceList) {
		this.cmsQuerySearchServiceList = cmsQuerySearchServiceList;
	}



	public void setBreakingNewsServiceList(
			List<CMSSearchService> breakingNewsServiceList) {
		this.breakingNewsServiceList = breakingNewsServiceList;
	}



	public void setSiteSearchServiceList(
			List<SiteSearchService> siteSearchServiceList) {
		this.siteSearchServiceList = siteSearchServiceList;
	}



	public void setPartnerSearchServiceList(
			List<SiteSearchService> partnerSearchServiceList) {
		this.partnerSearchServiceList = partnerSearchServiceList;
	}



	public void setDictionaryService(DictionaryService dictionaryService) {
		this.dictionaryService = dictionaryService;
	}



	/**
	 * @param combinedCmsSearchServiceList the combinedCmsSearchServiceList to set
	 */
	public void setCombinedCmsSearchServiceList(
			List<CMSSearchService> combinedCmsSearchServiceList) {
		this.combinedCmsSearchServiceList = combinedCmsSearchServiceList;
	}
	
	private String modifyKeywords(String keywords){
		/*if(keywords!=null && keywords.indexOf(" ")>0){
			keywords=keywords.trim();
			if(keywords.startsWith("\"") && keywords.endsWith("\"")) return keywords;
			else if (keywords.startsWith("\"") && !keywords.endsWith("\"")) return keywords+"\"";
			else if (!keywords.startsWith("\"") && keywords.endsWith("\"")) return "\""+keywords;
			else return "\""+keywords+"\"";
		}*/
		_logger.debug("modifyKeywords- "+keywords);
		return keywords;
	}
	
	private void setDefaultSiteSearchIndex(SearchRequest srequest, HttpServletRequest req){
		String partnerId=req.getParameter("partnerId");
		if((partnerId==null || partnerId.trim().length()<=0) && srequest.getSearchIndex()==-1){
			try{
			 String str=appConfigService.getAppConfigValue(Constants.COMPONENT_NAME, "DEFAULT_SITE_SEARCH_INDEX");
			 Integer defaultSiteSearchIndex=Integer.parseInt((str!=null && str.trim().length()>0)?str:"-1");
			 _logger.debug("No partner Id and searchIndex set in the request. Going with the default site search Index : "+defaultSiteSearchIndex);
			 srequest.setSearchIndex(defaultSiteSearchIndex);
			}catch (Exception e){
				_logger.error("Exception in setting the default site search index",e);
			}
		}
	}
	
	/**
	 * Put all the request paremter from http request in to CombinedSearchResponse
	 * 
	 * @param request
	 * @param sr
	 */
	@SuppressWarnings("unchecked")
	private void updateResponseWithRequestParameter(HttpServletRequest request, SearchResponse sr){
		Map<String, Object> requestParams = request.getParameterMap();
		sr.setRequest(new RequestParams());
		for(String paramName : requestParams.keySet()){
			Object value = requestParams.get(paramName);
			if(value instanceof String){
				sr.getRequest().params.add(new RequestParam(paramName, 
						(String)value));
			}
			else if(value instanceof String[]){
				String val = Arrays.toString((String[])value);
				if(val != null && val.length()>0){
					val = val.substring(1, val.length()-1);
				}
				sr.getRequest().params.add(new RequestParam(paramName, 
						val));
			}
			else{
				sr.getRequest().params.add(new RequestParam(paramName, 
						value.toString()));
			}
		}
	}
}
