package XXX.synapi.tasks.impl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.Vector;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.DynaProperty;
import org.apache.commons.lang.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.XPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import XXX.service.AsciiReplacementService;
import XXX.service.CMSContentService;
import XXX.synapi.businessobjects.BaseDynaBean;
import XXX.synapi.businessobjects.BusinessObjectFactory;
import XXX.synapi.businessobjects.ContentValidator;
import XXX.synapi.businessobjects.ExtendedDestinationInfo;
import XXX.synapi.businessobjects.SiteAsset;
import XXX.synapi.businessobjects.SiteURL;
import XXX.synapi.businessobjects.SyndicationPartner;
import XXX.synapi.businessobjects.SyndicationTracking;
import XXX.synapi.businessobjects.VideoAsset;
import XXX.synapi.constants.GlobalConstants;
import XXX.synapi.exception.ServiceException;
import XXX.synapi.service.ContentService;
import XXX.synapi.util.ExceptionUtils;
import XXX.synapi.util.GVPProperties;
import XXX.synapi.util.PubdateDescendingComparator;
import XXX.synapi.util.URLUtils;

public class SyndicateSiteContentTaskObjectImpl  extends SyndicateTaskObjectImpl
{
	private static Logger logger = LoggerFactory.getLogger( SyndicateSiteContentTaskObjectImpl.class );
	
	protected String		sitemapIndex	= null;
	protected boolean 		syndicateAll	= false;
	protected List<SiteURL> siteURLs	    = null;
	protected String 		siteIDs			= null;
	protected Long          locationsKey    = null;
	protected CMSContentService cmsContentService 	= null;
	protected AsciiReplacementService asciiReplacementService;
	protected ContentService vcpsContentService;
	private URLUtils urlUtils = new URLUtils();
	
	public void setAsciiReplacementService(AsciiReplacementService asciiReplacementService)
	{
		this.asciiReplacementService = asciiReplacementService;
	}
		
	public void setSitemapIndex(String sitemapIndex)
	{
		this.sitemapIndex = sitemapIndex;
	}
	public String getSitemapIndex()
	{
		return sitemapIndex;
	}
		
	public void setSyndicateAll(boolean syndicateAll)
	{
		this.syndicateAll = syndicateAll;		
	}
	
	public boolean getSyndicateAll()
	{
		return syndicateAll;
	}
	
	public void setSiteIDs(String siteIDs)
	{
		this.siteIDs = siteIDs;
		setIDs(siteIDs);
	}
	
	public String getSiteIDs()
	{
		return siteIDs;
	}
	
	public Long getLocationsKey()
	{
		return locationsKey;
	}
	
	public void setLocationsKey(Long locationsKey) 
	{
		this.locationsKey = locationsKey;
	}
	
	public void setCmsContentService( CMSContentService cmsContentService )
	{
		this.cmsContentService = cmsContentService;
	}	
	
	public String validateSiteURL(String url)
	{
		String urlTitle = null;
		
		try
		{
			if((url!=null) && (url.trim().length()>0))
			{			
				SiteURL siteURL = new SiteURL();					
				siteURL.setURL(url);								
				siteURL.setXMLURL(url);			
				
				Document sitePage = getSitePage(siteURL);
				
				//Parse Document and extract valid expiration date...
				if(sitePage!=null)
				{									
					XPath xpathSelector	= DocumentHelper.createXPath( "/root" );
					Element root		= (Element)xpathSelector.selectSingleNode(sitePage);		
					
					if(root!=null)
					{
						//Extract Expirated Date....
						String expiredDate	= root.valueOf("@end");
						
						if((expiredDate!=null) && (expiredDate.length()>0))
						{
							long expiredDateValue = Long.valueOf(expiredDate).longValue();
							
							if(expiredDateValue>PAGETIME_CONVERTVAL)
							{
								//Determine if story is still valid...
								Date convertedTime	= new Date(((expiredDateValue - PAGETIME_CONVERTVAL)/10000));
								
								Calendar expirationDate = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
					    		expirationDate.setTime(convertedTime);  		    		
					    		
					    		Calendar currentDate = Calendar.getInstance(TimeZone.getTimeZone("GMT"));  		    				    	
		
					    		if( currentDate.compareTo(expirationDate)<0 )
					    		{
					    			//Extract related link title...
					    			SiteAsset asset = BusinessObjectFactory.createSiteAsset();
					    			
					    			if(cmsContentService!=null)
					    			{
					    				setIfNotNull(asset,"title",cmsContentService.getTitle(sitePage.asXML()));
					    			}
					    			else
					    			{
					    				setPageTitleForValidation(sitePage, asset);
					    			}
									
									urlTitle = (String)asset.get("title");
									
									//Ensure DEFAULT SITE TITLE is appended
									if((urlTitle==null) || (urlTitle.trim().length()<=0))
									{
										StringBuffer titleBuffer = new StringBuffer();
										titleBuffer.append("XXX.COM - ");
										titleBuffer.append(root.valueOf("@documentid"));
										
										urlTitle = titleBuffer.toString(); 
									}
					    		}
							}
						}
						else
						{
							//NO EXPIRATION DATE FOUND...EXTRACT RELATED LINK TITLE..
							SiteAsset asset = BusinessObjectFactory.createSiteAsset();							
							
							if(cmsContentService!=null)
			    			{
								setIfNotNull(asset,"title",cmsContentService.getTitle(sitePage.asXML()));
			    			}
			    			else
			    			{
			    				setPageTitleForValidation(sitePage, asset);
			    			}
							
							urlTitle = (String)asset.get("title"); 
							
							//Ensure DEFAULT SITE TITLE is appended
							if((urlTitle==null) || (urlTitle.trim().length()<=0))
							{
								StringBuffer titleBuffer = new StringBuffer();
								titleBuffer.append("XXX.COM - ");
								titleBuffer.append(root.valueOf("@documentid"));
								
								urlTitle = titleBuffer.toString(); 
							}
						}
					}
				}			
			}
		}
		catch(Exception ex)
		{			
			logger.error("Error in validating url", ex);
		}
		
		return urlTitle;
	}
	/**
	 * Responsible for retrieving Video asset from VCPS and extracting assets's title 
	 * @param videoId
	 * @return Video Asset title
	 */
	public String validateVideoURL(Long videoId){
		String title = null;		
		try{
			VideoAsset videoAsset=vcpsContentService.getVideo(videoId);
			title=(String)videoAsset.getMap().get("contentTitle");
			
		}catch(ServiceException ex){
			logger.error("validateVideoURL: Error while getting video title");			
		}
		
		return title;
	}
	
	protected void setPageTitleForValidation(Document sitePage, SiteAsset siteAsset) throws Exception
	{
		XPath xpathSelector = DocumentHelper.createXPath( "/root/Head/PageHeadline" );
		Element pageHeadlineNode= (Element)xpathSelector.selectSingleNode(sitePage);
		
		xpathSelector = DocumentHelper.createXPath( "/root/Head/MenuHeadline" );
		Element menuHeadlineNode= (Element)xpathSelector.selectSingleNode(sitePage);		
		
		xpathSelector = DocumentHelper.createXPath( "/root/Head/LinkHeadline" );
		Element linkHeadlineNode= (Element)xpathSelector.selectSingleNode(sitePage);

		xpathSelector = DocumentHelper.createXPath( "/root/Head/PageTitle" );
		Element pageTitleNode= (Element)xpathSelector.selectSingleNode(sitePage);
				
		Element titleNode = null;
		if(pageHeadlineNode!=null)
		{
			titleNode = pageHeadlineNode;			
		}
		else if(menuHeadlineNode!=null)
		{
			titleNode = menuHeadlineNode;
		}
		else if(linkHeadlineNode!=null)
		{
			titleNode = linkHeadlineNode;
		}
		else if(pageTitleNode!=null)
		{
			titleNode = pageTitleNode;
		}
		
		if(titleNode!=null)
		{
			if(asciiReplacementService != null){
				logger.debug("Replacing UTF char with ascii in title");
				siteAsset.set("title", asciiReplacementService.mapUTFtoAsciiCharacter(titleNode.getStringValue()));
			}
			else{
				logger.debug("NOT Replacing UTF char with ascii in title");
				siteAsset.set("title", titleNode.getStringValue());
			}

		}
	}
		
	protected int getPageSize(){
		return PAGE_SIZE;
	}
	
	protected void updateLastExecuteLogs(List<Object> content){				
		if(content!=null){
			SimpleDateFormat formatter 	= new SimpleDateFormat ("EEE, d MMM yyyy HH:mm a z");
			formatter.setTimeZone(TimeZone.getTimeZone("GMT"));

			if(lastExecutionLogs==null){
				lastExecutionLogs = new Vector<String>();								
				lastExecutionLogs.add(
						StringUtils.join(new Object[]{"LAST BATCH EXECUTION RUN [",
						formatter.format(Calendar.getInstance().getTime()),"]"}));
			}
			
			for(int i=0;i<content.size();++i){
				SiteAsset asset  = (SiteAsset)content.get( i ); 
	    		long pubdate = (asset.get("pubDate")!=null) ? new Long(asset.get("pubDate").toString()).longValue() : 0;
	    		String pubDate_fmt 	= formatter.format(new Date(pubdate));
	    		String id 			= (asset.get("ID")!=null) ? asset.get("ID").toString() : "";
	    		
	    		//LOG SYNDICATED DOCUMENT INFO// 
	    		lastExecutionLogs.add(StringUtils.join(
	    				new Object[]{"SYNDICATED DOCUMENT[",id,"]  PUBLISHED[",pubDate_fmt,"]"}));						
			}
		}
	}
	
	private String getFeedLocationId(String feedLocation)
	{
		String tokens [] = feedLocation.split("/");
		String locationId = null;
		for(int j=0; j<tokens.length;++j)
		{
			if(tokens[j].equalsIgnoreCase("id"))
			{
				locationId = tokens[j+1]; 
			}
		}
		return locationId;
	}
	
	protected List<Object> getContent() throws Exception{			
		
		List<Object> assets = new ArrayList<Object>();
		siteURLs =(contentIndex==0) ? parseSiteURLs() : siteURLs;
		if(siteURLs!=null){						
			String[] additionalDataArr = ((additionalData!=null) && (additionalData.length()>0)) ?  
					additionalData.split("\\^") : null;
			String[] idTitlesArr = ((idTitles!=null) && (idTitles.length()>0)) ?  
					idTitles.split("\\|"):  null;
			String[] relatedLinkTitlesArr = ((relatedLinkTitles!=null) && (relatedLinkTitles.length()>0)) ?  
					relatedLinkTitles.split("\\^"): null;
			int i=contentIndex;
			for( ;(i<siteURLs.size()) && (i<contentIndexUpperBound); ++i){
				SiteURL url = (SiteURL)siteURLs.get(i);
				try{
					Document sitePage =null;
					if((siteIDs!=null) 	&& 
					   (xml!=null) 		&& 
					   (xml.trim().length()>0) && 
					   (xml.indexOf("<XXX-global-syndication-response>")==-1) ){
						sitePage= getSourceDocument(xml);
					}
					else{
						String feedLocation  = getFeedLocationId(url.getURL());
						sitePage= ((feedLocation != null) && (feedLocation.trim().length() > 0)) ? getSitePage(url) : null;
					}
					
					//Parse Document and store data as appropriate..
					SiteAsset asset = createSiteAsset(url, 
													 sitePage,
													 i,
													 additionalDataArr, 
													 idTitlesArr,
													 relatedLinkTitlesArr);
					if(asset != null){
						assets.add(asset);
					}
					else{
						logger.error("Site asset is null for - " + url.getXMLURL());
					}
				}
				catch(Exception ex){
					logger.error(" Error while getting content", ex);
				}
			}
			contentIndex 		   = i;
			contentIndexUpperBound+= getPageSize();
		}
		return assets;
	}
	
	protected SiteAsset createSiteAsset(SiteURL url, 
										Document sitePage,
										int currentIndex,
										String [] additionalDataArr,
										String [] idTitlesArr,
										String [] relatedLinkTitlesArr) throws Exception 
	{
		SiteAsset asset = null;
		if(sitePage != null){
			asset = BusinessObjectFactory.createSiteAsset();
			String link = StringUtils.join(new Object[]{"http://",GVPProperties.getXXXLink(),"/id/",url.getURLID()});
			asset.set("link", 	link);
			asset.set("guid", 	link);
			asset.set("rawContent", sitePage);
			Map<String, Object> map = url.getAdditonalData();
			for (Iterator<String> mapIterator = map.keySet().iterator(); mapIterator.hasNext();){
				String addData = mapIterator.next();
				if(addData!=null && !addData.startsWith("cmsAdd")){
					asset.set(addData, (String)map.get(addData));
				}
			}
		
			setPubDate(url.getPubDate(), sitePage,asset);
			setPageExpiredDate(sitePage, asset);
			setPageKeywordTags(sitePage, asset);
			setPageMetaData(sitePage, asset);
			setPageTickerSymbols(sitePage, asset);
			setImageProperties(sitePage,asset);
			String pgXml = sitePage.asXML();
			asset.set("ID", String.valueOf(cmsContentService.getPageId(pgXml)));
			setIfNotNull(asset,"title",cmsContentService.getTitle(pgXml));
			setIfNotNull(asset,"description",cmsContentService.getDescription(pgXml));
			setIfNotNull(asset,"source",cmsContentService.getSource(pgXml));
			setIfNotNull(asset,"byline",cmsContentService.getByline(pgXml));
			setIfNotNull(asset,"layout",cmsContentService.getLayout(pgXml));
			setIfNotNull(asset,"prefix",cmsContentService.getStoryPrefix(pgXml));
			setIfNotNull(asset,"syndicateFlag",cmsContentService.getSyndicateFlag(pgXml));
			
			int i = currentIndex;
			if((additionalDataArr!=null) && (i<additionalDataArr.length) ){
				asset.set( "relatedLinks", additionalDataArr[i].trim());					
			}
			
			if((idTitlesArr!=null) && (i<idTitlesArr.length) ){
				String title = idTitlesArr[i].trim();
				if((title!=null) && (title.length()>0)){
					asset.set( "title",title );
				}
			}
			
			if((relatedLinkTitlesArr!=null) && (i<relatedLinkTitlesArr.length) ){
				asset.set( "relatedLinkTitles", relatedLinkTitlesArr[i].trim());
			}
			//Added to have action as part of the Content Restriction
			asset.set("action", (((action!=null) && (action.length()>0))? action.toUpperCase() : "MODIFY"));
			setAdditionalContent(asset);
		}	
		return asset;
	}
	
	private void setIfNotNull(SiteAsset asset, String fieldName, Object fieldValue) {
		if(fieldValue != null) asset.set(fieldName, fieldValue);
	}
	
	private void setPageExpiredDate(Document sitePage, SiteAsset siteAsset) throws Exception{
		Date convertedTime	= cmsContentService.getExpiredDate(sitePage.asXML());
		if(convertedTime!=null){		
			siteAsset.set("expiredDate", new Long(convertedTime.getTime()) );
		}	
	}

	protected void setAdditionalContent(Object bean) throws Exception{
		if((additionalContentKeys!=null) && (additionalContentKeys.size()>0) && (additionalContentService!=null)){
			SiteAsset srcAsset	= (SiteAsset)bean;									
			String ID = (String)srcAsset.get( "ID" );
	    	if((ID!=null) && (ID.trim().length()>0) ){	    		
	    		//Transfer Archived Content to submitted data asset...
	    		Set<Map.Entry<String, Map<String,String>>> entries = additionalContentKeys.entrySet();
    			for(Iterator<Map.Entry<String, Map<String,String>>> it = entries.iterator();it.hasNext();){
    				Map.Entry<String, Map<String,String>> entry = it.next();
    				String key = entry.getKey();
    				Map<String,String> keyMap = entry.getValue();
    				String field = StringUtils.join(new Object[]{
							keyMap.get("idprefix"),"'",ID.trim(),"'",keyMap.get("idsuffix"),keyMap.get("dbkey")
					});
					String content = additionalContentService.getField(new Long(ID),
															   		   field,
															   		   keyMap.get("dbnamespace"),
															   		   keyMap.get("dbnamespacepath"));	
					if((content!=null) && (content.trim().length()>0)){
						srcAsset.set( key , content );
					}
    				 
    			    //Apply Any Additional ASSET METADATA
    				applyAdditionalContentFromDynamicSource(ID, key, srcAsset);	    								    			
    			}
	    	}			
		}
	}
	
	private Document getSitePage(SiteURL url) throws DocumentException,
	   												 MalformedURLException,
	   												 IOException
	{
		if(url!=null)
		{
			if((action!=null) && action.equalsIgnoreCase("DELETE"))
			{
				String sUrl = StringUtils.join(new Object[]{"<root documentid=\"",url.getURLID(),"\" site=\"14081545\"></root>"});
				return getSourceDocument(sUrl);			
			}
			else
			{
				return getSourceDocument(url.getXMLURL());
			}
		}
		return null;
	}
	
	private void setPubDate(Date xmlDate, Document sitePage, SiteAsset siteAsset) throws Exception
	{
		if(xmlDate==null)
		{		
			Date convertedTime	= cmsContentService.getPubDate(sitePage.asXML());
			
			if(convertedTime!=null)
			{
				siteAsset.set("pubDate", new Long(convertedTime.getTime()) );
			}
		}
		else
		{
			siteAsset.set("pubDate",Long.valueOf(xmlDate.getTime()));
		}
	}
	
	private void setPageKeywordTags(Document sitePage, SiteAsset siteAsset) throws Exception
	{
		List<String> keywords = cmsContentService.getKeywords(sitePage.asXML());		
		
		if(keywords!=null)
		{
			StringBuffer keywordsBuffer 	= new StringBuffer();
			
			for(int i=0;i<keywords.size();++i)
			{		
				String keyword = (String)keywords.get(i);
				if(keyword!=null)
				{
					keywordsBuffer.append(keyword);
					keywordsBuffer.append(";");
				}
			}
			
			if(keywordsBuffer.length()>0)
			{
				siteAsset.set("keywords", keywordsBuffer.toString());
			}
		}
		
		List<String> keywordIds = cmsContentService.getKeywordids(sitePage.asXML());		
		
		if(keywordIds!=null)
		{
			StringBuffer keywordIdsBuffer 	= new StringBuffer();
			for(int i=0;i<keywordIds.size();++i)
			{		
				String keywordid = (String)keywordIds.get(i);
				if(keywordid!=null)
				{
					keywordIdsBuffer.append(keywordid);
					keywordIdsBuffer.append(";");
				}
			}
			
			if(keywordIdsBuffer.length()>0)
			{
				siteAsset.set("keywordids", keywordIdsBuffer.toString());
			}	
		}
	}
	
	private void setPageMetaData(Document sitePage, SiteAsset siteAsset) throws Exception
	{
		Hashtable<String, List<String>> metadata = cmsContentService.getMetadata(sitePage.asXML());
		
		if(metadata!=null)
		{		
			if(metadata.containsKey("topics"))
			{				
				List<String> values = metadata.get("topics");
				StringBuffer buffer = new StringBuffer();
				
				for(int i=0;i<values.size();++i)
				{		
					String value = (String)values.get(i);
					if(value!=null)
					{
						buffer.append(value);
						buffer.append(";");
					}
				}
				
				if(buffer.length()>0)
				{
					siteAsset.set("topics",buffer.toString());
				}
			}
			
			if(metadata.containsKey("sectors"))
			{
				List<String> values = metadata.get("sectors");
				StringBuffer buffer = new StringBuffer();
				
				for(int i=0;i<values.size();++i)
				{		
					String value = (String)values.get(i);
					if(value!=null)
					{
						buffer.append(value);
						buffer.append(";");
					}
				}
				
				if(buffer.length()>0)
				{
					siteAsset.set("sectors",buffer.toString());
				}				
			}
			
			if(metadata.containsKey("companies"))
			{
				List<String> values = metadata.get("companies");
				StringBuffer buffer = new StringBuffer();
				
				for(int i=0;i<values.size();++i)
				{		
					String value = (String)values.get(i);
					if(value!=null)
					{
						buffer.append(value);
						buffer.append(";");
					}
				}
				
				if(buffer.length()>0)
				{
					siteAsset.set("companies",buffer.toString());
				}			
			}
			
			if(metadata.containsKey("industries"))
			{
				List<String> values = metadata.get("industries");
				StringBuffer buffer = new StringBuffer();
				
				for(int i=0;i<values.size();++i)
				{		
					String value = (String)values.get(i);
					if(value!=null)
					{
						buffer.append(value);
						buffer.append(";");
					}
				}
				
				if(buffer.length()>0)
				{
					siteAsset.set("industries",buffer.toString());
				}
			}
			
			if(metadata.containsKey("tickersymbols"))
			{
				List<String> values = metadata.get("tickersymbols");
				StringBuffer buffer = new StringBuffer();
				
				for(int i=0;i<values.size();++i)
				{		
					String value = (String)values.get(i);
					if(value!=null)
					{
						buffer.append(value);
						buffer.append(";");
					}
				}
				
				if(buffer.length()>0)
				{
					siteAsset.set("tickersymbols",buffer.toString());
				}
				
			}					
		}					
	}
			
	private void setPageTickerSymbols(Document sitePage, SiteAsset siteAsset) throws Exception
	{
		List<String> tickersymbols = cmsContentService.getTickerSymbols(sitePage.asXML());
				
		if(tickersymbols!=null)
		{
			StringBuffer tickerBuffer 	= new StringBuffer();
			for(int i=0;i<tickersymbols.size();++i)
			{		
				String ticker = (String)tickersymbols.get(i);
				if(ticker!=null)
				{
					tickerBuffer.append(ticker);
					tickerBuffer.append(";");
				}
			}
			
			if(tickerBuffer.length()>0)
			{
				String currentTickersymbols = (String)siteAsset.get("tickersymbols");
				
				if((currentTickersymbols!=null) && (currentTickersymbols.length()>0))
				{
					tickerBuffer.append(currentTickersymbols);
				}
				
				siteAsset.set("tickersymbols",tickerBuffer.toString());
			}	
		}	
	}
	
	private void setImageProperties(Document sitePage, SiteAsset siteAsset) throws Exception
	{
		try
		{
		  if(null != cmsContentService.getImageProperties(sitePage.asXML()) && 
				  cmsContentService.getImageProperties(sitePage.asXML()).size() > 0 ){
			Map<String,String> imageProperties = cmsContentService.getImageProperties(sitePage.asXML()).get(0);
					
			if(imageProperties!=null)
			{
				String imageSource = imageProperties.containsKey(CMSContentService.IMAGE_SOURCE) ? 
						(String)imageProperties.get(CMSContentService.IMAGE_SOURCE) : null;
				if((imageSource != null) && (imageSource.trim().length() > 0))
				{
					siteAsset.set(CMSContentService.IMAGE_SOURCE, imageSource);
					String imageSourceOriginal = imageProperties.containsKey(CMSContentService.IMAGE_SOURCE_ORIGINAL) ? 
							(String)imageProperties.get(CMSContentService.IMAGE_SOURCE_ORIGINAL) : null;
					if((imageSourceOriginal != null) && (imageSourceOriginal.trim().length() > 0))
					{
						siteAsset.set(CMSContentService.IMAGE_SOURCE_ORIGINAL, imageSourceOriginal);
					}
					String imageText = imageProperties.containsKey(CMSContentService.IMAGE_TEXT) ? 
							(String)imageProperties.get(CMSContentService.IMAGE_TEXT) : null;
					if((imageText != null) && (imageText.trim().length() > 0))
					{
						siteAsset.set(CMSContentService.IMAGE_TEXT, imageText);
					}
					
					String imageCredit = imageProperties.containsKey(CMSContentService.IMAGE_CREDIT) ? 
							(String)imageProperties.get(CMSContentService.IMAGE_CREDIT) : null;
					if((imageCredit != null) && (imageCredit.trim().length() > 0))
					{
						siteAsset.set(CMSContentService.IMAGE_CREDIT, imageCredit);
					}
				}
			}
		}
		}
		catch(Exception ex)
		{
			logger.error("Error setting image properties.", ex);
		}
		
	}
	
	@SuppressWarnings("unchecked")
	private List<Document> getSiteXMLDocs() throws Exception{	
		List<Document> xmlDocs = new ArrayList<Document>();
 		if((sitemapIndex!=null) && (sitemapIndex.length()>0)){
			
			//Get All Documents from SitemapIndex (i.e.http://www.XXX.com/SitemapIndexXXX.xml)
			Document sourceDoc = getSourceDocument(sitemapIndex);
			if(sourceDoc!=null){					      			
				HashMap<String,String> uris = new HashMap<String,String>();
				uris.put( "ns", "http://www.sitemaps.org/schemas/sitemap/0.9");
				
				XPath xpathSelector = DocumentHelper.createXPath( "//ns:sitemap/ns:loc" );
				xpathSelector.setNamespaceURIs(uris);
				List nodeList = xpathSelector.selectNodes(sourceDoc);							
				for (Iterator iter = nodeList.iterator(); iter.hasNext(); ){
					try{
						Element elem = (Element) iter.next();								
						String xml = elem.getStringValue();
						logger.debug("SITEMAP INDEX DOCUMENT[" + xml + "]");
						Document xmlDoc = getSourceDocument(xml);
			            
			            if(xmlDoc!=null){
			            	xmlDocs.add(xmlDoc);
			            }
					}
					catch(Exception ex){
						logger.error(" Error in parsing Site xml", ex);
					}
				 }
				syndicateAll  = ((duration==0) ? true : false );		
			}					
		}
		else if((xml!=null) && (xml.length()>0)){
			
			//Get All Site Documents from XML (i.e. http://www.XXX.com/sitemapXXX0.xml )
			Document xmlDoc = getSourceDocument(xml);
            if(xmlDoc!=null){
            	xmlDocs.add(xmlDoc);
            }
		}
		return xmlDocs;		
	}
	
	@SuppressWarnings("unchecked")
	protected List<SiteURL> parseSiteURLs() throws Exception
	{
		List<Document> xmlDocs = getSiteXMLDocs();
		List<SiteURL> siteURLs = null; 
		
		if((siteIDs!=null)|| (xmlDocs!=null)){
			logger.debug("[SITEMAP SITE PARSE URLs START]");
			long start = System.currentTimeMillis();
			siteURLs = new ArrayList<SiteURL>();
			if((siteIDs!=null) && (siteIDs.trim().length()>0)){
				String[] arr = siteIDs.split(",");
				for (int i=0;i<arr.length;++i){		
					if(arr[i].trim().length()>0){
						String buffer = StringUtils.join(new Object[]{"http://",GVPProperties.getServerHome(),"/id/",arr[i].trim()});
						SiteURL url = new SiteURL();					
						url.setURL(buffer);
						url.setXMLURL(buffer);
						siteURLs.add(url);
					}
				}
			}
			else{
				if(xmlDocs!=null){
					for (int i=0;i<xmlDocs.size();++i){
						Document xmlDoc = xmlDocs.get(i);
						HashMap<String,String> uris = new HashMap<String,String>();
						uris.put( "ns", "http://www.sitemaps.org/schemas/sitemap/0.9");
						XPath xpathSelector = DocumentHelper.createXPath( "//ns:url" );
						xpathSelector.setNamespaceURIs(uris);
						List nodeList = xpathSelector.selectNodes(xmlDoc);
						
						for (Iterator iter = nodeList.iterator(); iter.hasNext(); ){
							Element elem = (Element) iter.next();
							elem.addNamespace( "ns","http://www.sitemaps.org/schemas/sitemap/0.9");
							
							SiteURL url = new SiteURL();					
							url.setURL(elem.valueOf("ns:loc"));								
							url.setPubDate(elem.valueOf("ns:lastmod"), "yyyy-MM-dd'T'HH:mm:ss");
							url.setXMLURL(elem.valueOf("ns:loc"));
		
			    	    	//Ensure syndicated content is properly filtered based on time..
							Calendar pubDate = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
							pubDate.setTime(url.getPubDate());
							
							Calendar minimumIndexDate = Calendar.getInstance(TimeZone.getTimeZone("GMT"));		    	    	
							minimumIndexDate.add(Calendar.HOUR_OF_DAY, ((duration==0) ? (-DEFAULT_DURATION) : (-duration)) );					
		
							//NOTE:: The SyndicateAll Flag is only applicable when explicitly set or when a syndication request is made using 						
							//		 the SiteMapIndex File and when duration == 0.
							
							//		 Standard syndication requests coming from the MSNBC Site-map builder POST will only syndicate content within a specified
							//		 duration range (DEFAULT: N hours) 
							if(syndicateAll || (pubDate.compareTo(minimumIndexDate)>=0)){	
								siteURLs.add(url);							
							}
					    }						
					}
				}
				
				//Ensure Site URLs are sorted by MOST RECENT(LAST UPDATED DATE)...
				if(siteURLs.size()>0){
					Collections.sort(siteURLs, new PubdateDescendingComparator() );
				}				
			}
					
			//--LOG PROCESSING TIME...
			long end 	= System.currentTimeMillis();	        
	        long elapsed= end - start;
	        
			StringBuffer logBuffer = new StringBuffer();
	        logBuffer.append("[SITEMAP SITE PARSE URLs END -- ELAPSED TIME] : ");
	        logBuffer.append(new Long((elapsed/3600000)%24)).append("-HOURS ");
	        logBuffer.append(new Long((elapsed/60000)%60)).append("-MINUTES ");
	        logBuffer.append(new Long((elapsed/1000)%60)).append("-SECONDS ");
	        logger.debug(logBuffer.toString());
		}	
		
        return siteURLs;
	}	
	
	protected Document getSourceDocument(String source) throws DocumentException,
															   MalformedURLException,
															   IOException
	{ 
		Document sourceDoc = null;
		if((source!=null) && (source.trim().length()>0)){
			String trimmedSource = source.trim(); 
			
			//Parse source to determine if it is native XML or url path
			if(!trimmedSource .startsWith("http")){
				sourceDoc = DocumentHelper.parseText(trimmedSource);
			}			
			else{
				//URL path submitted...
				String s = urlUtils.getURLContent(trimmedSource);
				sourceDoc = DocumentHelper.parseText(s);
			}
		}
		return sourceDoc;
	}
				
	protected Document createOutputXML(Object content, SyndicationPartner partner, 
			ExtendedDestinationInfo extendedDestInfo, Calendar syndDate){
		String currId = null;
		Document newDoc =null;
		try{	
			logger.debug("createOutputXML started ");
			
			//Create XML DOCUMENT of resultant video list.			
	        newDoc =  DocumentHelper.createDocument();
	        
	        //--ROOT ELEMENT
	        Element rootElement = newDoc.addElement("XXX-global-sitesearch-response");

	        //--ERROR ELEMENT
	        Element errorElement = rootElement.addElement("error-code");
	        errorElement.addText("0");

	        TimeZone tz = TimeZone.getTimeZone(getDateTimezone(extendedDestInfo));
	        
	        //--SYNDICATION DATE
	        SimpleDateFormat formatter 	= new SimpleDateFormat (getDateFormat(extendedDestInfo)); 
	        formatter.setTimeZone(tz); 
	        
	        Element yearElement = rootElement.addElement("currentYear");
	        yearElement.addText(String.valueOf(syndDate.get(Calendar.YEAR)));
	        
	        Element pubDateElement = rootElement.addElement("syndicationDate");
	        pubDateElement.addText(String.valueOf(syndDate.getTimeInMillis()));	        
	        
	        Element fmtPubDateElement = rootElement.addElement("syndicationDate_fmt");
	        fmtPubDateElement.addText(formatter.format(syndDate.getTime()));	        
	                
	        //--DATA ELEMENT
	        int pagesize = 1;
	        Element dataElement = rootElement.addElement("data");
	        dataElement.addAttribute("page", "1");	        
	        dataElement.addAttribute("hasnext", "false");
	        dataElement.addAttribute("hasprev", "false");
		 	
        	SiteAsset asset = BusinessObjectFactory.createSiteAsset();
		    BeanUtils.copyProperties(asset, (SiteAsset)content);
		    currId = String.valueOf(asset.get("ID"));
		    logger.debug("Processing asset : {}", currId);
        	
        	//Apply Any Additional ASSET METADATA...
		    applyAdditionalContentFromStaticSource(asset,extendedDestInfo);	
		    logger.debug("createOutputXML action {} ",action);
        	if((action!=null) && action.equalsIgnoreCase("DELETE")){
    			//Do not parse any additional information....
        		Element siteElement = dataElement.addElement("site-asset");
        		Element idElement = siteElement.addElement("ID");
		    	idElement.addText( (asset.get("ID")!=null) ? asset.get("ID").toString() : "" );

				//Add All Date Nodes...
				Calendar currentDate = Calendar.getInstance(TimeZone.getTimeZone("GMT"));  	
				currentDate.add(Calendar.DATE, -1);
				String currentDate_fmt= formatter.format(currentDate.getTime());
	    		siteElement.addElement("pubDate").addText(String.valueOf(currentDate.getTimeInMillis()));
	    		siteElement.addElement("pubDate_fmt").addText(currentDate_fmt);	
	    		siteElement.addElement("expiredDate").addText(String.valueOf(currentDate.getTimeInMillis()));
	    		siteElement.addElement("expiredDate_fmt").addText(currentDate_fmt);
	    		
	    		//Add Action NODE...		    		
		    	siteElement.addElement("action").addText(action.toUpperCase());
    		}
        	else{		        			        						
        		Element siteElement = dataElement.addElement("site-asset");
			    
			    DynaProperty[] keys = asset.getDynaClass().getDynaProperties();
			    for( int j=0; j <keys.length; ++j){				    	
			    	String keyName 			= keys[j].getName();
			    	if(keyName.equalsIgnoreCase("categories")){
			    		if(asset.get(keyName)!=null){
			    			String keyValue = (String)asset.get(keyName);
			    			String [] categories = keyValue.split(";");
			    			
			    			for(int k=0;k<categories.length;++k){
			    				Element propertyElement = siteElement.addElement("category");
			    				propertyElement.addText(categories[k]);				    				
			    			}
			    		}
			    	}				    	
			    	else if(keyName.equalsIgnoreCase("topics")){
			    		if(asset.get(keyName)!=null){
			    			String keyValue = (String)asset.get(keyName);
			    			String [] topics = keyValue.split(";");
			    			
			    			for(int k=0;k<topics.length;++k){
			    				Element propertyElement = siteElement.addElement("topic");
			    				propertyElement.addText(topics[k]);	
			    			}
			    		}				    		
			    	}
			    	else if(keyName.equalsIgnoreCase("sectors")){
			    		if(asset.get(keyName)!=null){
			    			String keyValue = (String)asset.get(keyName);
			    			String [] sectors = keyValue.split(";");
			    			
			    			for(int k=0;k<sectors.length;++k){
			    				Element propertyElement = siteElement.addElement("sector");
			    				propertyElement.addText(sectors[k]);	
			    			}
			    		}
			    	}
			    	else if(keyName.equalsIgnoreCase("companies")){
			    		if(asset.get(keyName)!=null){
			    			String keyValue = (String)asset.get(keyName);
			    			String [] companies = keyValue.split(";");
			    			
			    			for(int k=0;k<companies.length;++k){
			    				Element propertyElement = siteElement.addElement("company");
			    				propertyElement.addText(companies[k]);	
			    			}
			    		}
			    	}
			    	else if(keyName.equalsIgnoreCase("industries")){
			    		if(asset.get(keyName)!=null){
			    			String keyValue = (String)asset.get(keyName);
			    			String [] industries = keyValue.split(";");
			    			
			    			for(int k=0;k<industries.length;++k){
			    				Element propertyElement = siteElement.addElement("industry");
			    				propertyElement.addText(industries[k]);
			    			}
			    		}
			    	}
			    	else if(keyName.equalsIgnoreCase("tickersymbols")){
			    		if(asset.get(keyName)!=null){
			    			String keyValue = (String)asset.get(keyName);
			    			String [] symbols = keyValue.split(";");
			    			
			    			for(int k=0;k<symbols.length;++k){
			    				Element propertyElement = siteElement.addElement("tickersymbol");
			    				propertyElement.addText(symbols[k]);
			    			}
			    		}
			    	}
			    	else if(keyName.equalsIgnoreCase("relatedLinks")){
			    		if(asset.get(keyName)!=null){
			    			String keyValue = (String)asset.get(keyName);
			    			String [] links = keyValue.split(",");
			    			
			    			String linkTitles = (String)asset.get("relatedLinkTitles");
			    			String [] linkTitlesArr = ( 
			    					(linkTitles!=null) && (linkTitles.trim().length()>0) ) ? 
			    					linkTitles.split("\\|") : null;
			    			for(int k=0;k<links.length;++k){
			    				StringBuffer buffer = new StringBuffer();
			    				buffer.append(links[k]);
			    				buffer.append("|");
			    				
			    				//NOTE:: Take related link override if it exists...
			    				String title = ( (linkTitlesArr!=null) && (k<linkTitlesArr.length) ) ? 
			    						linkTitlesArr[k] :null;
			    				buffer.append( (((title!=null) && (title.trim().length()>0)) ? 
			    						title : validateSiteURL(links[k])) );	    				
			    				Element propertyElement = siteElement.addElement("relatedLink");
			    				propertyElement.addText(buffer.toString());
			    			}				    				
			    		}
			    	}
			    	else if(keyName.equalsIgnoreCase("title") ||
			    			keyName.equalsIgnoreCase("description") ){
			    		Element propertyElement = siteElement.addElement(keyName);
				    	propertyElement.addCDATA((asset.get(keyName)!=null) ? asset.get(keyName).toString() : "" );					    	
			    	}
			    	else if(keyName.equalsIgnoreCase("rawContent") & (asset.get("rawContent") != null)){
			    		//TODO() Consolidate every thing here and apply external xsl in future to create this fields...
			    		String pgXml=((Document)asset.get("rawContent")).asXML();
			    		String pageContentHTML =null;
			    		//this is source xml received from the cms - create following xml elements
			    		if(extendedDestInfo == null){
			    			pageContentHTML =  cmsContentService.getPageContentHTML(pgXml, true, false);	
			    		}
			    		else{
			    			pageContentHTML =  cmsContentService.getPageContentHTML(
		    						pgXml, true, extendedDestInfo.getContentResticitonArray("pageContentNodes"), extendedDestInfo.getSuppressXXXLinks());	
			    		}
						siteElement.addElement("pageContent").addCDATA((pageContentHTML!=null) ? pageContentHTML.trim() : "" );
						createChieldElementIfValueNotNull(siteElement, "seoUrl",cmsContentService.getSeoUrl(pgXml));
						createChieldElementIfValueNotNull(siteElement, "seoTitle",cmsContentService.getSeoTitle(pgXml));
						createChieldElementIfValueNotNull(siteElement, "contentType", cmsContentService.getContentType(pgXml));
						Date d = cmsContentService.getUpdateDate(pgXml);
						if(d!=null){
							createChieldElementIfValueNotNull(siteElement, "updateDate", d.getTime());
							createChieldElementIfValueNotNull(siteElement, "updateDate_fmt", formatter.format(d));
				    	}
			    	}
			    	else{
			    		siteElement.addElement(keyName).addText((asset.get(keyName)!=null) ? asset.get(keyName).toString() : "" );
				    	
				    	if(keyName.equalsIgnoreCase("pubDate")){
				    		long pubdate 		= (asset.get(keyName)!=null) ? new Long(asset.get(keyName).toString()).longValue() : 0;
				    		String pubDate_fmt 	= formatter.format(new Date(pubdate));
				    		siteElement.addElement("pubDate_fmt").addText(pubDate_fmt);
				    	}
				    	else if(keyName.equalsIgnoreCase("expiredDate")){
				    		long expireddate = (asset.get(keyName)!=null) ? new Long(asset.get(keyName).toString()).longValue() : 0; 
				    		siteElement.addElement("expiredDate_fmt").addText(formatter.format(new Date(expireddate)));
				    	}
			    	}
			    }	
			    siteElement.addElement("action").addText( ((action!=null) && (action.length()>0))? action.toUpperCase() : "MODIFY") ;
        	}				  
	        dataElement.addAttribute("pagesize",String.valueOf(pagesize));
	        dataElement.addAttribute("total", String.valueOf(pagesize));
		}
		catch(Exception ex){	
			logger.error("Error in creating output xml @ currId " + currId, ex); 
			StringBuffer buffer = new StringBuffer();
			try{
				buffer.append("HOST: ").append(InetAddress.getLocalHost().getHostName());
				buffer.append("  IP: " ).append(InetAddress.getLocalHost().getHostAddress());
			}
			catch(Exception e){}
			buffer.append("\nSYNDICATION TO PARTNER FAILED: ").append(partner.getpartner_name());
            buffer.append(" @" ).append(partner.getdestination_target());
            buffer.append(", possible story id:").append(currId);
            buffer.append("\n\n").append(ExceptionUtils.getExceptionStackTrace(ex));
			updateDBSyndicationLogs(partner, 
					buildStatusXML(currId,GlobalConstants.FAILED_STATUS,buffer.toString()),
									GlobalConstants.FAILED_STATUS, new Date(), currId);
		}
		logger.debug("create output returning null.." );
		return newDoc;
	}
	
	private void createChieldElementIfValueNotNull(Element parentElement,
			String chieldElementName, Object value) {
		if(value != null){
			parentElement.addElement(chieldElementName).addText(value.toString());		
		}
	}

	protected boolean isValidForSyndication(BaseDynaBean assetO, SyndicationPartner partner, ExtendedDestinationInfo extendedDestInfo) throws Exception
	{
		SiteAsset asset = BusinessObjectFactory.createSiteAsset();
	    BeanUtils.copyProperties( asset, (SiteAsset)assetO );
		boolean valid = true;
		String partnerType = partner.getsyndication_type();
		
		//Ensure there is a valid VIDEO ASSET...
		String ID = (String)asset.get("ID");
    	if((ID==null) || (ID.trim().length()==0) )
    	{
    		valid = false;
    	}
    	
    	//Proceed with remaining validation..
    	//NOTE:: When ACTION TYPE is DELETE only ASSET ID is required.  There is
    	//		 NO NEED to perform further content checks.
    	if((action!=null) && action.equalsIgnoreCase("DELETE"))
    	{
    		if(valid && (trackingService!=null))
    		{
	    		List<SyndicationTracking> deliveredContent = trackingService.getTrackingListForPartnerById(partner,ID.trim());
	    		
	    		//VALID FOR DELETION ONLY IF CONTENT HAS ALREADY BEEN DELIVERED TO PARTNER
				valid = ( (deliveredContent!=null) && (deliveredContent.size()>=1) );				
				
				if(!valid && writeembargologs)
				{
					updateDBSyndicationLogs(partner, 
											buildStatusXML(ID ,
														   GlobalConstants.REJECTED_STATUS, 
														   GlobalConstants.DELETE_CONTENT_EXCEPTION),
										    GlobalConstants.REJECTED_STATUS, new Date(), ID);
				}
    		}
    	}
    	else if(valid)
    	{
			//Determine if ASSET is valid for SUBMITTED PARTNER
    		//--CONTENT  VALIDATION
	    	if(partnerType.equalsIgnoreCase("CONTENT"))
	    	{
	    		if(extendedDestInfo!=null)
	    		{
		    		List<ContentValidator> contentSourceList = extendedDestInfo.getContentSource();
		    		
		    		if(contentSourceList!=null)
		    		{
			    		for(int i=0; i<contentSourceList.size();++i)
			    		{
			    			ContentValidator contentValidator = (ContentValidator)contentSourceList.get(i);
			    			
			    			if(!contentValidator.isValid(asset,trackingService))
			    			{
			    				valid = false;
			    				
			    				if(writeembargologs)
			    				{
			    					writeMatchTypeExceptionLogs(ID, partner, contentValidator);
			    				}
			    				
			    				break;
			    			}	    			
			    		}
		    		}
		    		logger.debug("partnerType CONTENT "+valid);
	    		}
    		}	    
	    	//Determine if ASSET is valid for SUBMITTED PARTNER--BASED ON MANUAL EXECUTION ONLY
	    	//--MANUAL EXECUTION VALIDATION
	    	else if(partnerType.equalsIgnoreCase("ONDEMAND"))
	    	{    			    	
	    		valid = ((siteIDs==null) || (siteIDs.trim().length()==0)) ? false : true;
	    		
	    		if(valid)
	    		{
	    			//PERFORM CONTENT BASED VALIDATION
	    			if(extendedDestInfo!=null)
		    		{
			    		List<ContentValidator> contentSourceList = extendedDestInfo.getContentSource();
			    		
			    		if(contentSourceList!=null)
			    		{	    		
				    		for(int i=0; i<contentSourceList.size();++i)
				    		{
				    			ContentValidator contentValidator = (ContentValidator)contentSourceList.get(i);	    			
				    			if(!contentValidator.isValid(asset,trackingService))
				    			{
				    				valid = false;
				    			
				    				if(writeembargologs)
				    				{
				    					writeMatchTypeExceptionLogs(ID, partner, contentValidator);
				    				}
				    				
				    				break;
				    			}	    			
				    		}
			    		}
		    		}
	    		}
	    	}
	    	
	    	//VALIDATE IF CONTENT HAS ALREADY BEEN SYNDICATED....
	    	//--DELIVERY DUPLICATION VALIDATION
	    	valid = ((valid) ? IsContentSyndicated(asset, partner, extendedDestInfo) : valid);
	    		    	
	    	//VALIDATE IF DELIVERY CAP HAS BEEN REACHED...
	    	//--DELIVERY CAP VALIDATION
	    	valid = ((valid) ? IsDeliveryCapPreserved(asset, partner, extendedDestInfo) : valid);
    	}
    	
    	return valid;
	}

	protected boolean IsContentSyndicated(SiteAsset asset, SyndicationPartner partner, ExtendedDestinationInfo extendedDestInfo) throws Exception
	{
		boolean valid  = true;
		
		if((asset!=null) && (partner!=null) && (trackingService!=null) && getCheckLastUpdate())
		{
			Long pubDateTime= (Long)asset.get("pubDate");	    
			String ID 		= (String)asset.get("ID");
        	if(pubDateTime!=null)
    		{
    			List<SyndicationTracking> deliveredContent = trackingService.getTrackingListForPartnerById(partner, ID.trim());	    			
    			
    			if((deliveredContent!=null) && (deliveredContent.size()>=CHECK_LAST_UPDT_PAGE_SIZE))		    		
	    		{		    							    		
    				SyndicationTracking sTracking = deliveredContent.get(0);
    				
    				//see if current record is deleted then try to get record which is not deleted
    				if(sTracking.getUpdateDate()!=null && IsDeletedEnum.DELIVERY_SUCCESS.getValue() != sTracking.getIsDeleted() 
    						&& deliveredContent.size() >= 2){
    					
    					sTracking = deliveredContent.get(1);
    					
        				//if delivery is completed then check update Date 
        				logger.debug("Moving to next record as current one is Deleted record ");
    				}

    				//if delivery is completed then check update Date 
    				logger.debug("Checking delivery isDeleted : " + sTracking.getIsDeleted() 
    						+  " &  update dt : " + sTracking.getUpdateDate() 
    						+ " for id : " + ID);
    				if(sTracking.getUpdateDate()!=null && IsDeletedEnum.DELIVERY_SUCCESS.getValue() == sTracking.getIsDeleted())
    				{
    					//Ensure time is normalized prior to comparison....
    					SimpleDateFormat formatter 	= new SimpleDateFormat ("EEE MMM d HH:mm:ss z yyyy");
    					String strPubDate 			= formatter.format(new Date(pubDateTime.longValue()));
    					String strTrackingDate 		= formatter.format(sTracking.getUpdateDate());
    					 
    					Date pubDate 		 = formatter.parse(strPubDate);
	    				Date trackingPubDate = formatter.parse(strTrackingDate);
    					 
	    				logger.debug("pubDate : {} &  trackingPubDate : {}" ,
	    						pubDate.getTime(), trackingPubDate.getTime());
    					if(pubDate.getTime()<=trackingPubDate.getTime())
    					{
    						logger.debug("Rejecting id : {} as alredy syndicated - pubdat is {}",
    								ID, pubDate);
    	    				
    						valid = false;
    						
    						if(writeembargologs)
		    				{
	    						updateDBSyndicationLogs(partner, buildStatusXML(ID ,
			    						GlobalConstants.REJECTED_STATUS, GlobalConstants.DUPLICATE_CONTENT_EXCEPTION),
			    						GlobalConstants.REJECTED_STATUS, new Date(), ID);
		    				}
    					}
    				}
	    		}
    		}        	
		}
		return valid;
	}
	
	protected boolean IsDeliveryCapPreserved(SiteAsset asset, SyndicationPartner partner, 
			ExtendedDestinationInfo extendedDestInfo) throws Exception{
		boolean valid  = true;
		if(extendedDestInfo!=null){
			List<Calendar> deliveryCapRange = getDeliveryCapDateRange(extendedDestInfo);
	    	
	    	if((deliveryCapRange!=null) && (asset!=null) && (partner!=null) && (trackingService!=null)){
	    		String ID 			  = (String)asset.get("ID");
	    		List<SyndicationTracking> deliveredContent = trackingService.getTrackingList(partner, 
	 					((Calendar)deliveryCapRange.get(0)).getTime(),
			 			((Calendar)deliveryCapRange.get(1)).getTime() );		    		
	    		if(deliveredContent!=null){	
	    			int deliveryCap = extendedDestInfo.getDeliveryCap();
	    			if(deliveredContent.size()>=deliveryCap){
		    			logger.debug("DELIVERY CAP HAS BEEN REACHED FOR PARTNER: " + partner.getpartner_name() );
		    			
		    			//If DELIVERY_CAP has been reached then only allow further updates/modifications to 
		    			//content that has been syndicated.
		    			valid = false;
		    			
		    			for (Iterator<SyndicationTracking> iter = deliveredContent.iterator(); iter.hasNext(); ){
		    				SyndicationTracking sTracking = iter.next();
		    				if(sTracking.getSyndicationUId()!=null && 
		    						sTracking.getSyndicationUId().equals(ID.trim())){
		    					valid = true;
		    					break;
		    				}
		    		    }
		    			
		    			if(!valid && writeembargologs){
		    				updateDBSyndicationLogs(partner, buildStatusXML(ID,
		    						GlobalConstants.REJECTED_STATUS, GlobalConstants.DELIVERY_RAECHED_EXCEPTION),
		    						GlobalConstants.REJECTED_STATUS, new Date(), ID);
		    			}
		    		}
	    		}
    		}        	
		}
		
		return valid;
	}
		
	protected void writeContentExceptionLogs(String partnerID, String IDs, Exception ex)
	{
		logger.debug("Writing log in db for partnerID: " + partnerID + ", IDs:" + IDs + ", ex:" + ex);
		try		
		{	
			String exception;
			if(ex!=null)
			{
				
				StringBuffer buffer = new StringBuffer();
				buffer.append("Submitted asset(s) are not available.  Exception: ");
				buffer.append( ExceptionUtils.getExceptionStackTrace(ex));
				exception = buffer.toString();
			}
			else
			{
				exception = SITEASSET_UNAVAILABLE_EXCEPTION;
			}
			
			if( (partnerID==null) || (partnerID.trim().length()==0) )
			{
				updateDBSyndicationLogs(getPartnerBySyndPartnerId(DEFAULT_PARTNERID), 
										buildStatusXML(IDs, GlobalConstants.REJECTED_STATUS, exception),GlobalConstants.REJECTED_STATUS, new Date(), IDs);
			}
			else
			{
				if ( (IDs!=null) && (IDs.trim().length()> 0) )
				{
					String partnerIDs [] = partnerID.split(",");
					for(int i=0;i<partnerIDs.length;i++)
					{
						if((partnerIDs[i].trim().length() > 0) && (partnerIDs[i] != null))
						{
							updateDBSyndicationLogs(getPartnerBySyndPartnerId(Long.parseLong(partnerIDs[i])), 
													buildStatusXML(IDs, GlobalConstants.REJECTED_STATUS, exception),GlobalConstants.REJECTED_STATUS, new Date(), IDs);
						}
					}
				}
			}
		}
		catch(Exception exception)
		{
			logger.error("Error in contents ", exception);
		}			
	}
	
	protected String getID(Object bean) {
		SiteAsset asset=(SiteAsset)bean;
		String ID = (String)asset.get("ID");
		return ID;
	}

	public String toString()
	 {	
		StringBuffer buffer = new StringBuffer();
		buffer.append(" (partnerID): ").append( ((partnerID!=null) && (partnerID.length()>0)) ? partnerID : "" );
		buffer.append(" (action): " ).append( ((action!=null) && (action.length()>0)) ? action : "MODIFY");
		buffer.append(" (IDs): " ).append( ((siteIDs!=null) && (siteIDs.length()>0)) ? siteIDs : "");
		buffer.append(" (siteMapIndex): " ).append( ((sitemapIndex!=null) && (sitemapIndex.length()>0)) ? sitemapIndex : "");
		buffer.append(" (XML): " ).append( ((xml!=null) && (xml.length()>0)) ? "XML POST submitted from XXX.COM Sitemapbuilder" : "");
		buffer.append(" (duration): ").append(new Integer(duration).toString());
		return buffer.toString();
	 }
	 	 
	public final static int DEFAULT_DURATION 	= 2;
	public final long PAGETIME_CONVERTVAL 		= 621355968000000000L;
	public final static int PAGE_SIZE			= 100;
	private final static String SITEASSET_UNAVAILABLE_EXCEPTION   =  "Submitted asset(s) are not available.  Please check asset IDs in Workbench and/or XXX.com.";

	public ContentService getVcpsContentService() {
		return vcpsContentService;
	}

	public void setVcpsContentService(ContentService vcpsContentService) {
		this.vcpsContentService = vcpsContentService;
	}
}

