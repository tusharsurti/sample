package XXX.synapi.tasks.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.XPath;

import XXX.synapi.businessobjects.SiteURL;
import XXX.synapi.businessobjects.SyndicationLocation;
import XXX.synapi.util.GVPProperties;

public class SyndicateSiteWireContentTaskObjectImpl  extends SyndicateSiteContentTaskObjectImpl
{
	private static Logger logger = Logger.getLogger( SyndicateSiteWireContentTaskObjectImpl.class );
	
	private List<Document> getSiteXMLDocs() throws Exception
    {
		List<Document> responseDocs = new ArrayList<Document>();
		List<SyndicationLocation> contentList	=  (locationsKey != null) ? adminContentService.getSyndicationLocationsByType(locationsKey.longValue()) : null;
		
		if(contentList != null)
		{
			for(int i=0; i< contentList.size();i++)
			{
				String feedLocation = ((SyndicationLocation)contentList.get(i)).getLocationUrl();
				try
				{
					Document wireDoc = getSourceDocument(feedLocation);
					if(wireDoc != null)
					{
						responseDocs.add(getSourceDocument(feedLocation));
					}
					else{
						logger.error("Document empty for location - " + feedLocation);
					}
				}
				catch(Exception ex)
				{
					logger.error("Error getting document for databox - " + feedLocation, ex);
				}
			}
		}
	
		return responseDocs;
    }

	@SuppressWarnings("unchecked")
	protected List<SiteURL> parseSiteURLs() throws Exception
    {
		List<Document> xmlDocs = getSiteXMLDocs();
		List<SiteURL> siteURLs = null; 

		if(xmlDocs!=null)
		{
			logger.debug("[SITEMAP SITE PARSE URLs START]");

			long start = System.currentTimeMillis();

			siteURLs = new ArrayList<SiteURL>();
			for (int i=0;i<xmlDocs.size();++i)
			{
				Document xmlDoc = (Document)xmlDocs.get(i);
					
				XPath xpathSelector = DocumentHelper.createXPath( "//page" );				
				List<Element> nodeList = xpathSelector.selectNodes(xmlDoc);

				for (Iterator<Element> iter = nodeList.iterator(); iter.hasNext(); )
				{
					Element elem = (Element) iter.next();
					String location = (elem.valueOf("@PageID")!=null) ? 
							StringUtils.join(new Object[]{"http://",GVPProperties.getServerHome(),"/id/",elem.valueOf("@PageID")}) :
							null;				
					SiteURL url = new SiteURL();					
					url.setURL(location);								
					url.setXMLURL(location);
					
					siteURLs.add(url);		
				}						
			}

			//--LOG PROCESSING TIME...
			long end 	= System.currentTimeMillis();	        
			long elapsed= end - start;

			StringBuffer logBuffer = new StringBuffer();
			logBuffer.append("[SITEMAP SITE PARSE URLs END -- ELAPSED TIME] : ");
			logBuffer.append(new Long((elapsed/3600000)%24));
			logBuffer.append("-HOURS ");
			
			logBuffer.append(new Long((elapsed/60000)%60));	        
			logBuffer.append("-MINUTES ");

			logBuffer.append(new Long((elapsed/1000)%60));
			logBuffer.append("-SECONDS ");

			logger.debug(logBuffer.toString());
		}	

		return siteURLs;
    }	
}