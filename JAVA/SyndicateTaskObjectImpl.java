package XXX.synapi.tasks.impl;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.Vector;
import java.util.concurrent.Future;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.beanutils.BasicDynaBean;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.binary.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.XPath;
import org.dom4j.io.DocumentResult;
import org.dom4j.io.DocumentSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import XXX.logger.mdc.MDCUtil;
import XXX.service.ApplicationConfigService;
import XXX.service.DBContentService;
import XXX.service.impl.IntegerTypeConverter;
import XXX.synapi.businessobjects.BaseDynaBean;
import XXX.synapi.businessobjects.ContentAppender;
import XXX.synapi.businessobjects.ContentValidator;
import XXX.synapi.businessobjects.ExtendedDestinationInfo;
import XXX.synapi.businessobjects.SyndicationLog;
import XXX.synapi.businessobjects.SyndicationPartner;
import XXX.synapi.businessobjects.SyndicationTracking;
import XXX.synapi.constants.GlobalConstants;
import XXX.synapi.constants.ModuleType;
import XXX.synapi.exception.ApplicationException;
import XXX.synapi.exception.AssetUnavailableException;
import XXX.synapi.exception.ServiceException;
import XXX.synapi.service.AdminContentService;
import XXX.synapi.service.LogService;
import XXX.synapi.service.TrackingService;
import XXX.synapi.tasks.TaskObject;
import XXX.synapi.util.ExceptionUtils;
import XXX.synapi.util.GVPProperties;
import XXX.synapi.util.ThreadStatusInfo;
import XXX.synapi.util.URLUtils;


/**
 * Syndication specific Task object ...
 * 
 */
public abstract class SyndicateTaskObjectImpl implements TaskObject
{
	private static Logger logger = LoggerFactory.getLogger( SyndicateTaskObjectImpl.class );
	
	
	private URLUtils urlUtils = new URLUtils();
	
	
	protected int  contentIndex							= 0;
	protected int  contentIndexUpperBound				= 0;
	
	/**
	 * The Log messages stored as Syndication request execution..
	 */
	protected List<String> lastExecutionLogs			= null;
	
	/**
	 * Multiple partner Id's separated by ,(comma) - in case 
	 * selective partner syndication needs to be done...
	 */
	protected String partnerID 							= null;
	protected String additionalData						= null;
	
	/**
	 * The Duration identifies the duration for which query for data 
	 * should be made
	 * 
	 * Mainly used by the jobs to determine how much back it should 
	 * go for pulling out data from source system...
	 */
	protected int duration								= 0;
	
	/**
	 * What type of syndication action is being made 
	 * i.e. if ADD: the new items syndication is taking place
	 * MODIFY : Existing item is being updated
	 * DELETE : Existing item is being removed...
	 * 
	 *  and processing becomes easy...
	 */
	protected String action								= "MODIFY";
	protected AdminContentService adminContentService 	= null;
	protected TrackingService trackingService			= null;
	protected LogService logService						= null;
	protected String idTitles						    = null;
	protected String relatedLinkTitles					= null;
	protected String IDs                                = null;
	private boolean checkLastUpdate					= true;
	protected boolean writeembargologs                  = false;
	protected String 		xml							= null;
	protected Map<String, Map<String,String>> additionalContentKeys = null;
	protected DBContentService additionalContentService = null;
	protected long moduleId= 0;
	
	protected String syndRequestor						= "";	
	Map<String,String> retryCountMap					= new HashMap<String, String>();

	public final static int  MAX_SIZE 					= 1;
	public final static int  CHECK_LAST_UPDT_PAGE_SIZE	= 1;
	public final static long DEFAULT_PARTNERID			= 3;	
	public final static long FILE_SIZE_COMPARE_BUFFER			= 100;	
	protected ApplicationConfigService applicationConfigService 	= null;
	static boolean monitorInstanceFlag=true;
	private static final Random duplicateCheckRandom = new Random();
	private long objCreationTimeStamp = System.currentTimeMillis();
	
	public long getObjCreationTimeStamp() {
		return objCreationTimeStamp;
	}

	public static enum IsDeletedEnum{
		YES('Y'), DELIVERY_SUCCESS('N'), DELIVERY_PENDING('-'), DELIVERY_FAILED('F');
		char deliveryValue;
		IsDeletedEnum(char deliveryValue){
			this.deliveryValue = deliveryValue;
		}
		public char getValue(){
			return deliveryValue;
		}
	}
	
	
	/**
	 * Create output XML from the content and partner selection
	 * 
	 * based on content type implementation object should return XML 
	 * string
	 * 
	 * @param content
	 * @param partner
	 * @param extendedDestInfo
	 * @return
	 */
	protected abstract Document createOutputXML(Object content, 
			SyndicationPartner partner, 
			ExtendedDestinationInfo extendedDestInfo,
			Calendar syndDate);
	

	/**
	 * Check if given asset is valid for Syndication or not
	 * 
	 * @param content
	 * @param partner
	 * @param extendedDestInfo
	 * @return
	 */
	protected abstract boolean isValidForSyndication(BaseDynaBean content, 
			SyndicationPartner partner, 
			ExtendedDestinationInfo extendedDestInfo) throws Exception ;
	
	
	/**
	 * Get Content to be syndicated - each list item represents unique content  
	 * to syndicate which varies from implementation to implementation...
	 * @return
	 * @throws Exception
	 */
	protected abstract List<Object> getContent() throws Exception;
		
	protected abstract int getPageSize();
	
	/**
	 * Implementor Update {@link #lastExecutionLogs} object based on content 
	 * processed and their status preserved within content...
	 * 
	 * @param content
	 */
	protected abstract void updateLastExecuteLogs(List<Object> content);
	
	protected abstract void writeContentExceptionLogs(String partnerID, String IDs, Exception ex);
		
	protected abstract String getID(Object bean);
	
	protected abstract void setAdditionalContent(Object bean) throws Exception;
		

	protected  SyndicationPartner getPartnerBySyndPartnerId(long syndPartnerId) throws Exception{
		return adminContentService.getSyndPartnerBySyndPartnerId(syndPartnerId);
	}

	protected  ExtendedDestinationInfo getPartnerInfoBySyndPartnerId(long syndPartnerId) throws ServiceException {
		return adminContentService.getSyndPartnerInfoBySyndPartnerId(syndPartnerId);
	}
	
	protected  String getPartnerXslBySyndPartnerId(long syndPartnerId) throws Exception{
		return adminContentService.getSyndPartnerXslBySyndPartnerId(syndPartnerId);
	}
	
	/**
	 * Return syndication partners having modulId passed ... 

	 * @param moduleId
	 * @return
	 * @throws Exception
	 */
	protected List<SyndicationPartner> getPartners(long moduleId) throws Exception
	{
		return adminContentService.getSyndicationPartnersByModule(moduleId);
	}
	
	public void setAction(String action)
	{
		this.action = action;
	}
	public String getAction()
	{
		return this.action;
	}
	
	public void setPartnerID(String partnerID)
	{
		this.partnerID = partnerID;
	}
	
	public String getPartnerID()
	{
		return partnerID;
	}
	
	public void setAdditionalData(String additionalData)
	{
		this.additionalData = additionalData;
	}
	
	public String getAdditionalData()
	{
		return additionalData;
	}
	
	public void setDuration(int duration)
	{
		this.duration = duration;
	}
	public int getDuration()
	{
		return duration;
	}
	
	public void setAdminContentService( AdminContentService adminContentService )
	{
		this.adminContentService = adminContentService;
	}
	
	public void setLogService(LogService logService) 
	{
		this.logService = logService;
	}
	
	public void setTrackingService(TrackingService trackingService) 
	{
		this.trackingService = trackingService;
	}
	
	public void setAdditionalContentService( DBContentService contentService )
	{
		this.additionalContentService = contentService;
	}
	
	public List<String> getLastExecutionLogs()
	{
		return lastExecutionLogs;
	}
	
	public String getIdTitles() {
		return idTitles;
	}

	public void setIdTitles(String idTitles) {
		this.idTitles = idTitles;
	}

	public String getRelatedLinkTitles() {
		return relatedLinkTitles;
	}

	public void setRelatedLinkTitles(String relatedLinkTitles) {
		this.relatedLinkTitles = relatedLinkTitles;
	}
	
	public boolean getCheckLastUpdate() {
		return checkLastUpdate;
	}

	public void setCheckLastUpdate(boolean checkLastUpdate) {
		this.checkLastUpdate = checkLastUpdate;
	}
	
	public void setXML(String xml) {
		this.xml = xml;
	}
	
	public String getXML() {
		return xml;
	}
	
	public void setAdditionalContentKeys(Map<String, Map<String,String>> content){
		this.additionalContentKeys = content;
	}
	
	public Map<String, Map<String,String>> getAdditionalContentKeys(){
		return additionalContentKeys;
	}
	
	public void setLastExecutionLogs(List<String> content)
	{
		if((content!=null) && (lastExecutionLogs!=null))
		{
			lastExecutionLogs.clear();
			lastExecutionLogs = new Vector<String>(content);
		}
	}
	
	private void initialize()
	{
		contentIndex 			= 0;
		
		//TODO(Munjal) Understand Why do we need this ???
		contentIndexUpperBound 	= getPageSize();
		
		if(lastExecutionLogs!=null){
			lastExecutionLogs.clear();
			lastExecutionLogs = null;
		}
	}
	
	private void writePendingLogs(String partnerID, String IDs )
	{
		try
		{
			if(writeembargologs 			&&
			   partnerID!=null              && 
			   partnerID.trim().length() > 0&&
			   IDs != null                  &&
			   IDs.trim().length()       > 0 )
			{
				String partnerIDs [] = partnerID.split(",");
				for(int i=0;i<partnerIDs.length;i++)
				{
					if(partnerIDs[i].trim().length() > 0 && partnerIDs[i] != null)
					{
						logger.debug("writePendingLogs  update DB logs for pending");
						updateDBSyndicationLogs(getPartnerBySyndPartnerId(Long.parseLong(partnerIDs[i])),
								                buildStatusXML(IDs , GlobalConstants.PENDING_STATUS, ""),
								                GlobalConstants.PENDING_STATUS, new Date(), IDs);
					}
				}
			}
		}
		catch(Exception ex)
		{
			logger.error("Error in writePendingLogs partnerID : " + partnerID + ", IDs:" + IDs, ex);
		}
			
	}	
	
	protected void writeMatchTypeExceptionLogs(String ID, SyndicationPartner partner, ContentValidator contentValidator)
	{
		try
		{
			StringBuffer matchTypeExceptionReason = new StringBuffer();
			
			matchTypeExceptionReason.append(GlobalConstants.MATCH_TYPE_EXCEPTION);
			matchTypeExceptionReason.append("( ");
			matchTypeExceptionReason.append(contentValidator.toString());
			matchTypeExceptionReason.append(" )");
			
			updateDBSyndicationLogs(partner, buildStatusXML(ID ,
					GlobalConstants.REJECTED_STATUS, matchTypeExceptionReason.toString()),
					GlobalConstants.REJECTED_STATUS, new Date(), ID);
		}
		catch(Exception ex)
		{
			logger.error("Error in writeMatchTypeExceptionLogs ID " + ID + ", partner :" + partner 
					+ ", ContentValidator " + contentValidator, ex);
		}
	}	
	
	/**
	 * Execute Ingestion listener logic...
	 */
	public List<Future<ThreadStatusInfo>> execute() throws ApplicationException
	{
		List<Future<ThreadStatusInfo>> executedJobs = new ArrayList<Future<ThreadStatusInfo>>();
		
		logger.debug("[SYNDICATE EXECUTE START] :");
	
		//Starting the monitor thread as a daemon
		if(monitorInstanceFlag) {
//			Thread monitor=new Thread(new ThreadMonitor(syndicateTaskExecutor));
//			logger.debug("Thread Monitoring instance created!! "+syndicateTaskExecutor.getActiveCount());
//			monitor.setDaemon(true);
//			monitor.start();
			monitorInstanceFlag=false;
		}
		
		long start = System.currentTimeMillis();	  
		try{			 
			 initialize();
			 
			 //Where do we get the partnerID? : PartnerID are selected in re-syndication UI 
			 //they are received in request notify.do action when that is sent by NotifyQueueAction of the UI
			 writePendingLogs(partnerID, IDs);
			 
			 //Get Content from our BACK-END Store
			 //The Type (class) of content in List - could be any thing - in video it's VideoAsset 
			 //From SyndicateVideoTaskObjectImpl & VCPS it looks like the content is 
			 //VideoAsset -> subclass of BaseDaynaBean  
			 List<Object> content = getContent();
			 
			 if((content==null) || (content.size()==0)){
				 logger.debug("Contenet is null or size = 0 -- here is object" + content);
				 writeContentExceptionLogs(partnerID, IDs, null);
			 }
			 
			 while((content!=null) && (content.size()>0) ){
				 logger.debug("Contenet size : " + content.size());
				 
				 //Ensure content is processed as a single UNIQUE LIST...
				 //using LinkedHashMap to maintain order
				 Map<String, Object> uniques = Collections.synchronizedMap(new LinkedHashMap<String, Object>());				
				 for(int i=0; i<content.size();++i){
					 logger.debug("Building unique Map by ID - processing : {}", i);
					 Object bean 	= (Object)content.get(i);
					 String ID  	= getID(bean);					
					 if(ID!=null){
						 uniques.put( ID.trim(), bean );
					 }				
				 }
				 
				logger.debug("uniques Map key size : " + uniques.keySet().size());
				 
				//Get All Relevant Partners from the Database and SYNDICATE as appropriate..				
				List<SyndicationPartner>   partnerList = getPartners(moduleId);
				
				logger.debug("partner -- size : {}, Request partnerID : {}", partnerList.size(), partnerID);
				for(int i=0; i<partnerList.size();++i){
					SyndicationPartner syndPartner = partnerList.get(i);
					
					//Default process Syndication - if partners are submitted
					//then only properly isolate all submitted partners...
					boolean processSyndication = true;
					
					//Check if the partner is marked for syndication in the 
					//request or not...
					if((partnerID!=null) && (partnerID.trim().length() > 0) ){
						boolean partnerMatched = false;
						String [] partnerIdArray = partnerID.split(",");
						for(int j=0;j<partnerIdArray.length;++j){
							String partnerIdStr = partnerIdArray[j];
							if((partnerIdStr !=null) && (partnerIdStr.trim().length()>0)){
								if(Integer.parseInt(partnerIdStr) == syndPartner.getId().intValue()){
									logger.debug("Syndication Requested for : {}", syndPartner.getId());
									partnerMatched = true;
									break;
								}
							}
						}
						//mark this partner id to be processed for syndication...
						processSyndication = partnerMatched;
					}
						
					if(!processSyndication){
						continue;
					}
					logger.debug(" processSyndication is true for partner   " + syndPartner.getId() );					
					
					Syndicator syndicator = new Syndicator(uniques.keySet(), new ArrayList<Object>(uniques.values()), syndPartner,this);
					executedJobs.add(GVPProperties.getSyndicateTaskExecutor(syndPartner).submit(syndicator));
					logger.debug("SYNDICATOR THREAD ADDED TO THE POOL FOR PARTNER {} ", syndPartner.getpartner_name());
				}
				updateLastExecuteLogs(new ArrayList<Object>(uniques.values()));
				content = getContent();
			 }
		}
		catch(Exception ex){
			writeContentExceptionLogs(partnerID, IDs, ex);
			logger.error("Error in execute partnerID : " + partnerID + ", IDs :" + IDs, ex);			
		    throw new ApplicationException( ex );
		}
		
		//--LOG PROCESSING TIME...
		long end = System.currentTimeMillis();	        
        long elapsed= end - start;
        
		StringBuffer logBuffer = new StringBuffer();
        logBuffer.append("[SYNDICATE EXECUTE END -- ELAPSED TIME] : ");
        logBuffer.append(new Long((elapsed/3600000)%24)).append("-HOURS ");
        logBuffer.append(new Long((elapsed/60000)%60)).append("-MINUTES ");
        logBuffer.append(new Long((elapsed/1000)%60)).append("-SECONDS ");
        logBuffer.append(" ").append(toString());
        
        logger.debug(logBuffer.toString());
        return executedJobs;
	}
	
	protected String getDateFormat(ExtendedDestinationInfo extendedDestInfo)
	{
		String dateFormat = "EEE, dd MMM yyyy HH:mm z";
		
		if(extendedDestInfo!=null)
		{
			String extendedDateFormat = extendedDestInfo.getDateFormat();
			if ( (extendedDateFormat!=null) && (extendedDateFormat.trim().length()>0) )
			{
				dateFormat = extendedDateFormat; 
			}
		}
		
		return dateFormat;		
	}
	
	protected String getDateTimezone(ExtendedDestinationInfo extendedDestInfo)
	{
		String dateTimezone = "GMT";
		
		if(extendedDestInfo!=null)
		{
			String extendedTimezone = extendedDestInfo.getDateTimezone();
			if ( (extendedTimezone !=null) && (extendedTimezone.trim().length()>0) )
			{
				dateTimezone = extendedTimezone; 
			}
		}
		
		return dateTimezone;		
	}
	
	protected int getContentLife(ExtendedDestinationInfo extendedDestInfo)
	{
		int contentLife = 0;
		
		if(extendedDestInfo!=null)
		{
			int extendedContentLife = extendedDestInfo.getContentLife();
			
			if(extendedDestInfo.getContentLife()>0)
			{
				contentLife = extendedContentLife;
			}
		}
		
		return contentLife;
	}
	
	/**
	 * Calculates date range for records based on configuration of Delivery Cap
	 * 
	 * and delivery frequency  - daily, yearly, weekly etc...
	 * @param extendedDestInfo
	 * @return
	 */
	protected List<Calendar> getDeliveryCapDateRange(ExtendedDestinationInfo extendedDestInfo)
	{
		List<Calendar> range = null;
		
		if(extendedDestInfo!=null)
		{	    		
    		int deliveryCap = extendedDestInfo.getDeliveryCap();
    		if( deliveryCap > 0 )
    		{    		
    			String deliveryFrequency = ((extendedDestInfo.getDeliveryCapFreq()==null) || 
    										(extendedDestInfo.getDeliveryCapFreq().trim().length()==0)) ? "daily" : 
    																					   				  extendedDestInfo.getDeliveryCapFreq();	
        		Calendar after = Calendar.getInstance(TimeZone.getTimeZone("GMT"));		
    		    Calendar before= Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    		    
        		if ( deliveryFrequency.equalsIgnoreCase( "hourly" ) )
        		{        			
        			//NORMALIZE DATE TO TOP OF THE HOUR...
        			after.set(Calendar.MINUTE, 0);
        			after.set(Calendar.SECOND, 0);
        			after.set(Calendar.MILLISECOND, 0);
        			
        			before.set(Calendar.MINUTE, 0);
        			before.set(Calendar.SECOND, 0);
        			before.set(Calendar.MILLISECOND, 0);
        			
        			//after.add(Calendar.HOUR_OF_DAY, -(value-1) );
        			before.add(Calendar.HOUR_OF_DAY, 1 );        			
        		}         		
        		else if (deliveryFrequency.equalsIgnoreCase( "weekly" ) )
        		{
        			//NORMALIZE DATE TO SUNDAY, MIDNIGHT...
        			after.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
        			after.set(Calendar.HOUR_OF_DAY , 0);
        			after.set(Calendar.MINUTE, 0);
        			after.set(Calendar.SECOND, 0);
        			after.set(Calendar.MILLISECOND, 0);
        		
        			before.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
        			before.set(Calendar.HOUR_OF_DAY , 0);
        			before.set(Calendar.MINUTE, 0);
        			before.set(Calendar.SECOND, 0);
        			before.set(Calendar.MILLISECOND, 0);
        			
        			//after.add(Calendar.WEEK_OF_YEAR, -(value-1) );
        			before.add(Calendar.WEEK_OF_YEAR, 1 );        			
        		}
        		else if( deliveryFrequency.equalsIgnoreCase("monthly"))
        		{
        			//NORMALIZE DATE TO FIRST DAY OF MONTH, MIDNIGHT...
        			after.set(Calendar.DAY_OF_MONTH, 1);
        			after.set(Calendar.HOUR_OF_DAY , 0);
        			after.set(Calendar.MINUTE, 0);
        			after.set(Calendar.SECOND, 0);
        			after.set(Calendar.MILLISECOND, 0);
        		
        			before.set(Calendar.DAY_OF_MONTH, 1);
        			before.set(Calendar.HOUR_OF_DAY , 0);
        			before.set(Calendar.MINUTE, 0);
        			before.set(Calendar.SECOND, 0);
        			before.set(Calendar.MILLISECOND, 0);
        			
        			//after.add(Calendar.MONTH, -(deliveryCap-1) );
        			before.add(Calendar.MONTH, 1 );
        			
        		}
        		else if (deliveryFrequency.equalsIgnoreCase("yearly"))
        		{	        			
        			//NORMALIZE DATE TO FIRST DAY OF YEAR, MIDNIGHT...
        			after.set(Calendar.DAY_OF_YEAR, 1);
        			after.set(Calendar.HOUR_OF_DAY , 0);
        			after.set(Calendar.MINUTE, 0);
        			after.set(Calendar.SECOND, 0);
        			after.set(Calendar.MILLISECOND, 0);
        		
        			before.set(Calendar.DAY_OF_YEAR, 1);
        			before.set(Calendar.HOUR_OF_DAY , 0);
        			before.set(Calendar.MINUTE, 0);
        			before.set(Calendar.SECOND, 0);
        			before.set(Calendar.MILLISECOND, 0);
        			
        			//after.add(Calendar.YEAR, -(deliveryCap-1) );
        			before.add(Calendar.YEAR, 1 );	        			
        		}
        		else
        		{
        			//DAILY FREQUENCY
        			//NORMALIZE DATE TO MIDNIGHT...
        			after.set(Calendar.HOUR_OF_DAY , 0);
        			after.set(Calendar.MINUTE, 0);
        			after.set(Calendar.SECOND, 0);
        			after.set(Calendar.MILLISECOND, 0);
        			
        			before.set(Calendar.HOUR_OF_DAY , 0);
        			before.set(Calendar.MINUTE, 0);
        			before.set(Calendar.SECOND, 0);
        			before.set(Calendar.MILLISECOND, 0);
        			
        			//after.add(Calendar.DATE, -(value-1) );
        			before.add(Calendar.DATE, 1 );	        		
        		}
        		
    			range = new ArrayList<Calendar>();
        		range.add(after);
        		range.add(before);
        		
        		logger.debug("CAP FREQUENCY RANGE START: " + after.getTime().toString() );
    			logger.debug("CAP FREQUENCY RANGE END: " + before.getTime().toString());	    				        			        			        
    		}
		}
		
		return range;
	}
	
	//TODO: Test case for 
	//	1)	null destination info
	//	2) 	different strategy
	private String getOutputFileName(SyndicationPartner partner, 
			ExtendedDestinationInfo extendedDestInfo, Document outputDoc) throws Exception {
		//Retrieving file name strategy to determine name of output xml
		String filenameStrategy = extendedDestInfo == null ? null : extendedDestInfo.getFilenameStrategy();
		StringBuffer outputFileNamePrefixBuffer = new StringBuffer();

		logger.debug("File name strategy partner ID::" + partner.getSyndPartnerId() + " is : " + filenameStrategy );
		if(filenameStrategy == null || filenameStrategy.equalsIgnoreCase("default")) {
			outputFileNamePrefixBuffer.append(partner.getpartner_name());
			outputFileNamePrefixBuffer.append("_");
			outputFileNamePrefixBuffer.append(partner.getId().toString());
			outputFileNamePrefixBuffer.append("_");
			
			String appender=getContentByField(extendedDestInfo,outputDoc);
			// IF THE APPENDER IS NOT CONFIGURED THE CURRENT TIME STAMP IS THE APPENDER
			outputFileNamePrefixBuffer.append((appender!=null)?appender
														:Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTimeInMillis()+"");
		}
		else if(filenameStrategy.equalsIgnoreCase("fixed")){
			outputFileNamePrefixBuffer.append(extendedDestInfo.getFilename());
			logger.debug("outputFileNamePrefixBuffer::" + outputFileNamePrefixBuffer);
		}
		else if(filenameStrategy.equalsIgnoreCase("xsl_expr")){
	        String xsl = "<xsl:stylesheet version=\"2.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\"><xsl:output method=\"text\" omit-xml-declaration=\"yes\" indent=\"no\"/><xsl:template match=\"/\">{xslExpr}</xsl:template></xsl:stylesheet>";
	        String xslExpr = extendedDestInfo.getFilename();
	        xsl = xsl.replace("{xslExpr}", xslExpr);
			StreamSource xsltFile = new StreamSource(new StringReader(xsl));
			Transformer xsltTransformer = TransformerFactory.newInstance().newTransformer(xsltFile);
			DocumentSource src			= new DocumentSource(outputDoc);		    
		    
			StringWriter strWriter= new StringWriter();
			StreamResult dest	= new StreamResult(strWriter);
			xsltTransformer.transform(src, dest);

			outputFileNamePrefixBuffer.append(strWriter.toString());
			logger.debug("outputFileNamePrefixBuffer::" + outputFileNamePrefixBuffer);
		}
		else {
			logger.error("Incorrect configuration for filename-strategy for partner ID::" + partner.getSyndPartnerId() + " and Name:: " + partner.getpartner_name() + ". Expected null, default or fixed file name strategy. Found :: " + filenameStrategy);
			throw new Exception("Incorrect configuration for filename-strategy. Expected null, default or fixed file name strategy. Found :: " + filenameStrategy);
		}
			
		StringBuffer outputFileName = new StringBuffer();
		outputFileName.append(outputFileNamePrefixBuffer.toString());			
		if(!outputFileName.toString().toLowerCase().contains(".xml")) {
			outputFileName.append(".xml");
		}
		logger.debug("outputFileName::" + outputFileName);
		return outputFileName.toString();
	}

	/**
	 * Transform the XXX Syndication XML format to Partner format
	 */
	private Document transformToPartnerFormat(SyndicationPartner partner, Document src) throws Exception{
		String xsl = getPartnerXslBySyndPartnerId(partner.getId().longValue());
		//logger.debug("Transforming doc with xsl {} ", xsl);
		StreamSource xsltFile = (xsl!=null) ? new StreamSource(new StringReader(xsl.trim())) : null;
		//logger.debug("xsltFile {} ", xsltFile);
		TransformerFactory xslFactory 	= TransformerFactory.newInstance();
		//logger.debug("xslFactory {} ", xslFactory);
		Transformer xsltTransformer 	= (xsltFile!=null) ? xslFactory.newTransformer(xsltFile) :
													   		 xslFactory.newTransformer();
		logger.debug("xsltTransformer {} ", xsltTransformer);
		DocumentResult dest = new DocumentResult();
		xsltTransformer.transform(new DocumentSource(src), dest);
		return dest.getDocument();
	}
	
	public void sendPartnerConfirmation(String xmlFileName,
										 String credentials,
										 ExtendedDestinationInfo extendedDestInfo) throws Exception
	{
		if(extendedDestInfo!=null)
		{
			String confirmationURL = extendedDestInfo.getConfirmationURL();
			if((confirmationURL!=null) && (confirmationURL.trim().length()>0))
			{
				StringBuffer buffer = new StringBuffer();
				
				buffer.append(confirmationURL.trim());
				buffer.append(URLEncoder.encode(xmlFileName.trim(),"UTF-8"));
				buffer.append((credentials!=null)? credentials.trim() : "" );
				logger.debug("SENDING CONFIRMATION URL: " + buffer.toString());
				
				HttpURLConnection urlConnection =  urlUtils.getURLConnection(new URL(buffer.toString()));
				urlConnection.setUseCaches(false);
				urlConnection.setDoInput(true);
				urlConnection.setRequestProperty("Content-Type","application/x-www-form-urlencoded");

				//MAKE GET REQUEST...
				BufferedReader in = new BufferedReader(new InputStreamReader(urlConnection.getInputStream()));
				in.close();
			}
		}
	}
	
	private void trackContent(SyndicationPartner partner,
							  String contentID,
							  Long contentPubDate,										   
							  String contentFormat,
							  IsDeletedEnum deliveryStatus,
							  String contentHash) throws Exception
	{
		if(contentID!=null){
			if ((action!=null) && action.equalsIgnoreCase("DELETE")){
				updateDBSyndicationTracking(partner,contentID.trim(), 
						((deliveryStatus == IsDeletedEnum.DELIVERY_SUCCESS) ? IsDeletedEnum.YES : deliveryStatus ), null, null, contentHash);
			}
			else{
				updateDBSyndicationTracking(partner,contentID.trim(), deliveryStatus, 
						(contentPubDate == null ? null : new Date(contentPubDate)),contentFormat,contentHash);
			}
		}
	}

	/**
	 * Method to get the field value from outputXML given the filed Name. 
	 * It fethces the first entry alone, which is currently used for the fileName appender
	 * @param fieldName
	 * @param outputXML
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public String getContentByField(ExtendedDestinationInfo extendedDestInfo,Document outputDoc){
		String content=null,fieldName = null;
		if(extendedDestInfo!=null){
			fieldName=extendedDestInfo.getFilenameAppender();
		}
		
		if(fieldName!=null && fieldName.length()>0){
			try{
				//Parse IDs to be Syndicated...
				StringBuffer xpath=new StringBuffer("//");
				xpath.append(fieldName);
		        XPath 	 xpathSelector 	= DocumentHelper.createXPath( xpath.toString() );
				List 	 contentList		= xpathSelector.selectNodes(outputDoc);
				
				if(contentList!=null && contentList.size()>0){
					//GET THE FIRST ELEMENT ONLY
					Element elem = (Element)contentList.get(0);					
					content=elem.getStringValue();
					if(content!=null && content.trim().length()>0){
						return content;
					}
				}						
			}
			catch(Exception ex){				
				logger.error("Exception in getContentByField extendedDestInfo: "+ 
						extendedDestInfo +", outputXML:" + outputDoc.asXML(), ex);
			}	
		}
		return content;
	}
	
	@SuppressWarnings("unchecked")
	private String getSyndicatedContentFormats(Document outputDoc)
	{
		String contentFormats = null;
		
		try
		{
			//Parse IDs to be Syndicated...
	        XPath 	 xpathSelector 	= DocumentHelper.createXPath( "//video-asset" );
			List 	 content		= xpathSelector.selectNodes(outputDoc);
			if(content!=null)
			{
				Element elem = (Element)content.get(0);
				List formats = elem.elements("additionalFormat");
				
				StringBuffer fmtBuffer = new StringBuffer();
				
				for(int j=0;j<formats.size();)
				{			
					Element format = (Element)formats.get(j);
					String formatValue = format.getStringValue();
					
					if((formatValue!=null) && (formatValue.indexOf("Streaming")==-1))
                    {
						fmtBuffer.append(formatValue);
						fmtBuffer.append( (++j<formats.size()) ? ";" : "" );
                    }
					else
					{
						++j;
					}						
				}
				contentFormats = fmtBuffer.toString();
			}						
		}
		catch(Exception ex)
		{	
			logger.error("Error getSyndicatedContentFormats outputXML:" + outputDoc.asXML(),ex);
		}	
		return (contentFormats!=null) ? contentFormats.replace(";;", "").trim() : null;
	}
	
	public String getContentURL(String outputXML, Map<String,String> content)
	{
		StringBuffer contentURL = new StringBuffer();
		
		if(content!=null)
		{
			try
			{
				StringBuffer destinationBuffer	= new StringBuffer();
				for (Iterator<String> it = content.values().iterator(); it.hasNext();)
				{
					destinationBuffer.append((String)it.next());					
					destinationBuffer.append((it.hasNext()) ? "_" : "");
				}
				
				if(destinationBuffer.toString().length()>0)
				{
					Document outputDoc		= DocumentHelper.parseText(outputXML);
				    XPath 	 xpathSelector 	= DocumentHelper.createXPath( "//additionalProtectedURL" );
				    @SuppressWarnings("unchecked")
				    List 	 formatURLs		= xpathSelector.selectNodes(outputDoc);
					
					if(formatURLs!=null)
					{
						for(int i=0;i<formatURLs.size();++i)
						{
							Element elem= (Element)formatURLs.get(i);					
				    		
				    		String url	= elem.getStringValue();
				    		String [] formatTokens = url.split("_");
				    		
				    		if(formatTokens.length>=2)
				    		{				    		
								//Replace current bitrate value with Rounded Instance...
								long bitrate = Long.valueOf(formatTokens[1]).longValue();							   		     
								long roundedBitrate = Math.round(bitrate/100000.0) * 100000;
								     
								formatTokens[1] = String.valueOf(roundedBitrate);
				    		}
				    		
				    		//RE-ASSEMBLE STRING....
				    		StringBuffer formatBuffer = new StringBuffer();
				    		for(int j=0; j<formatTokens.length;)
				    		{
				    			formatBuffer.append(formatTokens[j]);
				    			formatBuffer.append((++j<formatTokens.length) ? "_" : "");
				    		}
				    		
				    		
					    	//Check For Content Match and Extract URL..
					    	if(formatBuffer.indexOf(destinationBuffer.toString()) != -1)
				    		{
				    			//MATCH FOUND -- STOP ITERATION..
				    			String [] urlTokens = url.split("\\|");
				    			if(urlTokens.length>=2)
				    			{
				    				contentURL.append( urlTokens[1] );
				    				contentURL.append(":");				    				
				    				contentURL.append( String.valueOf(Long.valueOf(formatTokens[1]).longValue()/1000) );
				    			}				  
			    				break;
				    		}
						}						
					}	
				}
			}
			catch(Exception ex)
			{				
				logger.error("Error getContentURL outputXML"+ outputXML+", content" + content,ex);
			}	
		}
		
		return contentURL.toString();
	}
	
	
	protected Document buildStatusXML(String IDs , String status, String rejectReason)
	{
        //Create XML DOCUMENT.			
		Document newDoc =  DocumentHelper.createDocument();
		try{		
	        //--ROOT ELEMENT
	        Element rootElement = newDoc.addElement("XXX-logs");
	        rootElement.addElement("XXXdblogcontentids").addText((IDs!=null) ? IDs.toString() : "" );
	        rootElement.addElement("status").addText( (status!=null) ? status : "");
	        rootElement.addElement("syndRequestor").addText( (syndRequestor!=null) ? getSyndRequestor() : "");
	        String mdcId = MDCUtil.getRequestIdFromMDC();
	        mdcId = mdcId == null ? "Missing" : mdcId;
	        rootElement.addElement("mdcId").addText(mdcId);
	        
	        // added retry details element contain each Id with retry count
	        if(null != getSyndRequestor() && getSyndRequestor().contains("retry")){	        	
	        	addRetryDetails(rootElement,IDs);
	        }
	        
	        if((rejectReason != null) && (rejectReason.trim().length() > 0) ){
	        	rootElement.addElement("reason").addText(rejectReason);
	        }
	        rootElement.addElement("host").addText( InetAddress.getLocalHost().getHostName());
	        rootElement.addElement("IP").addText( InetAddress.getLocalHost().getHostAddress());
	        
	        //logger.debug("BUILD STATUS XML " + newDoc.asXML());
		}
		catch(Exception ex){				
			logger.error("Error while buildStatusXML", ex);
		}
		return newDoc;
	}
	
	/**
	 * Does what {@link #buildStatusXML(String, String, String)} - adding outputDoc under Request Element   
	 * @param iD
	 * @param failedStatus
	 * @param rejectReason
	 * @param outputDoc
	 * @return
	 */
	private Document buildStatusXML(String iD, String failedStatus, String rejectReason, Document outputDoc) {
		Document doc = buildStatusXML(iD, failedStatus, rejectReason);
		doc.getRootElement().addElement("Request").add(outputDoc.getRootElement());
		return doc;
	}
	
	/**
	 * add syndication retry tag in status xml 
	 *  eg.
	 *  <retryDetails>	
	 *    	<syndAttemps id=345645566>2</syndAttemps> 		
	 * 		<syndAttemps id=345689654>1</syndAttemps>
	 * 	<retryDetails>
	 */
	public void addRetryDetails(Element rootElement, String IDs){
		Element retryElement =rootElement.addElement("retryDetails");
		Set<String> idSet =null;
    	if(null != IDs && IDs.length() > 0){ 
    		String []syndicateIdArray=IDs.split(",");
    		if(null != syndicateIdArray && syndicateIdArray.length > 0)
    			idSet = new HashSet<String>(Arrays.asList(syndicateIdArray));
		}
    	
    	Map<String, String> retryCntMap=getRetryCountMap();	        	
    	for(String id: retryCntMap.keySet()){	        		
    		if(null != idSet){
    			if(idSet.contains(id)){    				
    				Element syndAttempsElement=retryElement.addElement("syndAttemps");
    				syndAttempsElement.addAttribute("id", id);
    				syndAttempsElement.addText(retryCntMap.get(id).toString());
    			}
    		}
    	}
	}
	
	private Document getAdditionalContentDocument()
	{
		try
		{
			if((xml!=null) && (xml.trim().length()>0) && !xml.startsWith("http"))
			{
				return DocumentHelper.parseText(xml.trim());
			}			
		}
		catch(Exception ex)
		{				
			logger.error("Error getAdditionalContentDocument ", ex);
		}
		
		return null;
	}
	

	/**
	 * TODO(Munjal) Checkout this method logic what it does ??? 
	 * @param bean
	 * @param extendedDestInfo
	 * @throws Exception
	 */
	protected void applyAdditionalContentFromStaticSource(BasicDynaBean bean, ExtendedDestinationInfo extendedDestInfo) throws Exception
	{
		if(extendedDestInfo!=null)
		{
			List<ContentAppender> additionalContentSourceList = extendedDestInfo.getAdditionalContentSource();
			if(additionalContentSourceList!=null)
			{		
	    		for(int i=0; i<additionalContentSourceList.size();++i)
	    		{
	    			ContentAppender contentAppender = (ContentAppender)additionalContentSourceList.get(i);
	    			contentAppender.apply(bean, trackingService);	    				    		
	    		}
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	protected void applyAdditionalContentFromDynamicSource(String ID, String key, BasicDynaBean bean)
	{
		logger.debug("applyAdditionalContentFromDynamicSource " +  ID );
		try
		{
			if((ID!=null) && (key!=null) && (bean!=null))
			{
				Document additionalContent = getAdditionalContentDocument();
						
				if(additionalContent!=null)
				{
					String assetXPath = org.apache.commons.lang.StringUtils.join(
							new Object[]{"/XXX-global-syndication-response/data//syndication-asset[ID='",ID,"']"});
					
					logger.debug("Parsing additional document for asset: " + assetXPath);				
					
					//Parse syndication asset from  submitted XML Document
					XPath xpathSelector = DocumentHelper.createXPath(assetXPath);
					Element xmlAsset	= (Element)xpathSelector.selectSingleNode(additionalContent);			
			
					if(xmlAsset!=null)
					{
						List contentElements = xmlAsset.elements(key);
						
						//if element is not present then skip update to bean
						if(contentElements!=null && contentElements.size()>0)
						{
							logger.debug("Updating additional content Key : " + key);
							StringBuffer buffer = new StringBuffer();		
							
							for(int i=0;i<contentElements.size();++i)
							{
								Element contentElement = (Element)contentElements.get(i);
							
								if(contentElement.isTextOnly())
								{
									logger.debug("(Text Only): Parsing additional node ["+key+"] : (" + contentElement.getName() + ")");
									buffer.append(contentElement.getStringValue());
									
									if((i+1)<contentElements.size())
									{
										buffer.append(" ");
									}									
								}
								else
								{						
									logger.debug("(Complex Node): Parsing additional node ["+key+"] : (" + contentElement.getName() + ")");
									List childNodes = contentElement.elements();
									for(Iterator it = childNodes.iterator();it.hasNext();)
									{
										Element child = (Element)it.next();
										buffer.append(child.asXML());
									}
								}
							}							
							
							bean.set( key , buffer.toString().trim() );
						}
						else{
							logger.debug("Skipping update for additional content Key : " + key);
						}
					}
				}
			}
		}
		catch(Exception ex)
		{	
			logger.error("Error applyAdditionalContentFromDynamicSource ID"+ID+", key"+key+", bean :" + bean, ex);
		}		
	}
	
	protected String getIDs(List<Object> dynaBeanList) {
		StringBuffer idbuf=new StringBuffer();
		if(dynaBeanList!=null){
			int size=dynaBeanList.size();
			for(int index=0;index<size;index++){
				idbuf.append(getID(dynaBeanList.get(index)));
				if(index<size-1){
					idbuf.append(",");
				}
			}
		}
		return idbuf.toString();
	} 
	
	/** Helper class that manages syndicating all exportable data (CONCURRENTLY)*/
	
	public String getIDs() {
		return IDs;
	}

	public void setIDs(String ds) {
		IDs = ds;
	}

	public boolean isWriteembargologs() {
		return writeembargologs;
	}

	/**
	 * Writes transaction logs for each steps e.g. pending, failed , success etc...
	 * allowing to investigate any potential issue with syndication...
	 * 
	 * @param writeembargologs
	 */
	public void setWriteembargologs(boolean writeembargologs) {
		this.writeembargologs = writeembargologs;
	}
	
	protected void updateDBSyndicationLogs(SyndicationPartner partner, 
			Document outputDoc, 
			String status, 
			Date syndDate,
			String contentId)
	{
		
		try
		{
			if(logService!=null)
			{
				SyndicationLog slog = new SyndicationLog();
				slog.setSyndDate(syndDate);
				slog.setSyndPartnerId(partner.getSyndPartnerId().longValue());
				slog.setData(outputDoc!= null ? outputDoc.asXML() : null);
				slog.setStatus(status);
				slog.setContentId(contentId);
				logService.createSyndicationLog(slog);
			}
		}
		catch(Exception ex)
		{				
			logger.error("Error in updateDBSyndicationLogs partner: " + partner 
					+ ", outputXML: " + outputDoc.asXML()
					+ ", status: " + status 
					+ ", syndDate: "+ syndDate 
					+ ", contentId: "+ contentId, ex);		
		}
	}

	private void updateDBSyndicationTracking(SyndicationPartner partner, String contentId,  IsDeletedEnum isDeleted, Date contentUpdateDate, String format, String contentHash)
	{
		try
		{
			if(trackingService!=null)
			{
				logger.debug("Tracking for partner : {} , asset : {} , isDeleted: {}, contentUpdateDate: {}, format: {}",
						new Object []{
							partner.getSyndPartnerId(), 
							contentId,
							isDeleted,
							contentUpdateDate,
							format});
				 
				SyndicationTracking   sTracking = new SyndicationTracking ();
		     
				sTracking.setSyndDate(Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTime());
				sTracking.setSyndPartnerId(partner.getSyndPartnerId());
				sTracking.setSyndicationUId(contentId);
				sTracking.setIsDeleted(isDeleted.getValue());
				sTracking.setUpdateDate(contentUpdateDate);
				sTracking.setSyndFormat(format);
				sTracking.setContentHash(contentHash);
				trackingService.createSyndicationTracker(sTracking);
			}
		}
		catch(Exception ex)
		{
			logger.error("Error in updateDBSyndicationTracking", ex);		
		}
	}

	public void setModuleId(long moduleId) {
		this.moduleId = moduleId;
	}

	public long getModuleId() {
		return moduleId;
	}
	
	/**
	 * Returns FTP Details for given asset format and bitrate. To be overridden in subclass for proper implementation.
	 * 
	 * @param format Asset Format WMV, FLV, MPEG4 etc.
	 * @param bitrate Bitrate for given asset format
	 * @param thumbnailSize Size parameter is required in case of thumbnails small and large as both will have same format "jpeg" and bitrate "0"
	 * @return String containing FTP URL for content with given format, bitrate and thumbnailSize
	 * @throws Exception
	 */
	public String getFTPDetails(String format, String bitrate, String thumbnailSize) throws AssetUnavailableException,Exception {
		logger.error("Default implementation of getFTPDetails(). It should ideally not be called instead a subclass implementation be called.");		
		return null;
	}

	public String getSyndRequestor() {
		return syndRequestor;
	}

	public void setSyndRequestor(String syndRequestor) {
		this.syndRequestor = syndRequestor;
	}

	public Map<String, String> getRetryCountMap() {
		return retryCountMap;
	}

	public void setRetryCountMap(Map<String, String> retryCountMap) {
		this.retryCountMap = retryCountMap;
	}
	
	// MessageDigest is not thread-safe, so give one to each thread
	// http://stackoverflow.com/questions/415953/generate-md5-hash-in-java
	// Ref 1: http://stackoverflow.com/questions/817856/when-and-how-should-i-use-a-threadlocal-variable 
	// Ref 2: http://stackoverflow.com/questions/9654455/using-threadlocal-in-instance-variables
    private static final ThreadLocal<MessageDigest> digester = new ThreadLocal<MessageDigest>(){
        @Override
        protected MessageDigest initialValue()
        {
            try {
				return MessageDigest.getInstance("MD5");
			} catch (NoSuchAlgorithmException e) {
				logger.error("!!!! NoSuchAlgorithmException MD5 for MessageDigest !!!!", e);
			}
			return null;
        }
    };

    private String md5(String data){
    	return Hex.encodeHexString(digester.get().digest(StringUtils.getBytesUtf8(data))); 
    }
	
	public void deliverContentToPartner(List<Object> content, SyndicationPartner partner) throws ServiceException{
		try{
			logger.debug("deliverContentToPartner");
			long start = System.currentTimeMillis();
			ExtendedDestinationInfo extendedDestInfo = getPartnerInfoBySyndPartnerId(partner.getId());
			
			//Remove content which are not valid for syndication...
			content = removeContentNotValidForSyndication(content, partner, extendedDestInfo);
			
			//Reverse order of syndicating content based on the flag set in content restriction file.
			if(extendedDestInfo != null && extendedDestInfo.isReverseContentList()){
		        logger.debug("reversing content list");
				Collections.reverse(content);
		    }
			long partnerId = partner.getId();
			String partnerName = partner.getpartner_name();
			
			logger.debug("content size is {}, for Partner id {} name {} with destination target {} destination_type()", 
					new Object[]{content.size(), partnerId, partnerName, partner.getdestination_target(), partner.getdestination_type()});
			SyndicationDestination sdi = getSyndicationDestination(partner.getdestination_type());
			
			for(int i=0; i<content.size(); i++)
			{
				Document outputDoc = null;
				try{
					TimeZone tz = TimeZone.getTimeZone(getDateTimezone(extendedDestInfo));
			        Calendar syndDate = Calendar.getInstance(tz);
			        Object theContent = content.get(i);
			        BaseDynaBean asset = (BaseDynaBean)theContent;
			        String ID = (String)asset.get("ID");
			        Long contentPubDate = (Long)asset.get("pubDate");
			        outputDoc 		= createOutputXML(theContent, partner, extendedDestInfo ,syndDate);
					logger.debug("outputXML is != null {} for partner {}", (outputDoc!=null), partnerId);
					if(outputDoc!=null){
						String contentFormats= "";
						if( new Long(ModuleType.video.getId()).equals(partner.getModuleId())){
							//formats are only applicable for videos...
							contentFormats = getSyndicatedContentFormats(outputDoc);		
						}
						start = System.currentTimeMillis();
						try{
							//add output file name 
							String outputFileName = getOutputFileName(partner,extendedDestInfo,outputDoc);
							Element outputFileNameNode 	=  outputDoc.getDocument().getRootElement().addElement("XXXoutputfilename");
							outputFileNameNode.addText(outputFileName.toString());
							
				            // MODIFIED TO REMOVE DcoumentResult FOR CDATA FIX   
					        Document transformedDoc = transformToPartnerFormat(partner,outputDoc);
					        String contentHash = md5(transformedDoc.asXML());
					        
					        //make sure it's not duplicate one more time before proceeding...
							String dupRef = isDuplicateSyndication(ID, asset, partner,extendedDestInfo);
					        if(dupRef != null){
		    					logger.debug("DuplicateSyndication for {} partner id {} ", ID, partnerId);
		    					if(writeembargologs){
		    						updateDBSyndicationLogs(partner, buildStatusXML(ID ,
				    						GlobalConstants.REJECTED_STATUS, 
				    						GlobalConstants.DUPLICATE_CONTENT_EXCEPTION2 + dupRef) ,
				    						GlobalConstants.REJECTED_STATUS, new Date(), ID);
			    				}
		    					continue;
		    				}
					        trackContent(partner, ID, contentPubDate, contentFormats, IsDeletedEnum.DELIVERY_PENDING, contentHash);
					        logger.debug("SYNDICATION {} TO PARTNER {} ", ID, partnerId);
							sdi.deliverContent(outputDoc, partner, extendedDestInfo, transformedDoc, this, outputFileName);
								
							//addition tracking information added to xml 
							addSyndicationTrackingInformation(transformedDoc, ID, contentPubDate, start);
							
					        //--UPDATE CONTENT ID RECORDS (DB)
							trackContent(partner, ID, contentPubDate, contentFormats, IsDeletedEnum.DELIVERY_SUCCESS, contentHash);		
						
							//--UPDATE DB TRANSACTION LOG..
					        updateDBSyndicationLogs(partner, transformedDoc,
					        		GlobalConstants.SUCCESS_STATUS, 
					        		syndDate.getTime(), ID);
						}
						catch(Exception ex){
							logger.warn("Failed syndicating partner:" + partner + " ID:" + ID + ", failure logged", ex);
							
							StringBuffer buffer = new StringBuffer();
							buffer.append("\nCONTENT IDS[ ").append((ID!=null) ? ID : "" ).append(" ]\n\n");
							buffer.append("SYNDICATION TO PARTNER FAILED : ");
				            buffer.append(partnerName).append(" @" ).append(partner.getdestination_target());
				            buffer.append("\n\n").append(ExceptionUtils.getExceptionStackTrace(ex));
	
							trackContent(partner, ID, null, null, IsDeletedEnum.DELIVERY_FAILED, null);
							updateDBSyndicationLogs(partner, 
									buildStatusXML(ID,GlobalConstants.FAILED_STATUS,buffer.toString(), outputDoc),
									GlobalConstants.FAILED_STATUS, syndDate.getTime(), ID);
						}
						
						//--LOG PROCESSING TIME...
						long now = System.currentTimeMillis() ;
						long fir = now - objCreationTimeStamp;
				        long elapsed= now - start;
						logger.debug("[SYND EXECUTE END for {} Partner :{} - {} " +
								"ELAPSED(HR:MI:SE:MI)] : {}:{}:{}:{} from request start: {}:{}:{}:{}",
				        		new Object[]{
								ID, partnerName, partnerId,
								(elapsed/3600000)%24, (elapsed/60000)%60, (elapsed/1000)%60,elapsed/1000,
								(fir    /3600000)%24, (fir    /60000)%60, (fir    /1000)%60,fir    /1000});
					}
				}
				catch(Exception ex){	
					logger.error("Error in executing syndication thread "
							+  "\nSYNDICATION TO PARTNER FAILED: " + partnerName
							+" @" + partner.getdestination_target(), ex);
					updateDBSyndicationLogs(partner,
							buildStatusXML("", GlobalConstants.FAILED_STATUS,
							ExceptionUtils.getExceptionStackTrace(ex), outputDoc),
													  GlobalConstants.FAILED_STATUS, new Date(), "");
				}
			}
			long now = System.currentTimeMillis() ;
			if(now - start > GVPProperties.getPartnerDeliverTimeThreashold()){
				logger.warn("Delivery to Partner {}-{} took {} which is longer then > Deliver time threashold {} ", 
						new Object[]{partnerId, partnerName,(now-start), GVPProperties.getPartnerDeliverTimeThreashold()});
			}
		}
		catch(Exception ex){
			logger.error("Error in executing syndication " +  "\nSYNDICATION TO PARTNER FAILED: " + partner + " content " + content, ex);
			updateDBSyndicationLogs(partner,
					buildStatusXML("", GlobalConstants.FAILED_STATUS,
					ExceptionUtils.getExceptionStackTrace(ex) + " - " + content),
											  GlobalConstants.FAILED_STATUS, new Date(), "");
		}
	}

	private void addSyndicationTrackingInformation(Document transformedDoc, String contentIDs, Long contentPubDate, long start) throws UnknownHostException {
		Element rootElement =transformedDoc.getRootElement();
		
		//--UPDATE CONTENT ID RECORDS...
        Element contentIDNode = rootElement.addElement("XXXdblogcontentids");
		contentIDNode.addText(contentIDs);
		
		Element statusNode = rootElement.addElement("status");
		statusNode.addText("SUCCESS");
		// to add syndication mode
		Element syndRequestor = rootElement.addElement("syndRequestor");
		syndRequestor.addText((syndRequestor!=null) ? getSyndRequestor() : "");
	
		Element machineHostNode = rootElement.addElement("host");
		machineHostNode.addText(InetAddress.getLocalHost().getHostName());
		
		Element machineIPNode = rootElement.addElement("ip");
		machineIPNode.addText(InetAddress.getLocalHost().getHostAddress());
		
        String mdcId = MDCUtil.getRequestIdFromMDC();
        mdcId = mdcId == null ? "Missing" : mdcId;
        
        rootElement.addElement("mdcId").addText(mdcId);

		long now = System.currentTimeMillis() ;
		long fir = now - objCreationTimeStamp;
        long elapsed= now - start;
        rootElement.addElement("TimeExecution").addText(elapsed + "");
        rootElement.addElement("TimeSinceNotificaiton").addText(fir + "");

        Element pubDateNode = rootElement.addElement("XXXcontentpubdates");
		pubDateNode.addText(contentPubDate != null ? new Date(contentPubDate).toString():"");
	}


	private List<Object> removeContentNotValidForSyndication(List<Object> content, SyndicationPartner partner,
			ExtendedDestinationInfo extendedDestInfo) {
		List<Object> retContent = new ArrayList<Object>();
		for( int i = 0; i < content.size(); i++ ){	
		    //Verify ASSET Validity...
        	try {
        		BaseDynaBean bdb = (BaseDynaBean)content.get( i );
				if(isValidForSyndication(bdb, partner,extendedDestInfo)){
					retContent.add(bdb);
				}
			} catch (Exception e) {
				logger.error("Error while checking valid for syndication for asset " + content.get( i ) 
						+ ", partner : " + partner + ", extendedDestInfo:" + extendedDestInfo, e);
				//TODO : add to syndication log for this id...
			}
		}    
		return retContent;
	}

	private IntegerTypeConverter dupChkNulDelayChkMaxMillies= new IntegerTypeConverter(5000);
	protected String isDuplicateSyndication(String ID, BaseDynaBean asset, SyndicationPartner partner, ExtendedDestinationInfo extendedDestInfo) {
		return isDuplicateSyndication(ID, asset, partner, extendedDestInfo, true);
	}
	
	protected String isDuplicateSyndication(String ID, BaseDynaBean asset, SyndicationPartner partner, 
			ExtendedDestinationInfo extendedDestInfo, boolean delayIfNoPrevSynd) {
		try{
			//check if duplicate syndication verification is allowed
			Boolean dupSyndVerfyChkEnbld = GVPProperties.duplicateSyndicationVerificationSwitch();
			long partnerId = partner.getId();
			logger.debug("duplicateSyndicationVerificationSwitch {} for partner {} ", dupSyndVerfyChkEnbld, partnerId);
			if(getCheckLastUpdate() && dupSyndVerfyChkEnbld){
				
				logger.debug("Checking for duplicate syndication for partner {}" , partnerId);
				List<SyndicationTracking> deliveredContent = trackingService.getTrackingListForPartnerById(partner, ID.trim());
				if(deliveredContent!=null && deliveredContent.size()>0){
					SyndicationTracking sTracking = deliveredContent.get(0);
					
					//if this is failed delivery try again to get confirmed or pending delivery 
					if(IsDeletedEnum.DELIVERY_FAILED.getValue() == sTracking.getIsDeleted()){

						//previous one failed so try again any way...
						logger.debug("previous failed  for - {}",partnerId) ;
						return null;
					}
					
					//NOTE: Alternate Option: check for last syndication detail in translog table and 
					//get last record syndicated from translog table...
					TimeZone tz = TimeZone.getTimeZone(getDateTimezone(extendedDestInfo));
			        Calendar syndDate = Calendar.getInstance(tz);
			        syndDate.setTime(sTracking.getSyndDate());
					Document outputDoc  = createOutputXML(asset, partner, extendedDestInfo ,syndDate);
			        Document transformedDoc = transformToPartnerFormat(partner,outputDoc);

			        //see if hash is matching - then content is duplicate...
			        String contentHash = md5(transformedDoc.asXML());
			        logger.debug("ID - {}, contentHash - {}, sTracking.getContentHash() {} - partnerId {}",
			        		new Object[]{ID, contentHash,sTracking.getContentHash(),partnerId}) ;
					if(contentHash.equals(sTracking.getContentHash())){
						return String.valueOf(sTracking.getTrackId());
					}				
				}
				else if(delayIfNoPrevSynd){
					//if there are no records then delay for random time to avoid duplicate due to 
					//millisecond gaps....
					Thread.sleep(duplicateCheckRandom.nextInt(
							applicationConfigService.getAppConfigValueFromCache("XXXSYNDICATION","duplicateCheckNullDelayMaxMillies",dupChkNulDelayChkMaxMillies)));
					return isDuplicateSyndication(ID, asset, partner, extendedDestInfo, false);
				}
			}
			return null;
		}
		catch(Exception ex){
			logger.warn("isDuplicateSyndication check failed - allowing syndication - list : "
					+ asset + ",partner :" + partner + ", extendedDestInfo :" + extendedDestInfo, ex );
			return null;
		}
	}
	
	private static Map<String, SyndicationDestination> sd = new HashMap<String, SyndicationDestination>();
	private static final NoSyndicationDestination NO_SYND_DEST = new NoSyndicationDestination();
	static{
		sd.put("SERVICE", new ServiceSyndicationDestination());
		sd.put("FILE", new FileSyndicationDestination());
		sd.put("FTP", new FtpSyndicationDestination());
		sd.put("SFTP", new SftpSyndicationDestination());
		sd.put("NONE", NO_SYND_DEST );
	}
	
	private SyndicationDestination getSyndicationDestination(String desitnationName){
		if(desitnationName == null){
			logger.warn("No Syndication is configured - returning No Syndication Destination");
			return NO_SYND_DEST;
		}
		return sd.get(desitnationName);
	}

	public ApplicationConfigService getApplicationConfigService() {
		return applicationConfigService;
	}

	public void setApplicationConfigService(
			ApplicationConfigService applicationConfigService) {
		this.applicationConfigService = applicationConfigService;
	}

}