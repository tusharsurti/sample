package XXX.synapi.tasks.impl;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.Vector;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.DynaProperty;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import XXX.service.DBContentService;
import XXX.synapi.businessobjects.BaseDynaBean;
import XXX.synapi.businessobjects.BusinessObjectFactory;
import XXX.synapi.businessobjects.ContentValidator;
import XXX.synapi.businessobjects.ExtendedDestinationInfo;
import XXX.synapi.businessobjects.SyndicationPartner;
import XXX.synapi.businessobjects.SyndicationTracking;
import XXX.synapi.businessobjects.VideoAsset;
import XXX.synapi.constants.GlobalConstants;
import XXX.synapi.exception.AssetUnavailableException;
import XXX.synapi.service.AdminContentService;
import XXX.synapi.service.ContentService;
import XXX.synapi.service.HashService;
import XXX.synapi.util.Dom4jUtil;
import XXX.synapi.util.ExceptionUtils;
import XXX.synapi.util.GVPProperties;

/**
 * Task Object representing integration with VCPS (For doing syndication), and how to process video Produced by VCPS
 * 
 * 
 */
public class SyndicateVCPSTaskObjectImpl extends SyndicateTaskObjectImpl {
	/**
	 * Logger
	 */
	private static Logger logger = LoggerFactory.getLogger(SyndicateVCPSTaskObjectImpl.class);
	private ContentService contentService = null;
	private HashService hashService = null;
	private VideoAsset vAsset = null;
	private Document videoXmlDocument = null;
	
	/**
	 * Video Id's being processed with this task object
	 */
	private Long videoID;

	/**
	 * Constructor with required initialization attributes
	 */
	public SyndicateVCPSTaskObjectImpl(HashService hashService, ContentService contentService) {
		this.hashService = hashService;
		this.contentService = contentService;
	}

	public HashService getHashService() {
		return hashService;
	}

	public ContentService getContentService() {
		return contentService;
	}

	protected int getPageSize() {
		return PAGE_SIZE;
	}

	/**
	 * Implementation of abstract method...
	 * 
	 * @see SyndicateTaskObjectImpl#updateLastExecuteLogs(List)
	 */
	protected void updateLastExecuteLogs(List<Object> content) {
		if (content != null) {
			SimpleDateFormat formatter = new SimpleDateFormat("EEE, d MMM yyyy HH:mm a z");
			formatter.setTimeZone(TimeZone.getTimeZone("GMT"));

			if (lastExecutionLogs == null) {
				lastExecutionLogs = new Vector<String>();
				lastExecutionLogs.add(
						"LAST SyndicateVCPSTaskObject EXECUTION RUN ["
						+ formatter.format(Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTime()) + "]");
			}

			for (int i = 0; i < content.size(); ++i) {
				VideoAsset asset = (VideoAsset) content.get(i);
				long pubdate = (asset.get("contentAirdate") != null) ? new Long(asset.get("contentAirdate").toString()).longValue() : 0;
				String pubDate_fmt = formatter.format(new Date(pubdate));
				String id = (asset.get("ID") != null) ? asset.get("ID").toString() : "";

				// LOG SYNDICATED DOCUMENT INFO//
				lastExecutionLogs.add(StringUtils.join(new Object[]{"SYNDICATED DOCUMENT[",id,"]  PUBLISHED[",pubDate_fmt,"]"}));
			}
		}
	}

	/**
	 * Retrieve Asset Content from VCPS Server...
	 * @see SyndicateTaskObjectImpl#getContent()
	 */
	protected List<Object> getContent() throws Exception {
		
		logger.debug("Processing content Idex : " + contentIndex );
		if(contentIndex != 0){
			//This Task only process one video at a time so no point in
			//going beyond 0....
			return null;
		}
		VideoAsset asset = null;
		
		// Only make data request for Asset not marked for delete...
		boolean makeDataRequest = ((action == null) || !action.equalsIgnoreCase("DELETE"));
		logger.debug("makeDataRequest : " + makeDataRequest + ", action : " + action);
		if (makeDataRequest) {

			if(videoXmlDocument != null){
				logger.debug("Creating asset object from received xml");

				//xml is available get the document from xml
				asset = contentService.getVideo(videoXmlDocument);
			}
			if(asset == null){
				logger.debug("Creating asset object from video id");

				// requested to VCPS for getting actual content of event
				// GET Actual content of VIDEOS BASED ON ID...
				asset = contentService.getVideo(videoID);
			}
			if(asset == null){
				return new ArrayList<Object>();
			}
			if(additionalData != null){
				asset.set( "relatedLinks", additionalData);
			}
			if(relatedLinkTitles != null){
				asset.set( "relatedLinkTitles", relatedLinkTitles);
			}
		}
		else {
			
			//just a id is enough to process this request...
			asset = BusinessObjectFactory.createVideoAsset();
			asset.set("ID", videoID + "");
		}
		
		// Added to have action as part of the Content Restriction
		asset.set("action", (((action != null) && (action.length() > 0)) ? action.toUpperCase() : "MODIFY"));
		setAdditionalContent(asset);
		contentIndex++;
		contentIndexUpperBound += getPageSize();
		
		vAsset = asset;
		return Arrays.asList(new Object[]{vAsset});
	}
	
	private static final Set<String> notFromDynaSetFields = new HashSet<String>();
	static {
		notFromDynaSetFields.add("additionalFormats");
		notFromDynaSetFields.add("additionalHashcodes");
		notFromDynaSetFields.add("additionalFormatsizes");
		notFromDynaSetFields.add("GuestName");
		notFromDynaSetFields.add("action");
	}

	/**
	 */
	protected Document createOutputXML(Object content, SyndicationPartner partner, 
			ExtendedDestinationInfo extendedDestInfo, Calendar syndDate) {
		String currId = "";
		Document newDoc = null;
		try {
			
			logger.debug("createOutputXML(content:{}, partner:{}, extendedDestInfo:{})", new Object[]{"content NOT Logged", partner, extendedDestInfo});
			
			// Create XML DOCUMENT of resultant video list.
			newDoc = DocumentHelper.createDocument();

			// --ROOT ELEMENT
			Element rootElement = newDoc.addElement("XXX-global-video-response");

			// --ERROR ELEMENT
			Element errorElement = rootElement.addElement("error-code");
			errorElement.addText("0");

			// --SYNDICATION DATE
			SimpleDateFormat formatter = new SimpleDateFormat(getDateFormat(extendedDestInfo));
			formatter.setTimeZone(TimeZone.getTimeZone(getDateTimezone(extendedDestInfo)));

			Element yearElement = rootElement.addElement("currentYear");
			yearElement.addText(String.valueOf(syndDate.get(Calendar.YEAR)));

			Element pubDateElement = rootElement.addElement("syndicationDate");
			pubDateElement.addText(String.valueOf(syndDate.getTimeInMillis()));

			Element fmtPubDateElement = rootElement.addElement("syndicationDate_fmt");
			fmtPubDateElement.addText(formatter.format(syndDate.getTime()));

			// --DATA ELEMENT
			int pagesize = 1;
			Element dataElement = rootElement.addElement("data");
			dataElement.addAttribute("archivetotal", "");
			dataElement.addAttribute("page", "1");
			dataElement.addAttribute("hasnext", "false");
			dataElement.addAttribute("hasprev", "false");
			//TODO() Commented temporarily
			//Why we have category & keyword here ????? it should be in asset section...
//			dataElement.addAttribute("category", (category != null) ? category : "");
//			dataElement.addAttribute("keywords", (keywords != null) ? keywords : "");

			GVPProperties props = GVPProperties.getInstance();
			String thumbsFTPUser = props.getProperty("akamai.thumbs.ftp.user");
			String thumbsFTPPwd = props.getProperty("akamai.thumbs.ftp.user.password");

			String mediaFTPUser = props.getProperty("akamai.media.ftp.user");
			String mediaFTPPwd = props.getProperty("akamai.media.ftp.user.password");

			// --ALL VIDEO ASSETS...
			VideoAsset asset = BusinessObjectFactory.createVideoAsset();
			BeanUtils.copyProperties(asset, (VideoAsset) content);
			currId = String.valueOf(asset.get("ID"));
			
			// Apply Any Additional ASSET METADATA...
			applyAdditionalContentFromStaticSource(asset, extendedDestInfo);
			if ((action != null) && action.equalsIgnoreCase("DELETE")) {
				// Do not parse any additional information....
				Element videoElement = dataElement.addElement("video-asset");
				Element idElement = videoElement.addElement("ID");
				idElement.addText((asset.get("ID") != null) ? asset.get("ID").toString() : "");

				// Add All Date Nodes...
				Calendar currentDate = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
				currentDate.add(Calendar.DATE, -1);

				Element propertyElement = videoElement.addElement("contentAirdate");
				propertyElement.addText(String.valueOf(currentDate.getTimeInMillis()));

				String currentDate_fmt = formatter.format(currentDate.getTime());
				propertyElement = videoElement.addElement("contentAirdate_fmt");
				propertyElement.addText(currentDate_fmt);

				propertyElement = videoElement.addElement("expirationDate");
				propertyElement.addText(String.valueOf(currentDate.getTimeInMillis()));

				propertyElement = videoElement.addElement("expirationDate_fmt");
				propertyElement.addText(currentDate_fmt);

				// Add Action NODE...
				Element actionElement = videoElement.addElement("action");
				actionElement.addText(action.toUpperCase());
			} else {
				// NOTE:: Premium status will be controlled by XSL
				// transform....
				asset.set("Premium", new Boolean(false));
				
				//VCPS Does not produce any protected URL
				//asset.protectURL(login, password, hostname);
				asset.set( "protectedURL", asset.get( "URL"));
	    		asset.set( "additionalProtectedURLs", asset.get( "additionalURLs" ));
				
				if ((extendedDestInfo == null) || !extendedDestInfo.getAlwaysUseSeoURL()) {
					asset.set("seoURL", "");
				}

				Element videoElement = dataElement.addElement("video-asset");

				DynaProperty[] keys = asset.getDynaClass().getDynaProperties();
				//logger.debug("asset before building xml : {}",asset);
				for (int j = 0; j < keys.length; ++j) {
					String keyName = keys[j].getName();
					if (keyName.equalsIgnoreCase("Categories")) {
						if (asset.get(keyName) != null) {
							String keyValue = (String) asset.get(keyName);
							String[] categories = keyValue.split(";");

							for (int k = 0; k < categories.length; ++k) {
								Element propertyElement = videoElement.addElement("category");
								propertyElement.addText(categories[k]);
							}
						}
					} else if (keyName.equalsIgnoreCase("additionalProtectedURLs")) {
						if (asset.get(keyName) != null) {
							String keyValue = (String) asset.get(keyName);
							String[] addProtectedURLs = keyValue.split(";");
							
							for (int k = 0; k < addProtectedURLs.length; ++k) {
								Element propertyElement = videoElement.addElement("additionalProtectedURL");
								
								if (addProtectedURLs[k].indexOf("ftp://") != -1) {
									
									StringBuffer ftpBuffer = new StringBuffer();
									ftpBuffer.append(addProtectedURLs[k]);
									ftpBuffer.append("@");
									
									//if this is jpeg give thumbs ftp credentials
									if(addProtectedURLs[k].indexOf("jpeg") != -1){
										ftpBuffer.append(thumbsFTPUser).append(":").append(thumbsFTPPwd);
									}
									else{
										ftpBuffer.append(mediaFTPUser).append(":").append(mediaFTPPwd);
									}
									ftpBuffer.append(":");
									ftpBuffer.append((Long) asset.get("pubDate"));

									propertyElement.addText(ftpBuffer.toString());
								} else {
									propertyElement.addText(addProtectedURLs[k]);
								}
							}
						}
					} else if(notFromDynaSetFields.contains(keyName)){
						//derived from XML in VCPS
					} else if (keyName.equalsIgnoreCase("relatedLinks")) {
						if (asset.get(keyName) != null) {
							SyndicateSiteContentTaskObjectImpl taskObject = new SyndicateSiteContentTaskObjectImpl();
							String keyValue = (String) asset.get(keyName);
							String[] links = keyValue.split(",");

							String linkTitles = (String) asset.get("relatedLinkTitles");
							String[] linkTitlesArr = ((linkTitles != null) && (linkTitles.trim().length() > 0)) ? linkTitles
									.split("\\|") : null;

							for (int k = 0; k < links.length; ++k) {
								StringBuffer buffer = new StringBuffer();
								buffer.append(links[k]);
								buffer.append("|");

								// NOTE:: Take related link override if it
								// exists...
								String title = ((linkTitlesArr != null) && (k < linkTitlesArr.length)) ? linkTitlesArr[k] : null;
								buffer.append((((title != null) && (title.trim().length() > 0)) ? title : taskObject
										.validateSiteURL(links[k])));

								Element propertyElement = videoElement.addElement("relatedLink");
								propertyElement.addText(buffer.toString());
							}
						}
					} else if (keyName.equalsIgnoreCase("contentKeywords")) {
						
						//The contentKeywords key is obsolete - not used with VCPS
						//Instead region information is derived from "Market" and ticker from vcpsDocument itself... 
						
						//NOTE: The way of determining top story region from Page target is temporary
						//formal way should be determined involving all stack-holders & teams (CMS, VCPS, syndication etc...)
						String pageTarget = (String) asset.get("Market");
						boolean tvu = ((pageTarget != null) && pageTarget.toLowerCase().contains("tvu"));
						boolean tva = ((pageTarget != null) && pageTarget.toLowerCase().contains("tva"));
						boolean tve = ((pageTarget != null) && pageTarget.toLowerCase().contains("tve"));

						if(tvu || tva || tve){
							Element propertyElement = videoElement.addElement("topstoryregion");
							if(tvu){
								propertyElement.addText("US");
							}else if(tva){
								propertyElement.addText("EUROPE");
							}else{
								propertyElement.addText("ASIA");
							}
						}
					} else if(keyName.equalsIgnoreCase("vcpsDocument")){
						//The Raw XML Element stored in asset is used directly for producing some of the xml content...
						Element vcpsEvent = (Element) asset.get("vcpsDocument");

						StringBuffer buffer = new StringBuffer();
						
						buffer.delete(0, buffer.length());
						@SuppressWarnings("unchecked")
						List<Node> tickerNodeList = vcpsEvent.selectNodes("//TICKERS/TICKER");
						for (Node tickerNode : tickerNodeList) {
							Element tickerElement = videoElement.addElement("tickersymbol");
							tickerElement.addText(tickerNode.getText().trim());
						}
						
						//build additionalFormat Elements
						@SuppressWarnings("unchecked")
						List<Node> assetNodeList = vcpsEvent.selectNodes("//Assets/Asset");
						for (Node assetNode : assetNodeList) {
							Element assetEle = (Element) assetNode;
//							String assetType = Dom4jUtil.getChildElementValue(assetEle,"AssetType");
//							logger.debug("processing asset type : " + assetType);
							
							String formatString = Dom4jUtil.getChildElementValue(assetEle, "Format");
							String bitRate = Dom4jUtil.getChildElementValue(assetEle, "Bitrate");
							String hash = Dom4jUtil.getChildElementValue(assetEle, "Hash");
							String size = Dom4jUtil.getChildElementValue(assetEle, "Size");
							
							@SuppressWarnings("unchecked")
							List<Node> urlList = assetEle.selectNodes("URL");
//								logger.debug("AssetType {} has urlList of size {}  ", assetType, urlList.size());

							for (Node urlNode : urlList) {
								Element urlEle = (Element) urlNode;
								//TODO:  to confirm with kiran that this element comes under url not under asset...
								String releaseId = Dom4jUtil.getChildElementValue(urlEle, "mpx_releaseId");
								
								String protocol = 
									Dom4jUtil.getChildElementValue(urlEle, "Protocol");
																	
								String formatName = StringUtils.join(new Object[]{
											formatString,"_",bitRate,"_",protocol});

								String hashCode = StringUtils.join(new Object[]{
											formatName,"|", hash});
								
								String sizeVal = StringUtils.join(new Object[]{
										formatName,"|", size});

								videoElement.addElement("additionalFormat").addText(formatName);
								videoElement.addElement("additionalHashcode").addText(hashCode);
								videoElement.addElement("additionalFormatsize").addText(sizeVal);
								if(releaseId != null && releaseId.trim().length()>0 ){
									videoElement.addElement("additionalReleaseId")
											.addText(StringUtils.join(new Object[]{formatName,"|", releaseId}));
								}
							}
							
							//Retrieve Short Title for Video Asset
							Node shortTitleNode = vcpsEvent.selectSingleNode("//SHORT_EVENT_NAME");
							if(shortTitleNode != null) {
								Element shortTitleElement = videoElement.addElement("contentShortTitle");
								//logger.debug("shortTitle::" + shortTitleNode.getText().trim());
								shortTitleElement.addText(shortTitleNode.getText().trim());	
							}
							
							//Retrieve Short Description for Video Asset
							Node shortDescNode = vcpsEvent.selectSingleNode("//SHORT_DESCRIPTION");
							if(shortDescNode != null) {
								Element shortDescElement = videoElement.addElement("contentShortDescription");
								//logger.debug("shortDescription::" + shortDescNode.getText().trim());								
								shortDescElement.addText(shortDescNode.getText().trim());
							}
						}
						
						//add extendedCategory available in VCPS
						@SuppressWarnings("unchecked")
						List<Node> categoyNodeList = vcpsEvent.selectNodes("//CATEGORIES/CATEGORY");
						logger.debug("Found {} Category " , categoyNodeList.size());
						StringBuilder catBuilder = new StringBuilder();
						for (Node catNode : categoyNodeList) {
							Element destCatElement = videoElement.addElement("extendedCategory");
							if(catBuilder.length()>0) catBuilder.delete(0, catBuilder.length());
							
							Element catElement = (Element) catNode;
							catBuilder.append(Dom4jUtil.getChildElementValue(catElement,"tagCatId"));
							catBuilder.append("|");
							catBuilder.append(Dom4jUtil.getChildElementValue(catElement,"tagPathClean"));
							catBuilder.append("/");
							catBuilder.append(Dom4jUtil.getChildElementValue(catElement, "tagName"));
							logger.debug("adding extededCategory {} " , catBuilder.toString());
							destCatElement.addText(catBuilder.toString());
						}
						
						//guest name building
						@SuppressWarnings("unchecked")
						List<Node> guestNodeList = vcpsEvent.selectNodes("//GUESTS/NAME");
						for(Node guestNode : guestNodeList){
							videoElement.addElement("GuestName").addText(guestNode.getText().trim());
						}
					} 
					else {
						Element propertyElement = videoElement.addElement(keyName);
						propertyElement.addText((asset.get(keyName) != null) ? asset.get(keyName).toString() : "");

						if (keyName.equalsIgnoreCase("contentAirdate")) {
							long theDate = (asset.get(keyName) != null) ? new Long(asset.get(keyName).toString()).longValue() : 0;
							String pubDate_fmt = formatter.format(new Date(theDate));
							propertyElement = videoElement.addElement("contentAirdate_fmt");
							propertyElement.addText(pubDate_fmt);

							// Add Expiration Date Node
							Calendar expirationDate = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
							expirationDate.setTime(new Date(theDate));
							expirationDate.add(Calendar.DATE, new Integer(getContentLife(extendedDestInfo)).intValue());

							propertyElement = videoElement.addElement("expirationDate");
							propertyElement.addText(String.valueOf(expirationDate.getTimeInMillis()));

							String expirationDate_fmt = formatter.format(expirationDate.getTime());
							propertyElement = videoElement.addElement("expirationDate_fmt");
							propertyElement.addText(expirationDate_fmt);
						}
						if (keyName.equalsIgnoreCase("updateDate")) {
							long theDate = (asset.get(keyName) != null) ? new Long(asset.get(keyName).toString()).longValue() : 0;
							String theDate_fmt = formatter.format(new Date(theDate));
							propertyElement = videoElement.addElement("updateDate_fmt");
							propertyElement.addText(theDate_fmt);
						}
					}
				}
				Element propertyElement = videoElement.addElement("action");
				propertyElement.addText((((action != null) && (action.length() > 0)) ? action.toUpperCase() : "MODIFY"));
			}

			dataElement.addAttribute("pagesize", String.valueOf(pagesize));
			dataElement.addAttribute("total", String.valueOf(pagesize));
		} catch (Exception ex) {
		
			logger.error("Error in creating output xml", ex);
			String msg = StringUtils.join(new Object[]{
					"\nSYNDICATION TO PARTNER FAILED: ",partner.getpartner_name()," @",partner.getdestination_target(),
					"\n\n", ExceptionUtils.getExceptionStackTrace(ex)});
			updateDBSyndicationLogs(partner, buildStatusXML(currId, GlobalConstants.FAILED_STATUS, msg),
					GlobalConstants.FAILED_STATUS, new Date(), currId);
		}
		return newDoc;
	}

	
	/**
	 * Verify that the event is valid for syndication or not...
	 * It does all possible type of validation e.g. content cap, duplicate syndication etc...
	 * @param asset
	 * @param partner
	 * @param extendedDestInfo
	 * @return
	 * @throws Exception
	 */
	protected boolean isValidForSyndication(BaseDynaBean assetO, SyndicationPartner partner, ExtendedDestinationInfo extendedDestInfo)
			throws Exception {
		VideoAsset asset = BusinessObjectFactory.createVideoAsset();
		BeanUtils.copyProperties(asset, (VideoAsset)assetO );
		logger.debug("validForSyndication(assetid:{}, partner:{}, extendedDestInfo:{})",
				new Object[]{asset.get("ID"), partner, extendedDestInfo});
		//logger.debug("asset:{}",asset);
		String partnerType = partner.getsyndication_type();
		logger.debug("partnerType:{}", partnerType);

		// Ensure there is a valid VIDEO ASSET...
		String ID = (String) asset.get("ID");
		if ((ID == null) || (ID.trim().length() == 0)) {
			logger.warn("asset is missing id - asset is invalid for syndication to partner {}", partner);
			return false;
		}

		boolean valid = true;
		
		// Proceed with remaining validation..
		// NOTE:: When ACTION TYPE is DELETE only ASSET ID is required. There is
		// NO NEED to perform further content checks.
		if ((action != null) && action.equalsIgnoreCase("DELETE")) {
			if (valid && (trackingService != null)) {
				List<SyndicationTracking> deliveredContent = trackingService.getTrackingListForPartnerById(partner, ID.trim());
				
				logger.debug("Evaluating delete for partner {} - deliveredContent {}", partner, deliveredContent);

				// VALID FOR DELETION ONLY IF CONTENT HAS ALREADY BEEN DELIVERED
				// TO PARTNER
				valid = ((deliveredContent != null) && (deliveredContent.size() >= 1));

				if (!valid && writeembargologs) {
					updateDBSyndicationLogs(partner, buildStatusXML(ID, GlobalConstants.REJECTED_STATUS,
							GlobalConstants.DELETE_CONTENT_EXCEPTION), GlobalConstants.REJECTED_STATUS, new Date(), ID);
				}
			}
		} else if (valid) {
			String message = "";
			
			//Check if asset is having valid workflow status @ path /event/status
			Element vcpsEventEle = (Element)asset.get("vcpsDocument");
			String assetStatus = Dom4jUtil.getXPathVal(vcpsEventEle, "//event/status");
			if(!"submitted".equals(assetStatus)){
				message = String.format("asset id %1$s - is not submitted - status is %2$s", ID, assetStatus);
				valid = false;
			}
			
			String url = (String)asset.get("URL");
			if(valid && (url == null || url.trim().length() == 0)){
				//the asset is not ready to syndication
				message = String.format("ID %1$s Asset does not have valid URL value %2$s for partner %3$s - so failing it...", ID, url, partner);
				valid = false;
			}

	    	if(valid){
				// Determine if VIDEO ASSET is valid for SUBMITTED PARTNER
				// --CONTENT VALIDATION
				if ("CONTENT".equalsIgnoreCase(partnerType)) {
					logger.debug("Evaluating extendedDestInfo: {} for CONTENT partner {} ", extendedDestInfo, partner);
					valid = isValidForContentValidator(ID, asset, partner, extendedDestInfo);
				}
				// Determine if ASSET is valid for SUBMITTED PARTNER--BASED ON
				// MANUAL EXECUTION ONLY
				// --MANUAL EXECUTION VALIDATION
				else if ("ONDEMAND".equalsIgnoreCase(partnerType)) {
					valid = ((partnerID == null) || (partnerID.trim().length() == 0)) ? false : true;
					logger.debug("On demand partner  valid {} for partner {} ", valid, partner);
					if (valid) {
						valid = isValidForContentValidator(ID, asset, partner, extendedDestInfo);
					}
				}
				else{
					logger.debug("Partner Type {} for partner {} - not syndicating", partnerType, partner);
					valid = false;
				}
				if(valid){
			    	// VALIDATE IF CONTENT HAS ALREADY BEEN SYNDICATED....
					// --DELIVERY DUPLICATION VALIDATION
					valid = IsContentSyndicated(asset, partner, extendedDestInfo);
					logger.debug("Resultof  IsContentSyndicated {} for partner {} ", valid, partner);
				}

		    	if(valid){
					//VALIDATE IF ALL CONTENT ASSETS ARE AVAILABLE..
			    	//PACKAGE COMPLETION VALIDATION --
		    		valid = IsPackageComplete(asset, partner, extendedDestInfo);
		    		logger.debug("Result of  IsPackageComplete {} for partner {} ", valid, partner);
		    	}
		    	
		    	if(valid){
					// VALIDATE IF DELIVERY CAP HAS BEEN REACHED...
					// --DELIVERY CAP VALIDATION
					valid = IsDeliveryCapPreserved(asset, partner, extendedDestInfo);
					logger.debug("Result of  IsDeliveryCapPreserved {} for partner {} ", valid, partner);
		    	}
			}
	    	else{
				logger.debug("Rejected with message {}", message);
				
				//mention that asset is not ready for syndication
				updateDBSyndicationLogs(partner, buildStatusXML(ID , GlobalConstants.REJECTED_STATUS, message),
						GlobalConstants.REJECTED_STATUS, new Date(), ID);
			}
		}
		return valid;
	}
	
	private boolean isValidForContentValidator(String ID, VideoAsset asset, SyndicationPartner partner, ExtendedDestinationInfo extendedDestInfo) 
	throws Exception{
		logger.debug("Evaluating extendedDestInfo: {} for partner {} ", extendedDestInfo, partner);
		// PERFORM CONTENT BASED VALIDATION
		if (extendedDestInfo != null) {
			List<ContentValidator> contentSourceList = extendedDestInfo.getContentSource();
			logger.debug("found this contentSourceList : {} for partner {} ", contentSourceList, partner);
			if (contentSourceList != null) {
				for (int i = 0; i < contentSourceList.size(); ++i) {
					ContentValidator contentValidator = (ContentValidator) contentSourceList.get(i);
					if (!contentValidator.isValid(asset, trackingService)) {
						if (writeembargologs) {
							writeMatchTypeExceptionLogs(ID, partner, contentValidator);
						}
						logger.debug("Failed contentValidator {} for partner {} ", contentValidator, partner);
						return false;
					}
				}
			}
		}
		return true;
	}
	
	private boolean IsPackageComplete(VideoAsset asset, SyndicationPartner partner, ExtendedDestinationInfo extendedDestInfo){
		boolean valid  = true;
		if((asset!=null) && (partner!=null) && (extendedDestInfo!=null)){
			String ID = (String)asset.get("ID");
			logger.debug("IsPackageComplete for ID : " + ID + " packaged delivery ? : " + extendedDestInfo.isPackageDelivery());
			if(extendedDestInfo.isPackageDelivery()){
        		List<Map<String, String>> contentList = extendedDestInfo.getContent();
        		logger.debug("contentList : " + contentList);
    			if(contentList!=null){
        			String contentXml = getContentXml(asset);
        			logger.debug("contentXml : " + contentXml);
        			for(int i =0; i<contentList.size(); ++i){
        				Map<String, String> content = (Map<String, String>)contentList.get(i);
        				String contentURL = getContentURL(contentXml, content);
        				logger.debug(" Checking Content URL " + contentURL);
        				if((contentURL == null) || (contentURL.trim().length()==0)){
        					valid = false;
        					if(writeembargologs){        						
        						// MODIFIED TO UPDATE AS FAILED SO THAT ITS PICKED UP AND RE-SYNDICATED
        						updateDBSyndicationLogs(partner, 
        								buildStatusXML(ID ,GlobalConstants.REJECTED_STATUS, GlobalConstants.ASSET_UNAVAILABLE_EXCEPTION),
        								GlobalConstants.FAILED_STATUS, new Date(), ID);
        					}
        					break;
        				}			
            		}
            	}
        	}
		}
		return valid;
	}
	
	private String getContentXml(VideoAsset asset)
	{
	    //Create XML DOCUMENT.			
        Document newDoc =  DocumentHelper.createDocument();
        
        //--ROOT ELEMENT
        Element rootElement = newDoc.addElement("root");
        
        String thumbnail 	= (String)asset.get("smallThumbnailURL");
		String lgThumbnail 	= (String)asset.get("largeThumbnailURL");
		
		StringBuffer smallthumbBuffer = new StringBuffer();
		if(thumbnail != null && thumbnail.trim().length() > 0)
		{
			smallthumbBuffer.append("jpeg_0_Download_small|");
			smallthumbBuffer.append(thumbnail);
			rootElement.addElement("additionalProtectedURL").addText(smallthumbBuffer.toString());
		}
		
		StringBuffer largethumbBuffer = new StringBuffer();
		if(lgThumbnail != null && lgThumbnail.trim().length() > 0)
		{
			largethumbBuffer.append("jpeg_0_Download_large|");
			largethumbBuffer.append(lgThumbnail);
	        rootElement.addElement("additionalProtectedURL").addText(largethumbBuffer.toString());
		}
		
		String keyValue = (String)asset.get("additionalURLs");
		if(keyValue != null && keyValue.trim().length() > 0 )
		{
			String [] addProtectedURLs = keyValue.split(";");
			for(int k=0;k<addProtectedURLs.length;++k)
			{
				Element propertyElement = rootElement.addElement("additionalProtectedURL");
				propertyElement.addText(addProtectedURLs[k]);
			}
		}
       
       return newDoc.asXML();
	}


	/**
	 * Checked for VCPS (Good)
	 */
	protected boolean IsContentSyndicated(VideoAsset asset, SyndicationPartner partner, ExtendedDestinationInfo extendedDestInfo)
			throws ParseException {
		boolean valid = true;

		if ((asset != null) && (partner != null) && (extendedDestInfo != null) && (trackingService != null) && getCheckLastUpdate()) {
			Long pubDateTime = (Long) asset.get("pubDate");
			String additionalFormats = (String) asset.get("additionalURLs");
			if ((pubDateTime != null) || (additionalFormats != null)) {
				String ID = (String) asset.get("ID");
				List<SyndicationTracking> deliveredContent = trackingService.getTrackingListForPartnerById(partner, ID.trim());

				if ((deliveredContent != null) && (deliveredContent.size() >= CHECK_LAST_UPDT_PAGE_SIZE)) {
					
					logger.debug("partner {} Found deliveredContent.size()={}", partner.getSyndPartnerId(), deliveredContent.size() );
					
					SyndicationTracking vTracking = deliveredContent.get(0);

					// --METADATA UDPATE CHECK....
					// NOTE: CONTENT UPDT FLAG will only be enabled if CONTENT
					// DATE is not found in our DB or
					// WHEN the incoming ASSET DATE is less than what was
					// returned from DB
					boolean needContentUpdate = true;

					if ((pubDateTime != null) && (vTracking.getUpdateDate() != null)) {
						// Ensure time is normalized prior to comparison....
						SimpleDateFormat formatter = new SimpleDateFormat("EEE MMM d HH:mm:ss z yyyy");
						String strPubDate = formatter.format(new Date(pubDateTime.longValue()));
						String strTrackingDate = formatter.format(vTracking.getUpdateDate());

						Date pubDate = formatter.parse(strPubDate);
						Date trackingPubDate = formatter.parse(strTrackingDate);

						if ((pubDate.getTime() <= trackingPubDate.getTime()) || (pubDate.getTime() - trackingPubDate.getTime() < MINUTE)) {
							needContentUpdate = false;
						}
						logger.debug("partner {} needContentUpdate={}, pubDate={}, trackDate={}", 
								new Object[]{partner.getSyndPartnerId(), needContentUpdate, pubDateTime, vTracking.getUpdateDate().getTime()} );
					}

					// --FORMAT CHECK...
					// NOTE:: FORMAT UPDT FLAG will only be enabled WHEN
					// DELIVERED FMTS are not found in our DB or
					// when the incoming ASSET FMTS are not found in what was
					// returned from DB
					boolean needFormatUpdate = true;
					String videoFormat = vTracking.getSyndFormat();

					if ((additionalFormats != null) && (videoFormat != null)) {
						String[] videoFormatArr = videoFormat.split(";");
						String[] additionalFormatArr = additionalFormats.split(";");

						for (int i = 0; i < videoFormatArr.length; ++i) {
							for (int j = 0; j < additionalFormatArr.length; ++j) {
								if (additionalFormatArr[j].trim().indexOf(videoFormatArr[i].trim()) != -1) {
									if (additionalFormatArr[j].trim().indexOf("ftp://") != -1) {
										// SPECIFIC MEDIA CONTENT HAS BEEN
										// DELIVERED -- REMOVE FROM ADDITIONAL
										// URLs LIST (FTP ONLY)..
										additionalFormats = additionalFormats.replace(additionalFormatArr[j].trim(), "");
//										logger.debug("Removing format "+ additionalFormatArr[j].trim() 
//												+" from additionalFormats : " + additionalFormats);
										
									}

									// DELIVERED FORMAT HAS BEEN FOUND -- STOP
									// ITERATING...
									break;
								}
							}
						}

						if (additionalFormats.indexOf("ftp://") == -1) {
							// All expected VIDEO Formats have been delivered...
							needFormatUpdate = false;

							String thumbnail = (String) asset.get("smallThumbnailURL");
							String lgThumbnail = (String) asset.get("largeThumbnailURL");

							// Check if small thumbnail is delivered or not....
							if ((thumbnail != null) && (thumbnail.trim().length() > 0)) {
								needFormatUpdate = (videoFormat.indexOf("jpeg_0_Download_small") == -1);
							}

							// Check large thumbnail is delivered or not...
							if (!needFormatUpdate && (lgThumbnail != null) && (lgThumbnail.trim().length() > 0)) {
								needFormatUpdate = (videoFormat.indexOf("jpeg_0_Download_large") == -1);
							}
						}
						
						additionalFormats = additionalFormats.replace(";;", ";").trim();
						additionalFormats = additionalFormats.replace(";;", ";").trim();
//						logger.debug("replace ;; additionalFormats : " + additionalFormats);
						
						logger.debug("partner {} needFormatUpdate={}, videoFormat={}, additionalFormats={}", 
								new Object[]{partner.getSyndPartnerId(), needFormatUpdate, videoFormat, additionalFormats} );
						//see if download URL's should be stripped or not for delivered content
						List<Map<String, String>> contentList = extendedDestInfo.getContent();
						//Remove delivered content where incremental delivery is being done for the partner
						//i.e. package delivery is false...
						if ("FTP".equalsIgnoreCase(partner.getdestination_type()) 
								&& contentList != null && contentList.size() >= 1
								&& (!extendedDestInfo.isPackageDelivery())) {
							logger.debug("stripping DownloadableURLs");
							asset.set("additionalURLs", additionalFormats);
						}
					}

					valid = (needContentUpdate || needFormatUpdate);
					logger.debug("IsContentSyndicated Result : " + valid);
					if (!valid && writeembargologs) {
						updateDBSyndicationLogs(partner, buildStatusXML(ID, GlobalConstants.REJECTED_STATUS,
								GlobalConstants.DUPLICATE_CONTENT_EXCEPTION), GlobalConstants.REJECTED_STATUS, new Date(), ID);
					}
				}
			}
		}
		return valid;
	}

	/**
	 * Check if partner is configured with any delivery cap - if confirm cap has been reached or not
	 * 
	 * Delivery cap is generally configured for day/week/monthly/year and count of content allowed
	 * 
	 * @param asset
	 * @param partner
	 * @param extendedDestInfo
	 * @return
	 */
	private boolean IsDeliveryCapPreserved(VideoAsset asset, SyndicationPartner partner, ExtendedDestinationInfo extendedDestInfo) {
		boolean valid = true;
		if (extendedDestInfo != null) {
			List<Calendar> deliveryCapRange = getDeliveryCapDateRange(extendedDestInfo);
			if ((deliveryCapRange != null) && (asset != null) && (partner != null) && (trackingService != null)) {
				String ID = (String) asset.get("ID");
				List<SyndicationTracking> deliveredContent = trackingService.getTrackingList(partner, ((Calendar) deliveryCapRange.get(0))
						.getTime(), ((Calendar) deliveryCapRange.get(1)).getTime());
				if (deliveredContent != null) {
					int deliveryCap = extendedDestInfo.getDeliveryCap();
					if (deliveredContent.size() >= deliveryCap) {

						// If DELIVERY_CAP has been reached then only allow
						// further updates/modifications to
						// content that has been syndicated.
						valid = false;
						for (Iterator<SyndicationTracking> iter = deliveredContent.iterator(); iter.hasNext();) {
							SyndicationTracking vTracking = (SyndicationTracking) iter.next();
							if (vTracking.getSyndicationUId() != null && vTracking.getSyndicationUId().equals(ID.trim())) {
								valid = true;
								break;
							}
						}
						if (!valid && writeembargologs) {
							updateDBSyndicationLogs(partner, buildStatusXML(ID, GlobalConstants.REJECTED_STATUS,
									GlobalConstants.DELIVERY_RAECHED_EXCEPTION), GlobalConstants.REJECTED_STATUS, new Date(), ID);
						}
					}
				}
			}
		}
		return valid;
	}

	/**
	 * Checked for VCPS (Good)
	 */
	protected void writeContentExceptionLogs(String partnerID, String IDs, Exception ex) {
		logger.debug("Writing log in db for partnerID: {}, IDs:{}, ex:{}", new Object[]{partnerID, IDs, ex});
		try {
			String exception;

			if (ex != null) {
				
				StringBuffer buffer = new StringBuffer();
				buffer.append("Submitted asset(s) are not available.  Exception: ");
				buffer.append(ExceptionUtils.getExceptionStackTrace(ex));
				exception = buffer.toString();
			} else {
				exception = VCPSASSET_UNAVAILABLE_EXCEPTION;
			}

			if ((partnerID == null) || (partnerID.trim().length() == 0)) {
				updateDBSyndicationLogs(getPartnerBySyndPartnerId(DEFAULT_PARTNERID), buildStatusXML(IDs, GlobalConstants.REJECTED_STATUS,
						exception), GlobalConstants.REJECTED_STATUS, new Date(), IDs);
			} else {
				if ((IDs != null) && (IDs.trim().length() > 0)) {
					String partnerIDs[] = partnerID.split(",");
					for (int i = 0; i < partnerIDs.length; i++) {
						if ((partnerIDs[i].trim().length() > 0) && (partnerIDs[i] != null)) {
							SyndicationPartner sp = getPartnerBySyndPartnerId(Long.parseLong(partnerIDs[i]));
							if(sp != null){
								updateDBSyndicationLogs(sp, buildStatusXML(IDs,
										GlobalConstants.REJECTED_STATUS, exception), GlobalConstants.REJECTED_STATUS, new Date(), IDs);
							}
							else{
								logger.warn("Syndication Partner not found for id " + partnerIDs[i]);
							}
						}
					}
				}
			}
		} catch (Exception exception) {
			logger.error("Error writing writeContentExceptionLogs", exception);
		}
	}

	/**
	 * String representation of Object
	 * @see Object#toString()
	 */
	public String toString() {
		return 	new ToStringBuilder(this)
				.append("videoIDs",videoID)
				.toString();
	}

	public final static int PAGE_SIZE = 99; // NOTE:: SIZE OF 99 IS MAX REQUESTS THAT CAN BE HANDLED BY "thePlatform"
	public final static long MINUTE = (60 * 1000);
	private final static String VCPSASSET_UNAVAILABLE_EXCEPTION = "Submitted asset(s) are not available.  Please check asset IDs in VCPS";

	/**
	 * Get ID of Asset object passed...
	 */
	protected String getID(Object bean) {
		VideoAsset asset = (VideoAsset) bean;
		String ID = (String) asset.get("ID");
		return ID;
	}

	/**
	 * Checked for VCPS (Good) 
	 */
	protected void setAdditionalContent(Object bean) throws Exception {
		//logger.debug("Setting Additional Content to bean : {}",  bean);
		if ((additionalContentKeys != null) && (additionalContentKeys.size() > 0) && (additionalContentService != null)) {
			VideoAsset srcAsset = (VideoAsset) bean;
			String ID = (String) srcAsset.get("ID");

			if (ID != null) {
				// Transfer Archived Content to submitted data asset...
				Set<Map.Entry<String, Map<String,String>>> entries = additionalContentKeys.entrySet();
    			for(Iterator<Map.Entry<String, Map<String,String>>> it = entries.iterator();it.hasNext();)
    			{
    				Map.Entry<String, Map<String,String>> entry = it.next();
    				String key = entry.getKey();
    				Map<String,String> keyMap = entry.getValue();
					StringBuffer buffer = new StringBuffer();
					buffer.append((String) keyMap.get("idprefix"));
					buffer.append("'");
					buffer.append(ID.trim());
					buffer.append("'");
					buffer.append((String) keyMap.get("idsuffix"));
					buffer.append((String) keyMap.get("dbkey"));
					String content = additionalContentService.getField(new Long(ID), buffer.toString(), (String) keyMap.get("dbnamespace"),
							(String) keyMap.get("dbnamespacepath"));
					if ((content != null) && (content.trim().length() > 0)) {
						srcAsset.set(key, content);
						if("contentCCTranscript".equals(key)){
							logger.debug("existing contentCCTranscript for ID " + ID + " -- " 
									+ (srcAsset.get("contentCCTranscript")!= null && srcAsset.get("contentCCTranscript").toString().trim().length()>0));
						}
					}

					logger.debug("Applying additional content to id {} for key : {}", ID, key );
					// Apply Any Additional ASSET METADATA
					applyAdditionalContentFromDynamicSource(ID, key, srcAsset);
				}
    			logger.debug("contentCCTranscript for ID " + ID + " -- " 
    					+ (srcAsset.get("contentCCTranscript")!= null && srcAsset.get("contentCCTranscript").toString().trim().length()>0));
			}
		}
		
		//logger.debug("After additional Content to bean is set : {}", bean);
	}

	public DBContentService getAdditionalContentService() {
		return additionalContentService;
	}
	
	public AdminContentService  getAdminContentService()
	{
		return this.adminContentService;
	}

	public Long getVideoID() {
		return videoID;
	}

	/**
	 * Set the video id
	 * 
	 * This task object method call is sequence dependent -- Ideally this 
	 * should be called before calling execute method of super class...
	 * 
	 * @param videoIDs
	 */
	public void setVideoID(Long videoID) {
		this.videoID = videoID;
		
		//set id's for logging information...
		IDs = String.valueOf(videoID);
	}
	
	/**
	 * Returns FTP Details for given asset format and bitrate. Returns the values using VCPS Metadata XML Document stored in VideoAsset object as XML Element.
	 * 
	 * @param format Asset Format WMV, FLV, MPEG4 etc.
	 * @param bitrate Bitrate for given asset format
	 * @thumbnailSize Size of thumbnail. This parameter is required as large and small thumbnails both have same bitrate and format ie 0 and jpeg. 
	 * 					Hence this third parameter is required to retrieve UploadURL
	 * @return
	 * @throws Exception
	 */
	public String getFTPDetails(String format, String bitrate, String thumbnailSize) throws AssetUnavailableException,Exception {
		
		String ftpURL = null;
		String assetType= null;
		
		logger.debug("Retrieving FTP Details for format::" + format + " and bitrate::" + bitrate + " and thumbnailSize::" + thumbnailSize);
		
		try{
		VideoAsset asset = vAsset;
		logger.debug("Retrieved VideoAsset::" + asset.toString());
		
		Element vcpsEventEle = (Element)asset.get("vcpsDocument");
		logger.debug("Retrieved Element::" + vcpsEventEle.asXML());
		
			
		if(thumbnailSize != null) {
			//For thumbnails format and bitrate is the same. So the distinguishing assettype is used to retrieve UploadURL
			if(thumbnailSize.equalsIgnoreCase("large")){
				assetType="largeThumbnail";
				ftpURL = Dom4jUtil.getXPathVal(vcpsEventEle, "//Assets/Asset[AssetType='largeThumbnail']/UploadURL");
			}else if(thumbnailSize.equalsIgnoreCase("small")){
				assetType="smallThumbnail";
				ftpURL = Dom4jUtil.getXPathVal(vcpsEventEle, "//Assets/Asset[AssetType='smallThumbnail']/UploadURL");
			}
		 }
		 else {
			//Thumbnail is null so retrieving URL using combination of bitrate and format
			ftpURL = Dom4jUtil.getXPathVal(vcpsEventEle, "//Assets/Asset[Bitrate='" + bitrate + "' and Format='" + format + "']/UploadURL");
			assetType=Dom4jUtil.getXPathVal(vcpsEventEle, "//Assets/Asset[Bitrate='" + bitrate + "' and Format='" + format + "']/AssetType");
		 }
			
		logger.debug("Retrieved ftpURL::" + ftpURL);		
		if(null == ftpURL || ftpURL.length() < 1)
			throw new AssetUnavailableException("FTP Url not Proper. "+getExceptionMessage(assetType,bitrate,format));
		
		//Stripping off FTP Username and IP Address from UploadURL
		StringBuffer tempFtpUrl = new StringBuffer(255);
		tempFtpUrl.append(ftpURL.substring(0, ftpURL.indexOf("//")+2));
		tempFtpUrl.append(ftpURL.substring(ftpURL.indexOf("@")+1, ftpURL.indexOf("@", ftpURL.indexOf("@")+1)));
		tempFtpUrl.append(ftpURL.substring(ftpURL.indexOf("/", ftpURL.indexOf("@"))));
		
		//Retrieve FTP username and password from properties file XXXgvp.properties
		GVPProperties props = GVPProperties.getInstance();
		String thumbsFTPUser = props.getProperty("akamai.thumbs.ftp.user");
		String thumbsFTPPwd = props.getProperty("akamai.thumbs.ftp.user.password");

		String mediaFTPUser = props.getProperty("akamai.media.ftp.user");
		String mediaFTPPwd = props.getProperty("akamai.media.ftp.user.password");

		//Append @User:Password:PublishTimestamp:Bitrate
		tempFtpUrl.append("@");
		
		//Set appropriate username and password depending on whether format is thumbnails image or media asset
		if(format != null && format.equals("jpeg"))
			tempFtpUrl.append(thumbsFTPUser).append(":").append(thumbsFTPPwd);
		else
			tempFtpUrl.append(mediaFTPUser).append(":").append(mediaFTPPwd);
		
		tempFtpUrl.append(":").append((Long) asset.get("pubDate")).append(":").append(String.valueOf(Long.valueOf(bitrate).longValue()/1000));		
		
		logger.debug("Complete FTP URL::" + tempFtpUrl.toString());
		
		return tempFtpUrl.toString();
		
		}catch(IndexOutOfBoundsException  e){
			throw new AssetUnavailableException(getExceptionMessage(assetType,bitrate,format),e);			
		}
	}
	
	private String getExceptionMessage(String assetType, String bitrate, String format){
		return new StringBuffer()
			.append("Asset Unavailable: ")
			.append((null != assetType)?"Type["+assetType+"] ":"")
			.append((null != bitrate)?"BitRate["+bitrate+"] ":"")
			.append((null != format)?"Format["+format+"] ":"")
			.toString();
	}

	public void setVideoXmlDocument(Document document) {
		videoXmlDocument = document;
	}
}