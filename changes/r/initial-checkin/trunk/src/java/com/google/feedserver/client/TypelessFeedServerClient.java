/* Copyright 2008 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.feedserver.client;

import com.google.gdata.client.GoogleService;
import com.google.gdata.data.Entry;
import com.google.gdata.data.Feed;
import com.google.gdata.data.OtherContent;
import com.google.gdata.util.ServiceException;
import com.google.inject.Inject;
import com.google.feedserver.util.FeedServerClientException;
import com.google.feedserver.util.XmlUtil;

import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

/**
 * Implements a Gdata feed client that represents feeds as generic maps of String->String pairs.
 * 
 * @author rayc@google.com (Ray Colline)
 */
public class TypelessFeedServerClient {
  
  // Logging instance
  private static final Logger log = Logger.getLogger(TypelessFeedServerClient.class);
  
  // Dependencies
  private GoogleService service; 
  
  /**
   * Creates the client using provided dependencies.
   * 
   * @param service the configured Gdata service.
   */
  @Inject
  public TypelessFeedServerClient(GoogleService service) {
    this.service = service;
  }
  
  /**
   * Fetches generic "payload-in-content" feed to a Map.  The returned map is a 
   * "map of lists of strings" where the lists or strings represent values.  
   * For non-repeatable elements, the list will only have one value.  The keys of this map 
   * are the element names.
   * 
   * @param feedUrl the feed URL which can contain any valid ATOM "query"
   * @return a map of lists of strings representing the "payload-in-content" entry.
   * @throws FeedServerClientException if we cannot contact the feedserver, fetch the URL, or 
   * parse the XML.
   */
  public Map<String, List<String>> getEntry(URL feedUrl) throws FeedServerClientException {
    try {
      Entry entry = service.getEntry(feedUrl, Entry.class);
      return getEntryMap(entry);
    } catch (IOException e) {
      throw new FeedServerClientException("Error while fetching " + feedUrl, e);
    } catch (ServiceException e) {
      throw new FeedServerClientException(e);
    }
  }
  
  /**
   * Fetches generic "payload-in-content" entries for the given feed query and returns them
   * as a list of maps. Each entry in the map is one entry returned by the feed and
   * they can be consumed without any knowledge of the schema for the feed.  See 
   * {@link TypelessFeedServerClient#getEntry(URL)} for description of the maps returned.
   * 
   * @param feedUrl the feed URL which can contain any valid ATOM "query"
   * @return a list of maps representing all the "payload-in-content" entries.
   * @throws FeedServerClientException if we cannot contact the feedserver, fetch the URL, or 
   * parse the XML.
   */
  public List<Map<String, List<String>>> getFeed(URL feedUrl) throws FeedServerClientException {
    // Retrieve Feed from network
    Feed feed;
    try {
      feed = service.getFeed(feedUrl, Feed.class);
    } catch (IOException e) {
      throw new FeedServerClientException("Error while fetching " + feedUrl, e);
    } catch (ServiceException e) {
      throw new FeedServerClientException(e);
    }
    
    // Go through all entries and build the map.
    List<Map<String, List<String>>> feedMap = new ArrayList<Map<String, List<String>>>();
    for (Entry entry : feed.getEntries()) {
      feedMap.add(getEntryMap(entry));
    }
    return feedMap;
  }
  
  /**
   * Deletes entry specified by supplied URL.  This URL must include the full path.
   * 
   * @param feedUrl the full URL to the entry in this feed.
   * @throws FeedServerClientException if any communication issues occur with the feed or the
   * feed ID is invalid or malformed..
   */
  public void deleteEntry(URL feedUrl) throws FeedServerClientException {
    try {
      service.delete(feedUrl);
    } catch (IOException e) {
      throw new FeedServerClientException("Error while deleting " + feedUrl, e);
    } catch (ServiceException e) {
      throw new FeedServerClientException(e);
    }
  }
  
  /**
   * Deletes specified by "name" in supplied entry map.
   * 
   * @param baseUrl Feed url not including ID.
   * @param entry a valid entry map.
   * @throws FeedServerClientException if any communication issues occur with the feed or the
   * feed ID is invalid or malformed.
   */
  public void deleteEntry(URL baseUrl, Map<String, List<String>> entry) throws 
      FeedServerClientException {
    
    try {
      String name = entry.get("name").get(0);
      URL feedUrl = new URL(baseUrl.toString() + "/" + name);
      deleteEntry(feedUrl);
    } catch (NullPointerException e) {
      throw new RuntimeException("entry map does not have 'name' key", e);
    } catch (IndexOutOfBoundsException e) {
      throw new RuntimeException("'name' in entry map is invalid.", e);
    } catch (MalformedURLException e) {
      throw new FeedServerClientException("invalid base URL", e);
    }
  }
  
  /**
   * Deletes each entry in the supplied list of entries.  This makes one request per entry.
   * 
   * @param baseUrl the feed URL not including ID.
   * @param entries a list of valid entries.
   * @throws FeedServerClientException if any communication issues occur with the feed or the
   * feed ID is invalid.
   */
  public void deleteEntries(URL baseUrl, List<Map<String, List<String>>> entries) throws
      FeedServerClientException {
    for (Map<String, List<String>> entry : entries) {
      deleteEntry(baseUrl, entry);
    }
  }
  
  public void updateEntry(URL feedUrl, Map<String, String> entry) {
    // pass 
  }
  
  public void updateEntries(URL feedUrl, List<Map<String, String>> entries) {
    // pass
  }

  public void insertEntry(URL feedUrl, Object bean) {
    // pass 
  }
  
  public void insertEntry(URL feedUrl, Map<String, String> entry) {
    // pass 
  }
  
  public void insertEntries(URL feedUrl, List<Map<String, String>> entries) {
    // pass
  }
  
  /**
   * Helper function that parses entry into an entry map of with string keys and list of string
   * values.  
   * 
   * @param entry the entry to parse.
   * @return the populated map.
   * @throws FeedServerClientException if the XML parse fails.
   */
  private Map<String, List<String>> getEntryMap(Entry entry) throws FeedServerClientException {
    // Get XML and convert to primitive Object map. 
    OtherContent content = (OtherContent) entry.getContent();  
    log.info("Entry info " + content.getXml().getBlob());
    XmlUtil xmlUtil = new XmlUtil();
    Map<String, Object> rawEntryMap;
    try {
      rawEntryMap = xmlUtil.convertXmlToProperties(content.getXml().getBlob());
    } catch (SAXException e) {
      throw new FeedServerClientException(e);
    } catch (IOException e) {
      throw new FeedServerClientException(e);
    } catch (ParserConfigurationException e) {
      throw new RuntimeException(e);
    }
    
    // Convert into more consumable format.
    Map<String, List<String>> entryMap = new HashMap<String, List<String>>();
    for (String key : rawEntryMap.keySet()) {
      List<String> value = new ArrayList<String>();
      if (rawEntryMap.get(key) instanceof Object[]) {
        Object[] rawValues = (Object[]) rawEntryMap.get(key);
        for (Object rawValue : rawValues) {
          value.add((String) rawValue);
        }
      } else {
        value.add((String) rawEntryMap.get(key));
      }
      entryMap.put(key, value);
    }
    return entryMap;
  }
}
