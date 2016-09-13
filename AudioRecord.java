package com.apple.siri.audioapi;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ByteBufferInputStream;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.*;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

public class AudioRecord {
  private static final Logger logger = LoggerFactory.getLogger(AudioRecord.class);
  private final long id;
  private final String audioId;
  private AudioRecordDAO dao = null;
  private boolean isHbaseEvent = false;
  private Result audioResult = null;
 // private SpeechAudioRecord speechAudioRecord = new SpeechAudioRecord();
  public byte[] audioBytes;
  public String language;
  public String sessionID;
  public String codec;
  private final String columnFamily = ConfigHandler.getClientMetaConfig().getProperty("columnFamily","a");

  public AudioRecord(String audioId, String language, String codec, byte[] audioBytes) throws IOException {
    this.id = 0;
    this.audioId    = audioId;
    this.audioBytes = audioBytes;
    this.language   = language;
    this.codec      = codec;
    this.sessionID  = this.audioId.toString();

    try {
      this.dao = new AudioRecordDAO();
    } catch(Exception e) {
      throw new IOException("Couldn't create connection to HBase", e);
    }
  }

  public AudioRecord(long id, String audioId) throws IOException {
    this.id = id;
    this.audioId = audioId;

    // Get event payload from HBase if this is a UUID event
    logger.info("Creating new audio record for " + audioId);
    if (this.audioId.matches("[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}")) {
      logger.info("Fetching " + this.audioId + " from HBase...");
      this.isHbaseEvent = true;
      try {
        this.dao = new AudioRecordDAO();
        audioResult = this.dao.getRow(this);
      } catch (Exception e) {
        throw new IOException("Couldn't get client to HBase", e);
      }
      this.audioBytes = this.getAudioBytes();
      this.language = this.getAudioLanguage();
      this.codec = this.getAudioCodec();
      this.sessionID = this.getAudioSessionID();
    } else {
      logger.info("This appears to be a WOS object");
    }
  }

  public static ResponseEntity<InputStreamResource> deleteAudio(String audioId) {
    AudioRecordDAO staticDao = null;
    try {
      staticDao = new AudioRecordDAO();
    } catch (Exception e) {
      return new ResponseEntity<>(null, null, HttpStatus.INTERNAL_SERVER_ERROR);
    }

    try {
      staticDao.deleteRow(audioId);
    } catch (Exception e) {
      return new ResponseEntity<>(null, null, HttpStatus.NOT_FOUND);
    }

    return new ResponseEntity<>(null, null, HttpStatus.OK);
  }

  public ResponseEntity<InputStreamResource> getResponseEntity() {
    logger.info("Creating response for " + this.audioId + " isHbaseEvent? " + isHbaseEvent);
    if (this.isHbaseEvent) {
      return getHBaseResponseEntity();
    } else {
      return getWosResponseEntity();
    }
  }

  private ResponseEntity<InputStreamResource> getHBaseResponseEntity() {
    HttpHeaders httpHeaders = new HttpHeaders();
    ByteBufferInputStream byteBufferInputStream = null;

    try {
      byteBufferInputStream = new ByteBufferInputStream(ByteBuffer.wrap(this.audioBytes));
    } catch (Exception e) {
      // return some 404/500 response
      return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    }

    httpHeaders.setContentType(MediaType.APPLICATION_OCTET_STREAM);
    httpHeaders.setContentLength(this.audioBytes.length);
    httpHeaders.add("x-ddn-oid", audioId);
    httpHeaders.add("x-ddn-meta", "\"codec\":\"" + this.codec + "\", \"language\":\"" + this.language + "\", \"sessionid\":\"" + this.sessionID + "\"");
    httpHeaders.add("x-ddn-length", new Long(this.audioBytes.length).toString());
    httpHeaders.add("x-ddn-status", "0 ok");

    httpHeaders.setContentLength(this.audioBytes.length);
    return new ResponseEntity<>(new InputStreamResource(byteBufferInputStream), httpHeaders, HttpStatus.OK);
  }

  private ResponseEntity<InputStreamResource> getWosResponseEntity() {
    Properties wosProperties = null;
    try {
      wosProperties = ConfigHandler.getWosMetaConfig();
    } catch (Exception e) {
      logger.error("Unable to get configuration for WOS connectivity!");
      return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    for(String wosUri : wosProperties.getProperty("wos.rest_endpoint_uri").split(",") ) {
      String requestUri = wosUri + "/objects/" + audioId;
      logger.info("GET " + requestUri);

      RestTemplate restTemplate = new RestTemplate();
      ByteBufferInputStream byteBufferInputStream = null;
      ResponseEntity<byte[]> fetchResponse = null;
      try {
        fetchResponse = restTemplate.exchange(requestUri, HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), byte[].class);
      } catch (HttpClientErrorException e) {
        logger.warn("Returned " + e.getLocalizedMessage() + " for " + requestUri);
        continue;
      }

      if (fetchResponse != null && fetchResponse.getStatusCode() == HttpStatus.OK) {
        try {
          byteBufferInputStream = new ByteBufferInputStream(ByteBuffer.wrap(fetchResponse.getBody()));
        } catch (Exception e) {
          // return some 404/500 response
          return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<>(new InputStreamResource(byteBufferInputStream), fetchResponse.getHeaders(), fetchResponse.getStatusCode());
      } else {
        return new ResponseEntity<>(HttpStatus.NOT_FOUND);
      }
    }
    return new ResponseEntity<>(HttpStatus.NOT_FOUND);
  }

  public ResponseEntity<String> save() throws IOException {
    Result result = this.dao.getRow(this);
    if (!result.isEmpty()) {
      throw new IOException("A record with this ID already exists!");
    }

    try {
      this.dao.writeAudioRecord(this);
    } catch (Exception e) {
      return new ResponseEntity<String>("Unable to post new audio", HttpStatus.EXPECTATION_FAILED);
    }
    return new ResponseEntity("Success:" + this.audioId, HttpStatus.ACCEPTED);
  }

  public byte[] rowKey() {
    return this.audioId.getBytes();
  }

  private byte[] getAudioBytes() throws IOException {
    return this.audioResult.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes("audio"));
  }

  private String getAudioCodec() throws IOException {
    return new String(this.audioResult.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes("codec")));
  }

  private String getAudioSessionID() throws IOException {
    return new String(this.audioResult.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes("sessionID")));
  }

  private String getAudioLanguage() throws IOException {
    return new String(this.audioResult.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes("language")));
  }
}
