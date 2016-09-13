package com.apple.siri.audioapi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@RestController
public class AudioRecordController {
  private static final Logger logger = LoggerFactory.getLogger(AudioRecordController.class);
  private final AtomicLong counter = new AtomicLong();

  @RequestMapping(value="/objects/{audioId}", method=RequestMethod.GET)
  public ResponseEntity<InputStreamResource> getHBaseAudio(
      @RequestParam(value="format", defaultValue="speex") String format,
      @PathVariable final String audioId) {
    AudioRecord audioRecord = null;
    logger.info("Fetch " + audioId);
    try {
      audioRecord = new AudioRecord(counter.incrementAndGet(), audioId);
    } catch (Exception e) {
      // return some 404/500 response
      return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    }
    return audioRecord.getResponseEntity();
  }

  @RequestMapping(value="/objects", method=RequestMethod.POST)
  public ResponseEntity<String> writeHBaseAudio(
      @RequestParam(value="audio", required=true) MultipartFile audioBytes,
      @RequestParam(value="codec", required=true) String codec,
      @RequestParam(value="language", required=true) String language) {

    String audioId = UUID.randomUUID().toString().toUpperCase();
    ResponseEntity<String> responseEntity = null;

    AudioRecord audioRecord = null;
    logger.info("Post " + audioId);

    try {
      audioRecord = new AudioRecord(audioId, language, codec, audioBytes.getBytes());
      responseEntity = audioRecord.save();
    } catch (Exception e) {
      // return some 404/500 response
      return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);

    }
    return responseEntity;
  }

  @RequestMapping(value="/objects/{audioId}", method=RequestMethod.POST)
  public ResponseEntity<String> writeHBaseAudio(
      @RequestParam(value="audio", required=true) MultipartFile audioBytes,
      @RequestParam(value="codec", required=true) String codec,
      @RequestParam(value="language", required=true) String language,
      @PathVariable final String audioId) {

    AudioRecord audioRecord = null;
    ResponseEntity<String> responseEntity = null;

    logger.info("Post " + audioId);

    try {
      audioRecord = new AudioRecord(audioId, language, codec, audioBytes.getBytes());
      responseEntity = audioRecord.save();
    } catch (Exception e) {
      // return some 404/500 response
      return new ResponseEntity<String>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
    }
    return responseEntity;
  }

  @RequestMapping(value="/objects/{audioId}", method=RequestMethod.DELETE)
  public ResponseEntity<InputStreamResource> deleteHBaseAudio(
      @PathVariable final String audioId) {
    return AudioRecord.deleteAudio(audioId);
  }
}
