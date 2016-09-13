package com.apple.siri.audioapi;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by blindblom on 4/10/16.
 */
@Repository
public class AudioRecordDAO {
  private static Connection connection = null;
  private Properties clientMetaConfig = ConfigHandler.getClientMetaConfig();
  private final String table = clientMetaConfig.getProperty("table", "audio_archive");
  private final String columnFamily = clientMetaConfig.getProperty("columnFamily","a");
  private Get row = null;

  public AudioRecordDAO () throws IOException {
    if (connection == null){
      connection = ConnectionFactory.createConnection(ConfigHandler.getHBaseConfig());
    }
  }

  // Get value of specific column qualifier, caching Get result to speed up subsequent lookups
  public byte[] getColumnIdentifier(AudioRecord record, String identifier) throws IOException {
    Table t = connection.getTable(TableName.valueOf(table));
    if (row == null || !row.getRow().equals(record.rowKey())) {
      row = new Get(record.rowKey());
    }
    return t.get(row).getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(identifier));
    //return t.get(new Get(record.rowKey())).getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(identifier));
  }

  // Create a new Put request containing columns to add audio record
  public void writeAudioRecord(AudioRecord audioRecord) throws IOException {
    Table t = connection.getTable(TableName.valueOf(table));
    Put put = new Put(audioRecord.rowKey());
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("sessionID"), Bytes.toBytes(audioRecord.sessionID));
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("codec"), Bytes.toBytes(audioRecord.codec));
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("language"), Bytes.toBytes(audioRecord.language));
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("audio"), audioRecord.audioBytes);

    t.put(put);
  }

  // Fetch HBase.Result object for given record.rowKey()
  public Result getRow(AudioRecord record) throws IOException {
    Table t = connection.getTable(TableName.valueOf(table));
    return t.get(new Get(record.rowKey()));
  }

  // Delete the given row from the table
  public void deleteRow(String audioId) throws IOException {
    Delete d = new Delete(Bytes.toBytes(audioId));
    Table t = connection.getTable(TableName.valueOf(table));
    t.delete(d);
  }
}
