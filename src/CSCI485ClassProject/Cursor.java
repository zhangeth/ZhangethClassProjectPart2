package CSCI485ClassProject;

import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.ByteArrayUtil;


import java.sql.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Cursor {
  public enum Mode {
    READ,
    READ_WRITE
  }
  private Database db;
  private DirectorySubspace recordsSubspace;
  private Transaction cursorTx;
  private String tableName;
  private List<String> recordsPath;
  private Mode mode;
  private final int readLimit = 3;

  private int count;

  private Object currentRecord;
  private AsyncIterable<KeyValue> iterable;
  private AsyncIterator<KeyValue> iterator;
  private boolean goingForward;

  // your code here cursor table it's bound to, starting point, current point
  // constructor
  Cursor(String tableName, Mode mode, Database db){
    // store records subdir
    List<String> recordsSubdirectory = new ArrayList<>();
    recordsSubdirectory.add(tableName);
    recordsSubdirectory.add("records");

    recordsPath = recordsSubdirectory;
    // set name and mode
    this.tableName = tableName;
    this.mode = mode;
    this.db = db;

    // get subspace of path
    this.cursorTx = FDBHelper.openTransaction(db);
    // get subspace
    recordsSubspace = FDBHelper.createOrOpenSubspace(cursorTx, recordsPath);
    // by default start at beginning of records
    goingForward = true;

    this.iterable = cursorTx.getRange(recordsSubspace.range());
    this.iterator = iterable.iterator();

    FDBKVPair kvPair = convertKeyValueToFDBKVPair(iterator.next());

    currentRecord = kvPair.getKey().getItems().get(0);

    count = 0;

    System.out.println("Successfully made cursor");
  }

  /*
  private void initializeIterableAndIterator()
  {
    this.iterable = cursorTx.getRange(startBytes, endBytes, readLimit, !goingForward);
    this.iterator = iterable.iterator();
  }
  */

  /*
  private void initializeAttrList()
  {
    TableMetadataTransformer tbmTransformer = new TableMetadataTransformer(tableName);
    List<String> attrPath = tbmTransformer.getTableAttributeStorePath();
    List<FDBKVPair> attrs = FDBHelper.getAllKeyValuePairsOfSubdirectory(db, cursorTx, attrPath);

    for (FDBKVPair p : attrs)
    {
      String rawString = p.getKey().toString();
      String attrName = rawString.substring(2, rawString.length() - 2);

      if (!tbm.getPrimaryKeysAsSet().contains(attrName))
      {
        attrNamesInOrder.add(attrName);
        System.out.println("Adding: " + String.valueOf(attrName + " to np Keys in order"));
      }
      // primary keys
      else {
        primaryKeysInOrder.add(attrName);
        System.out.println("Adding: " + String.valueOf(attrName + " to primaryKeys in order"));
      }
    }

   for (String s : tbm.getPrimaryKeys())
    {
      System.out.println("primary key strings " + s);
    }
  }
  */
  public Record goToFirst()
  {
    System.out.println("starting go to first");
    goingForward = true;
    //initializeIterableAndIterator();

    // get all the keyValues that start with same primary value
    Record rec = new Record();

    KeyValue keyValue = iterator.next();
    FDBKVPair kvPair = convertKeyValueToFDBKVPair(keyValue);

    List<Object> keyObjects = kvPair.getKey().getItems();

    while (keyObjects.get(0).equals(currentRecord))
    {
      rec.setAttrNameAndValue(keyObjects.get(1).toString(), kvPair.getValue().toString());
      keyValue = iterator.next();
      keyObjects = convertKeyValueToFDBKVPair(keyValue).getKey().getItems();
    }


    System.out.println("Tuple KeyBytes: " + kvPair.getKey().toString());
    System.out.println("Tuple valueBytes: " + kvPair.getValue().toString());

    // made record
    System.out.println("Made record key: " + rec.getValueForGivenAttrName(keyObjects.get(1).toString()));

    return rec;

  }

  public Record goToLast()
  {
    goingForward = false;

    //initializeIterableAndIterator();

    KeyValue keyValue = iterator.next();

    FDBKVPair kvPair = convertKeyValueToFDBKVPair(keyValue);

    System.out.println("Tuple KeyBytes: " + kvPair.getKey().toString());
    System.out.println("Tuple valueBytes: " + kvPair.getValue().toString());

    return convertFDBKVPairToRecord(kvPair);
  }

  public Record getPrev()
  {
    if (iterator.hasNext())
    {


      //return convertFDBKVPairToRecord(convertKeyValueToFDBKVPair(kv));
    }
    // return EOF
    System.out.println("cursor reached EOF");
    return null;
  }

  public Record getNext()
  {
    return null;
  }

  public StatusCode commit()
  {
    try
    {
      FDBHelper.commitTransaction(cursorTx);
    }
    catch (Exception e)
    {
      System.out.println(e);
    }
    return StatusCode.SUCCESS;
  }
  // helper methods

  private FDBKVPair convertKeyValueToFDBKVPair(KeyValue kv)
  {
    Tuple keyTuple = recordsSubspace.unpack(kv.getKey());
    return FDBHelper.getCertainKeyValuePairInSubdirectory(recordsSubspace, cursorTx, keyTuple, recordsPath);
  }

  private Record convertFDBKVPairToRecord(FDBKVPair kv)
  {
    Record rec = new Record();
    return rec;
  }


}
