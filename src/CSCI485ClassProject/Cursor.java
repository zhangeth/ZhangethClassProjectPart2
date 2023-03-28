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
  private TableMetadata tbm;
  private Mode mode;
  private final int readLimit = 3;

  private int count;

  byte[] startBytes;
  byte[] endBytes;
  private AsyncIterable<KeyValue> iterable;
  private AsyncIterator<KeyValue> iterator;
  private List<String> attrNamesInOrder;
  private List<String> primaryKeysInOrder;
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
    // make table meta data object for ease of access
    tbm = RecordsHelper.convertNameToTableMetaData(db, cursorTx, tableName);
    attrNamesInOrder = new ArrayList<>();
    primaryKeysInOrder = new ArrayList<>();

    // initialize order of attributes
    initializeAttrList();

    // initialize iterable over range of keys in subspace
    this.startBytes = recordsSubspace.pack();
    this.endBytes = recordsSubspace.range().end;

    count = 0;

    // needs to initialize iterable through either goToFirst or goToLast
    this.startBytes = recordsSubspace.pack();
    this.endBytes = recordsSubspace.range().end;

    System.out.println("Successfully made cursor");
  }

  private void initializeIterableAndIterator()
  {
    this.iterable = cursorTx.getRange(startBytes, endBytes, readLimit, !goingForward);
    this.iterator = iterable.iterator();
  }

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

/*    for (String s : tbm.getPrimaryKeys())
    {
      System.out.println("primary key strings " + s);
    }*/
  }

  public Record goToFirst()
  {
    System.out.println("starting go to first");
    goingForward = true;
    initializeIterableAndIterator();

    KeyValue keyValue = iterator.next();

    FDBKVPair kvPair = convertKeyValueToFDBKVPair(keyValue);

    System.out.println("Tuple KeyBytes: " + kvPair.getKey().toString());
    System.out.println("Tuple valueBytes: " + kvPair.getValue().toString());

    return convertFDBKVPairToRecord(kvPair);

  }

  public Record goToLast()
  {
    goingForward = false;

    initializeIterableAndIterator();

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
      KeyValue kv = iterator.next();
      count++;
      // load next keys if not at end of subdir yet
      if (count >= (readLimit - 1))
      {
        System.out.println("triggered");
        Tuple lastKey = recordsSubspace.unpack(kv.getKey());
        endBytes = recordsSubspace.pack(lastKey);
        if (!goingForward)
        {
          System.out.println("going backwards");
        }
        iterable = cursorTx.getRange(startBytes, endBytes, readLimit, !goingForward);
        iterator = iterable.iterator();
        iterator.next();

        count = 0;
      }

      return convertFDBKVPairToRecord(convertKeyValueToFDBKVPair(kv));
    }
    // return EOF
    System.out.println("cursor reached EOF");
    return null;
  }

  public Record getNext()
  {
    if (iterator.hasNext())
    {
      KeyValue kv = iterator.next();
      count++;
      // load next keys if not at end of subdir yet
      if (count >= (readLimit - 1) && ByteArrayUtil.compareUnsigned(startBytes, endBytes) < 0)
      {
        Tuple lastKey = recordsSubspace.unpack(kv.getKey());
        startBytes = recordsSubspace.pack(lastKey);

        iterable = cursorTx.getRange(startBytes, endBytes, readLimit, !goingForward);
        iterator = iterable.iterator();
        iterator.next();

        count = 0;
      }
      return convertFDBKVPairToRecord(convertKeyValueToFDBKVPair(kv));
    }
    // return EOF
    System.out.println("cursor reached EOF");
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
    // convert according to type
    Tuple keyTuple = kv.getKey();
    Tuple valueTuple = kv.getValue();

    System.out.println(keyTuple.toString() + "  record key");
    System.out.println(valueTuple.toString() + " Value");

    List<Object> pkValues = keyTuple.getItems();
    List<Object> values = valueTuple.getItems();

    // add primary keys
    for (int i = 0; i < primaryKeysInOrder.size(); i++)
    {
      rec.setAttrNameAndValue(primaryKeysInOrder.get(i), pkValues.get(primaryKeysInOrder.size() - 1 - i));
      System.out.println("primaryKeys printing: " + primaryKeysInOrder.get(i) + " from values read: " + pkValues.get(primaryKeysInOrder.size() - 1 - i));
    }
    // add non-primary attributes
    for (int i = 0; i < attrNamesInOrder.size(); i++)
    {
      rec.setAttrNameAndValue(attrNamesInOrder.get(i), values.get(attrNamesInOrder.size() - 1 - i));
    }
/*    System.out.println("ssn: " + String.valueOf(rec.getValueForGivenAttrName("SSN")));

    System.out.println("Name: " + String.valueOf(rec.getValueForGivenAttrName("Name")));

    for (Map.Entry e : rec.getMapAttrNameToValue().entrySet())
    {
      System.out.println(e.getKey() + " rec Key");
      System.out.println(e.getValue().toString() + " rec Val");
    }*/
    // check record

    return rec;
  }


}
