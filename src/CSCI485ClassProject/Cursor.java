package CSCI485ClassProject;

import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.models.ComparisonOperator;
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

import static com.apple.foundationdb.ReadTransaction.ROW_LIMIT_UNLIMITED;

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

  private String attrToParse;
  private Object threshold;
  private KeyValue currentKeyValue;
  private Object currentPrimaryValue;
  private ComparisonOperator operator;
  private AsyncIterable<KeyValue> iterable;
  private AsyncIterator<KeyValue> iterator;
  private boolean goingForward;
  private boolean eof = false;
  private boolean isUsingIndex = false;

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

    System.out.println("Successfully made cursor");
  }

  Cursor(String tableName, String attrToParse, Object threshold, ComparisonOperator operator, Cursor.Mode mode, boolean isUsingIndex, Database db)
  {
      this(tableName, mode, db);
      this.isUsingIndex = isUsingIndex;
      this.attrToParse = attrToParse;
      this.threshold = threshold;
      this.operator = operator;
  }


  private void setCurrentKeyToNext()
  {
    currentKeyValue = iterator.next();
    // SSN value of current record
    currentPrimaryValue = convertKeyValueToFDBKVPair(currentKeyValue).getKey().get(1);

    System.out.println("First record value: " + currentPrimaryValue.toString());
    System.out.println("First record value: " + convertKeyValueToFDBKVPair(currentKeyValue).getKey().get(1).toString());
  }
  private void initializeIterable()
  {
    this.iterable = cursorTx.getRange(recordsSubspace.range(), ROW_LIMIT_UNLIMITED, !goingForward);
    this.iterator = iterable.iterator();

    setCurrentKeyToNext();
  }

  public Record goToFirst()
  {
    System.out.println("starting go to first");
    goingForward = true;
    initializeIterable();
    // get all the keyValues that start with same primary value
    return makeRecordFromCurrentKey();
  }

  private Record makeRecordFromCurrentKey()
  {
    if (!eof)
    {
      Record rec = new Record();
      FDBKVPair kvPair = convertKeyValueToFDBKVPair(currentKeyValue);

      // first object is table key, second is primaryKeyValue, third is attribute name
      List<Object> keyObjects = kvPair.getKey().getItems();

      System.out.println("makin record: " + keyObjects.get(1).toString());

      while (keyObjects.get(1).equals(currentPrimaryValue))
      {
        rec.setAttrNameAndValue((String) keyObjects.get(2), kvPair.getValue().get(0));
        // System.out.println("adding attr: " + keyObjects.get(2).toString());
        if (!iterator.hasNext())
        {
          System.out.println("reached EOF");
          eof = true;
          return rec;
        }
        currentKeyValue = iterator.next();
        kvPair = convertKeyValueToFDBKVPair(currentKeyValue);
        keyObjects = kvPair.getKey().getItems();
      }
      // set to next key

      currentPrimaryValue = convertKeyValueToFDBKVPair(currentKeyValue).getKey().get(1);

      return rec;
    }

    return null;
  }


  public Record goToLast()
  {
    goingForward = false;
    initializeIterable();
    return makeRecordFromCurrentKey();
  }

  public Record getPrev()
  {
    return makeRecordFromCurrentKey();
  }

  public Record getNext()
  {
    Record res = makeRecordFromCurrentKey();

    if (operator != null)
    {
      if (operator == ComparisonOperator.GREATER_THAN_OR_EQUAL_TO)
      {
        System.out.println("comparing greater or equal to");
        // get type of butt crack
        AttributeType attrType =  res.getTypeForGivenAttrName(attrToParse);
        if (attrType == AttributeType.INT)
        {
          if (Integer.valueOf(res.getValueForGivenAttrName(attrToParse).toString()) >= Integer.valueOf(threshold.toString())) {
            return res;
          }
        }
        else if (attrType == AttributeType.DOUBLE)
        {
          if (Double.valueOf(res.getValueForGivenAttrName(attrToParse).toString()) >= Double.valueOf(threshold.toString())) {
            return res;
          }
        }
        else{
          if (res.getValueForGivenAttrName(attrToParse).toString().compareTo(threshold.toString()) > 0)
          {
            return res;
          }
        }
      }
      else if (operator == ComparisonOperator.GREATER_THAN)
      {

      }
      else if (operator == ComparisonOperator.EQUAL_TO)
      {

      }
      else if (operator == ComparisonOperator.LESS_THAN)
      {

      }
      else if (operator == ComparisonOperator.LESS_THAN_OR_EQUAL_TO)
      {

      }
    }
    else {
      return res;
    }

    return getNext();
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
    Tuple keyTuple = Tuple.fromBytes(kv.getKey());
    Tuple valueTuple = Tuple.fromBytes(kv.getValue());

    return new FDBKVPair(recordsPath, keyTuple, valueTuple);
  }

  private Record convertFDBKVPairToRecord(FDBKVPair kv)
  {
    Record rec = new Record();
    return rec;
  }


}
