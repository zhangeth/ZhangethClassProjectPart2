package CSCI485ClassProject;

import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import jdk.internal.dynalink.linker.ConversionComparator;


import java.security.Key;
import java.sql.Array;
import java.util.*;

import static com.apple.foundationdb.ReadTransaction.ROW_LIMIT_UNLIMITED;

public class Cursor {
  public enum Mode {
    READ,
    READ_WRITE
  }

  public static Set<ComparisonOperator> equalToOperators = new HashSet<>();

  static {
    equalToOperators.add(ComparisonOperator.EQUAL_TO);
    equalToOperators.add(ComparisonOperator.GREATER_THAN_OR_EQUAL_TO);
    equalToOperators.add(ComparisonOperator.LESS_THAN_OR_EQUAL_TO);
  }
  private Database db;
  private DirectorySubspace recordsSubspace;
  private Transaction cursorTx;
  private String tableName;
  private List<String> recordsPath;
  private Mode mode;

  private String attrToParse;
  private Object threshold;

  private KeyValue prevKeyValue;

  private Object prevPrimaryValue;
  private KeyValue currentKeyValue;
  private Object currentPrimaryValue;

  private List<FDBKVPair> currentRecord;
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
    if (iterator.hasNext())
    {
      currentKeyValue = iterator.next();
      // SSN value of current record
      currentPrimaryValue = convertKeyValueToFDBKVPair(currentKeyValue).getKey().get(0);

      System.out.println("First record value: " + currentPrimaryValue.toString());
      System.out.println("First tuple attr: " + convertKeyValueToFDBKVPair(currentKeyValue).getKey().get(1).toString());
    }
  }

  private void initializeIterable()
  {
    this.iterable = cursorTx.getRange(recordsSubspace.range(), ROW_LIMIT_UNLIMITED, !goingForward);
    this.iterator = iterable.iterator();

    setCurrentKeyToNext();
  }

  public Record goToFirst()
  {
    goingForward = true;
    initializeIterable();
    // get all the keyValues that start with same primary value
    return getNext();
  }

  private Record makeRecordFromCurrentKey()
  {
    if (!eof)
    {
      //if (FDBHelper.getCertainKeyValuePairInSubdirectory(cursorTx, ))
      Record rec = new Record();
      FDBKVPair kvPair = convertKeyValueToFDBKVPair(currentKeyValue);

      // first object is table key, second is primaryKeyValue, third is attribute name
      List<Object> keyObjects = kvPair.getKey().getItems();

      prevKeyValue = currentKeyValue;
      prevPrimaryValue = kvPair.getKey().get(0);

      List<FDBKVPair> newRecord = new ArrayList<>();

      System.out.println("making record: " + keyObjects.get(0).toString());

      while (keyObjects.get(1).equals(currentPrimaryValue))
      {
        newRecord.add(kvPair);

        rec.setAttrNameAndValue((String) keyObjects.get(1), kvPair.getValue().get(0));
        // System.out.println("adding attr: " + keyObjects.get(2).toString());
        if (!iterator.hasNext())
        {
          System.out.println("reached EOF");
          currentRecord = newRecord;
          eof = true;
          return rec;
        }

        currentKeyValue = iterator.next();
        kvPair = convertKeyValueToFDBKVPair(currentKeyValue);
        keyObjects = kvPair.getKey().getItems();
      }
      currentRecord = newRecord;
      // set to next key

      currentPrimaryValue = convertKeyValueToFDBKVPair(currentKeyValue).getKey().get(1);

      for (Map.Entry e : rec.getMapAttrNameToValue().entrySet())
      {
        System.out.println("found rec attr: " + e.getKey() + " value: " + e.getValue().toString());
      }
      //System.out.println(rec.getValueForGivenAttrName("SSN") + " obtained ssn");

      return rec;
    }

    return null;
  }

  public Record goToLast()
  {
    goingForward = false;
    initializeIterable();
    return getNext();
  }

  public Record getPrev()
  {
    return makeRecordFromCurrentKey();
  }

  public Record getNext()
  {
    // add check for thing
    Record res = makeRecordFromCurrentKey();
    if (res == null)
    {
      return null;
    }

    if (operator != null)
    {
      //System.out.println(res.getMapAttrNameToValue().toString());
      if (satisfiesOperator(res))
      {
        System.out.print("returning: " + res.getValueForGivenAttrName("SSN"));
        //System.out.println();
        return res;
      }

      if (eof)
      {
        return null;
      }

      return getNext();

    }
    else {
      return res;
    }

  }

  private boolean satisfiesOperator(Record res)
  {
    // check record has entry for given attr
    if (!res.getMapAttrNameToValue().containsKey(attrToParse))
    {
      return false;
    }

    AttributeType attrType = res.getTypeForGivenAttrName(attrToParse);
    //attrType = RecordsHelper.getType(threshold);

    /*System.out.println("attrParse: " + attrToParse);
    System.out.println("type of attrPars: " + attrType.toString());*/
    //compare ints
    if (attrType == AttributeType.INT)
    {
      System.out.println("Entered int block");
      Integer recordValue = Integer.valueOf(res.getValueForGivenAttrName(attrToParse).toString());
      Integer thresholdInt = Integer.valueOf(threshold.toString());

      // System.out.println("recordVal: " + recordValue + " threshold: " + thresholdInt);

      if (recordValue > thresholdInt) {
        // System.out.println("greater than");
        if (operator == ComparisonOperator.GREATER_THAN || operator == ComparisonOperator.GREATER_THAN_OR_EQUAL_TO)
        {
          // System.out.println("returning true");
          return true;
        }
      }
      else if (recordValue == thresholdInt)
      {

        if (operator == ComparisonOperator.GREATER_THAN_OR_EQUAL_TO || operator == ComparisonOperator.EQUAL_TO || operator == ComparisonOperator.LESS_THAN_OR_EQUAL_TO)
        {
          System.out.println("equal to found");
          return true;
        }
      }
      else
      {
        // System.out.println("greater than");
        if (operator == ComparisonOperator.LESS_THAN || operator == ComparisonOperator.LESS_THAN_OR_EQUAL_TO)
        {
          return true;
        }
        return false;
      }
    }
    // compare doubles
    else if (attrType == AttributeType.DOUBLE)
    {
      System.out.println("Entered double block");
      Double recordValue = Double.valueOf(res.getValueForGivenAttrName(attrToParse).toString());
      Double thresholdDouble = Double.valueOf(threshold.toString());

      if (recordValue > thresholdDouble) {
        if (operator == ComparisonOperator.GREATER_THAN || operator == ComparisonOperator.GREATER_THAN_OR_EQUAL_TO)
          return true;
      }
      else if (recordValue == thresholdDouble)
      {
        if (operator == ComparisonOperator.GREATER_THAN_OR_EQUAL_TO || operator == ComparisonOperator.EQUAL_TO || operator == ComparisonOperator.LESS_THAN_OR_EQUAL_TO)
          return true;
      }
      else
      {
        if (operator == ComparisonOperator.LESS_THAN || operator == ComparisonOperator.LESS_THAN_OR_EQUAL_TO)
          return true;
        return false;
      }
    }
    // compare strings
    else {
      // System.out.println("Entered string block");

      String recordValue = res.getValueForGivenAttrName(attrToParse).toString();
      String thresholdStr = threshold.toString();

      if (recordValue.compareTo(thresholdStr) > 0 ) {
        if (operator == ComparisonOperator.GREATER_THAN || operator == ComparisonOperator.GREATER_THAN_OR_EQUAL_TO)
          return true;
      }
      else if (recordValue.equals(thresholdStr))
      {
        if (operator == ComparisonOperator.GREATER_THAN_OR_EQUAL_TO || operator == ComparisonOperator.EQUAL_TO || operator == ComparisonOperator.LESS_THAN_OR_EQUAL_TO) {
          return true;
        }
      }
      else
      {
        if (operator == ComparisonOperator.LESS_THAN || operator == ComparisonOperator.LESS_THAN_OR_EQUAL_TO)
          return true;
        return false;
      }
    }

      return false;
  }

  public StatusCode deleteRecord()
  {
    if (currentRecord != null)
    {
      System.out.println("deleting: " + currentRecord.get(0).getKey().get(0).toString());
      for (FDBKVPair p : currentRecord)
      {

        System.out.println("deleting attr: " + p.getKey().get(1).toString());
        FDBHelper.removeKeyValuePair(cursorTx, recordsSubspace, p.getKey());
      }

    }
    else {
      System.out.println("curr record is null: ");
    }

    // set to next key

    //currentPrimaryValue = convertKeyValueToFDBKVPair(currentKeyValue).getKey().get(1);

    return StatusCode.SUCCESS;
  }



  public StatusCode commit()
  {
    try
    {
      FDBHelper.commitTransaction(cursorTx);
      mode = null;
      iterator = null;
      mode = null;
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
    Tuple keyTuple = Tuple.fromBytes(kv.getKey()).popFront();
    Tuple valueTuple = Tuple.fromBytes(kv.getValue());

    return new FDBKVPair(recordsPath, keyTuple, valueTuple);
  }

  private Record convertFDBKVPairToRecord(FDBKVPair kv)
  {
    Record rec = new Record();
    return rec;
  }


}
