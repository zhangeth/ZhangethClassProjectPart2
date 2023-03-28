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

import java.sql.Array;
import java.util.ArrayList;
import java.util.List;

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

  private AsyncIterable<KeyValue> iterable;

  private List<String> attrNamesInOrder;
  private List<String> primaryKeysInOrder;

  private boolean startAtBeginning;


  // place in table

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

    // get subsapce of path
    this.cursorTx = FDBHelper.openTransaction(db);
    // get subspace
    recordsSubspace = FDBHelper.createOrOpenSubspace(cursorTx, recordsPath);
    // by default start at beginning of records
    startAtBeginning = true;
    // make table meta data object for ease of access
    tbm = RecordsHelper.convertNameToTableMetaData(db, cursorTx, tableName);
    attrNamesInOrder = new ArrayList<>();
    primaryKeysInOrder = new ArrayList<>();

    // initialize order of attributes
    initializeAttrList();

    // initialize iterable over range of keys in subspace
    byte[] startBytes = recordsSubspace.pack();
    byte[] endBytes = recordsSubspace.range().end;
    this.iterable = cursorTx.getRange(startBytes, endBytes);

    System.out.println("Succcessfully made cursor");
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

    for (String s : tbm.getPrimaryKeys())
    {
      System.out.println("primary key strings " + s);
    }
  }

  public Record goToFirst()
  {
    System.out.println("starting go to first");

    AsyncIterator<KeyValue> iterator = iterable.iterator();
    KeyValue keyValue = iterator.next();

    Tuple keyTuple = recordsSubspace.unpack(keyValue.getKey());

    FDBKVPair kvPair =  FDBHelper.getCertainKeyValuePairInSubdirectory(recordsSubspace, cursorTx, keyTuple, recordsPath);

    System.out.println("Tuple KeyBytes: " + kvPair.getKey().toString());
    System.out.println("Tuple valueBytes: " + kvPair.getValue().toString());


    // iterate through and make map attribute map for record, with key tuple defined by metadata, and value data is non-primary key attributes
    // structure of meta data is not sorted, so need to go through and by order see what primary keys are mapped to which values

    // first fdbkvPair
    return convertKeyValueToRecord(kvPair);

  }

  public Record goToLast()
  {
    return null;
  }

  public Record getNext()
  {
    return null;
  }

  public Record convertKeyValueToRecord(FDBKVPair kv)
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
    //rec.setAttrNameAndValue()
    // add non-primary attributes
    for (int i = 0; i < attrNamesInOrder.size(); i++)
    {
      rec.setAttrNameAndValue(attrNamesInOrder.get(i), values.get(attrNamesInOrder.size() - 1 - i));
    }

    return rec;
  }


}
