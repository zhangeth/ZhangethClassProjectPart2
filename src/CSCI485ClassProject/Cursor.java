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

    // initialize iterable over range of keys in subspace
    byte[] startBytes = recordsSubspace.pack();
    byte[] endBytes = recordsSubspace.range().end;
    this.iterable = cursorTx.getRange(startBytes, endBytes);

    System.out.println("Succcessfully made cursor");
  }

  public Record goToFirst()
  {
    System.out.println("starting go to first");

    AsyncIterator<KeyValue> iterator = iterable.iterator();
    KeyValue keyValue = iterator.next();

    Tuple keyBytes = recordsSubspace.unpack(keyValue.getKey());
    System.out.println("Tuple KeyBytes: " + keyBytes.toString());

    // iterate through and make map attribute map for record, with key tuple defined by metadata, and value data is non-primary key attributes
    // structure of meta data is not sorted, so need to go through and by order see what primary keys are mapped to which values

    // first fdbkvPair
    Record rec = new Record();
    // convert according to type

/*    System.out.println(firstRecord.getKey().toString() + " this is first record key");
    System.out.println(firstRecord.getValue().toString() + "first record Value");

    List<Object> values = firstRecord.getValue().getItems();
    List<Object> pkValues = firstRecord.getKey().getItems();
    // add primary keys
    for (int i = 0; i < primaryKeysInOrder.size(); i++)
    {
      rec.setAttrNameAndValue(primaryKeysInOrder.get(i), pkValues.get(primaryKeysInOrder.size() - 1 - i));
    }
    //rec.setAttrNameAndValue()
    // add non-primary attributes
    for (int i = 0; i < attrNamesInOrder.size(); i++)
    {
      rec.setAttrNameAndValue(attrNamesInOrder.get(i), values.get(attrNamesInOrder.size() - 1 - i));
    }*/

    // make primary attr t
    tx.close();

    return rec;
  }

  public Record goToLast()
  {
    return null;
  }

  public Record getNext()
  {
    return null;
  }



}
