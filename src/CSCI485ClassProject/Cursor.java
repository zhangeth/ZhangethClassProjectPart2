package CSCI485ClassProject;

import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;

import java.util.ArrayList;
import java.util.List;

public class Cursor {
  public enum Mode {
    READ,
    READ_WRITE
  }
  private Database db;
  private DirectorySubspace recordsSubspace;
  private String tableName;
  private List<String> recordsPath;
  private TableMetadata tbm;
  private Mode mode;

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
    Transaction tx = FDBHelper.openTransaction(db);
    // get subspace
    recordsSubspace = FDBHelper.createOrOpenSubspace(tx, recordsPath);
    // by default start at beginning of records
    startAtBeginning = true;
    // make table meta data object for ease of access
    tbm = RecordsHelper.convertNameToTableMetaData(db, tx, tableName);

    System.out.println("Succcessfully made cursor");
    tx.close();
  }

  public Record goToFirst()
  {
    System.out.println("starting go to first");
    Transaction tx = FDBHelper.openTransaction(db);
    // open all kv pairs
    List<FDBKVPair> pairs = FDBHelper.getAllKeyValuePairsOfSubdirectory(db, tx, recordsPath);

   // get meta data
    TableMetadataTransformer tbmTransformer = new TableMetadataTransformer(tableName);
    List<String> attrPath = tbmTransformer.getTableAttributeStorePath();
    List<FDBKVPair> attrs = FDBHelper.getAllKeyValuePairsOfSubdirectory(db, tx, attrPath);

    for (FDBKVPair p : attrs)
    {
      System.out.println("key: " + p.getKey().toString());
      System.out.println("value: " + p.getValue().toString());
    }

    List<String> attributes = new ArrayList<>(tbm.getAttributes().keySet());

    // iterate through and make map attribute map for record, with key tuple defined by metadata, and value data is non-primary key attributes
    // structure of meta data is not sorted, so need to go through and by order see what primary keys are mapped to which values

    // first fdbkvPair
    Record rec = new Record();
    // convert according to type
    FDBKVPair firstRecord = pairs.get(0);
    // make ordering form metatable

    // print firstRecord for now

    System.out.println(firstRecord.getKey().toString() + " this is first record key");
    System.out.println(firstRecord.getValue().toString() + "first record Value");

    // make primary attr t
    tx.close();


    // make the first record

    return null;
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
