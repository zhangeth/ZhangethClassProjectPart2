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
    List<String> recordsSubdirectory = new ArrayList<>();
    recordsSubdirectory.add(tableName);
    recordsSubdirectory.add("records");

    recordsPath = recordsSubdirectory;

    this.tableName = tableName;
    this.mode = mode;

    Transaction tx = FDBHelper.openTransaction(db);
    // get subspace
    recordsSubspace = FDBHelper.createOrOpenSubspace(tx, recordsPath);

    startAtBeginning = true;

    tbm = RecordsHelper.convertNameToTableMetaData(db, tx, tableName);
    tx.close();
  }

  public Record goToFirst()
  {
    Transaction tx = FDBHelper.openTransaction(db);
    List<FDBKVPair> records = FDBHelper.getAllKeyValuePairsOfSubdirectory(db, tx, recordsPath);

    List<String> attributes = new ArrayList<>(tbm.getAttributes().keySet());
    // List<String> primaryKeys =

    // iterate through and get first thing

    Record res = new Record();
    // convert according to type
    records.get(0).getKey();



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
