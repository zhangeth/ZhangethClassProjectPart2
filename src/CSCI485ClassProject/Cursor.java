package CSCI485ClassProject;

import CSCI485ClassProject.models.Record;
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
    tx.close();

    startAtBeginning = true;
  }

  public Record goToFirst()
  {
    Transaction tx = FDBHelper.openTransaction(db);
    FDBHelper.getAllKeyValuePairsOfSubdirectory(db, tx, recordsPath);
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
