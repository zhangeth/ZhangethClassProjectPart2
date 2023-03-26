package CSCI485ClassProject;

import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.Record;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.directory.Directory;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.awt.image.AreaAveragingScaleFilter;
import java.util.ArrayList;
import java.util.List;

public class RecordsImpl implements Records{

  private Database db;

  // constructor
  public RecordsImpl()
  {
    db = FDBHelper.initialization();
  }

  @Override
  public StatusCode insertRecord(String tableName, String[] primaryKeys, Object[] primaryKeysValues, String[] attrNames, Object[] attrValues) {
    Transaction tx = FDBHelper.openTransaction(db);

    List<String> tableSubdirectory = new ArrayList<>();
    tableSubdirectory.add(tableName);

    if (!FDBHelper.doesSubdirectoryExists(tx, tableSubdirectory))
      return StatusCode.TABLE_NOT_FOUND;

    // check parameters
    if (primaryKeysValues.length != primaryKeys.length || attrNames.length != attrValues.length)
      return StatusCode.DATA_RECORD_CREATION_ATTRIBUTES_INVALID;

    // make tuple to insert
    TableMetadataTransformer transformer = new TableMetadataTransformer(tableName);
    DirectorySubspace tableAttrSpace = FDBHelper.createOrOpenSubspace(tx, transformer.getTableAttributeStorePath());

    List<String> tblAttributeDirPath = transformer.getTableAttributeStorePath();



    // sample read
    Transaction readTX = db.createTransaction();

    List<FDBKVPair> kvPairs = FDBHelper.getAllKeyValuePairsOfSubdirectory(db, readTX, tblAttributeDirPath);

    for (FDBKVPair pair : kvPairs)
    {
      System.out.print("pair key: " + pair.getKey().toString());
      System.out.println(" value : " + pair.getValue().toString());
    }

    readTX.close();

    Transaction createTX = db.createTransaction();
    // check table metadata
    List<String> recordsPath = new ArrayList<>();
    recordsPath.add(tableName);
    recordsPath.add("records");



    DirectorySubspace recordSubspace = DirectoryLayer.getDefault().createOrOpen(createTX, recordsPath).join();
    createTX.commit();

    Transaction newPair = FDBHelper.openTransaction(db);
    // make record
    Tuple primaryTuple = new Tuple();
    // make key value pair Tuple
    for (int i = 0 ; i < primaryKeysValues.length; i++)
    {
      primaryTuple.addObject(primaryKeysValues[i]);
    }

    Tuple valueTuple = new Tuple();

    for (int i = 0; i < attrValues.length; i++)
    {
      valueTuple.addObject(attrValues[i]);
    }
    newPair.set(recordSubspace.pack(primaryTuple), valueTuple.pack());
    // open subdirectory records
    newPair.commit();
    newPair.close();

    // print
    Transaction t = db.createTransaction();
    System.out.println("Records exists?: " + FDBHelper.doesSubdirectoryExists(t, recordsPath));
    List<FDBKVPair> newPairs = FDBHelper.getAllKeyValuePairsOfSubdirectory(db, t, recordsPath);

    for (FDBKVPair pair : newPairs)
    {
      System.out.print("pair key: " + pair.getKey().toString());
      System.out.println(" value : " + pair.getValue().toString());
    }
    t.close();

    // make key value  pair for each attribute

    // collect all into key value record to add to the subdirectory

    return null;
  }

  @Override
  public Cursor openCursor(String tableName, String attrName, Object attrValue, ComparisonOperator operator, Cursor.Mode mode, boolean isUsingIndex) {
    // make transaction that opens the correct table
    Transaction tx = FDBHelper.openTransaction(db);
    // find table

    // FDBHelper.getAllDirectSubspaceName()
    return null;
  }

  @Override
  public Cursor openCursor(String tableName, Cursor.Mode mode) {
    Cursor cursor = new Cursor();

    Transaction tx = FDBHelper.openTransaction(db);
    // find table
    // FDBHelper.
    return cursor;
  }

  @Override
  public Record getFirst(Cursor cursor) {
    return null;
  }

  @Override
  public Record getLast(Cursor cursor) {
    return null;
  }

  @Override
  public Record getNext(Cursor cursor) {
    return null;
  }

  @Override
  public Record getPrevious(Cursor cursor) {
    return null;
  }

  @Override
  public StatusCode updateRecord(Cursor cursor, String[] attrNames, Object[] attrValues) {
    return null;
  }

  @Override
  public StatusCode deleteRecord(Cursor cursor) {
    return null;
  }

  @Override
  public StatusCode commitCursor(Cursor cursor) {
    return null;
  }

  @Override
  public StatusCode abortCursor(Cursor cursor) {
    return null;
  }

  @Override
  public StatusCode deleteDataRecord(String tableName, String[] attrNames, Object[] attrValues) {
    return null;
  }
}
