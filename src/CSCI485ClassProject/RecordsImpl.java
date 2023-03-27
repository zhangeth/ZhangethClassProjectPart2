package CSCI485ClassProject;

import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.*;
import com.apple.foundationdb.directory.Directory;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import jdk.net.SocketFlow;

import java.awt.image.AreaAveragingScaleFilter;
import java.util.*;
import java.util.concurrent.ExecutionException;

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

    // check parameters
    List<String> tableSubdirectory = new ArrayList<>();
    tableSubdirectory.add(tableName);

    if (!FDBHelper.doesSubdirectoryExists(tx, tableSubdirectory))
      return StatusCode.TABLE_NOT_FOUND;

    // get tableMetaData
    TableMetadata tbm = RecordsHelper.convertNameToTableMetaData(db, tx, tableName);

    // compare primaryKeys
    if (primaryKeys.length != primaryKeysValues.length || !RecordsHelper.arePrimaryKeysValid(primaryKeys, tbm)) {
      return StatusCode.DATA_RECORD_PRIMARY_KEYS_UNMATCHED;
    }

    // compare attributes
    if (attrNames.length != attrValues.length ) {
      return StatusCode.DATA_RECORD_CREATION_ATTRIBUTES_INVALID;
    }

    // compare attribute types
    HashMap<String, AttributeType> attrMap = tbm.getAttributes();

    for (int i = 0; i < attrNames.length; i++)
    {
      AttributeType attrType = attrMap.get(attrNames[i]);
      if (    !(attrType == AttributeType.INT && (attrValues[i] instanceof Integer || attrValues[i] instanceof Long)) &&
              !(attrType == AttributeType.VARCHAR && attrValues[i] instanceof String) &&
              !(attrType == AttributeType.DOUBLE && (attrValues[i] instanceof Double))
          )
      {
        FDBHelper.abortTransaction(tx);
        return StatusCode.DATA_RECORD_CREATION_ATTRIBUTE_TYPE_UNMATCHED;
      }
    }

    if (!RecordsHelper.areAttributesValid(attrNames, tbm))
    {
      return StatusCode.DATA_RECORD_CREATION_ATTRIBUTES_INVALID;
    }
    tx.close();
    // start creating record: make records subdir under table dir
    Transaction createTX = FDBHelper.openTransaction(db);

    List<String> recordsPath = new ArrayList<>();
    recordsPath.add(tableName); recordsPath.add("records");

    DirectorySubspace recordSubspace = DirectoryLayer.getDefault().createOrOpen(createTX, recordsPath).join();

    // make key Tuple
    Tuple keyTuple = new Tuple();
    for (Object key : primaryKeysValues)
      keyTuple.addObject(key);

    // make value Tuple
    Tuple valueTuple = new Tuple();
    for (Object value : attrValues)
      valueTuple.addObject(value);

    createTX.close();
    Transaction ts = db.createTransaction();
    Transaction readTx = db.createTransaction();
    // commit key and value tuples to db
    // check if key exists
    if (FDBHelper.getCertainKeyValuePairInSubdirectory(recordSubspace, readTx, keyTuple, recordsPath) == null)
    {
      FDBHelper.setFDBKVPair(recordSubspace, ts, new FDBKVPair(recordsPath, keyTuple, valueTuple));
      int counter = 0;
      FDBHelper.tryCommitTx(ts, 0);
      ts.close();
    }

    readtx.close();


    // print existing records

    Transaction t = db.createTransaction();

    try {
      List<FDBKVPair> pairs = FDBHelper.getAllKeyValuePairsOfSubdirectory(db, t, recordsPath);
      for (FDBKVPair p : pairs)
      {
        System.out.println("added pair: " + p.getKey().toString());
      }
    }  catch (Exception e) {
      System.out.println(e);
    }

    t.close();

    return StatusCode.SUCCESS;
  }

  @Override
  public Cursor openCursor(String tableName, String attrName, Object attrValue, ComparisonOperator operator, Cursor.Mode mode, boolean isUsingIndex) {
    // make transaction that opens the correct table
    Transaction tx = FDBHelper.openTransaction(db);
    // find table

    // FDBHelper.getAllDirectSubspaceName()
    return null;
  }

  // structure of record:  public HashMap<String, Value> getMapAttrNameToValue() {
  //    return mapAttrNameToValue;
  //  }
  @Override
  public Cursor openCursor(String tableName, Cursor.Mode mode) {

    // check if table exists
    if (!RecordsHelper.doesTableExists(db, tableName))
    {
      System.out.println("table doesn't exist");
      return null;
    }
    // make cursor
    return new Cursor(tableName, mode, db);
  }

  @Override
  public Record getFirst(Cursor cursor) {

    //DirectorySubspace dir = FDBHelper.createOrOpenSubspace(tx, path);
    return cursor.goToFirst();
  }

  @Override
  public Record getLast(Cursor cursor) {
    return cursor.goToLast();
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
