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

    // add attributes that don't exist
    TableManagerImpl tbManager = new TableManagerImpl();

    Set<String> addedAttrSet = new HashSet<>();

    if (attrNames.length > (attrMap.keySet().size() - tbm.getPrimaryKeys().size()))
    {
      System.out.println("entered adding attributes");
      addedAttrSet = new HashSet<>(Arrays.asList(attrNames));

      // add attributes that don't exist
      addedAttrSet.removeAll(attrMap.keySet());

      // need index of values to type check
      for (int i = 0; i < attrValues.length; i++)
      {
        if (addedAttrSet.contains(attrNames[i]))
        {
          tbManager.addAttribute(tableName, attrNames[i], RecordsHelper.getType(attrValues[i]));
        }
      }
    }

    /*for (String s: attrMap.keySet())
    {
      System.out.println(s + " attributes in thingy");
    }*/

    for (int i = 0; i < attrNames.length; i++)
    {
      AttributeType attrType = attrMap.get(attrNames[i]);
      if ((!addedAttrSet.contains(attrNames[i])) &&
              (!(attrType == AttributeType.INT && (attrValues[i] instanceof Integer || attrValues[i] instanceof Long)) &&
              !(attrType == AttributeType.VARCHAR && attrValues[i] instanceof String) &&
              !(attrType == AttributeType.DOUBLE && (attrValues[i] instanceof Double)))
          )
      {
        System.out.println(attrType.toString() + " type that failed on");
        System.out.println(attrValues[i] + " provided value");

        FDBHelper.abortTransaction(tx);
        return StatusCode.DATA_RECORD_CREATION_ATTRIBUTE_TYPE_UNMATCHED;
      }
    }

    if (!RecordsHelper.areAttributesValid(attrNames, tbm) && addedAttrSet.isEmpty())
    {
      return StatusCode.DATA_RECORD_CREATION_ATTRIBUTES_INVALID;
    }

    tx.close();
    // start creating record: make records subdir under table dir
    Transaction createTX = FDBHelper.openTransaction(db);

    List<String> recordsPath = new ArrayList<>();
    recordsPath.add(tableName); recordsPath.add("records");

    DirectorySubspace recordSubspace = DirectoryLayer.getDefault().createOrOpen(createTX, recordsPath).join();

    // make key and value tuple for each attribute, with each key having the value of the primary key first
    Object primaryValue = primaryKeysValues[0];
    // add primary Keys first
    for (int i = 0; i < primaryKeys.length; i++)
    {
      Tuple keyTuple = new Tuple().addObject(primaryValue);
      keyTuple = keyTuple.addObject(primaryKeys[i]);

      Tuple valueTuple = new Tuple().addObject(primaryKeysValues[0]);

      if (FDBHelper.getCertainKeyValuePairInSubdirectory(recordSubspace, createTX, keyTuple, recordsPath) == null)
      {
        FDBHelper.setFDBKVPair(recordSubspace, createTX, new FDBKVPair(recordsPath, keyTuple, valueTuple));
      }
    }

    for (int i = 0; i < attrNames.length; i++)
    {
      Tuple keyTuple = new Tuple().addObject(primaryValue);
      keyTuple = keyTuple.addObject(attrNames[i]);

      // make value Tuple
      Tuple valueTuple = new Tuple().addObject(attrValues[i]);

      // set key and value tuples
      if (FDBHelper.getCertainKeyValuePairInSubdirectory(recordSubspace, createTX, keyTuple, recordsPath) == null)
      {
        FDBHelper.setFDBKVPair(recordSubspace, createTX, new FDBKVPair(recordsPath, keyTuple, valueTuple));
      }
    }

    FDBHelper.commitTransaction(createTX);

    createTX.close();


    return StatusCode.SUCCESS;
  }

  @Override
  public Cursor openCursor(String tableName, String attrName, Object attrValue, ComparisonOperator operator, Cursor.Mode mode, boolean isUsingIndex) {
    // make transaction that opens the correct table
    if (!RecordsHelper.doesTableExists(db, tableName))
    {
      System.out.println("table doesn't exist");
      return null;
    }
    // make cursor
    return new Cursor(tableName, attrName, attrValue, operator, mode, isUsingIndex, db);
  }

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
    return cursor.goToFirst();
  }

  @Override
  public Record getLast(Cursor cursor) {
    return cursor.goToLast();
  }

  @Override
  public Record getNext(Cursor cursor) {
    return cursor.getNext();
  }

  @Override
  public Record getPrevious(Cursor cursor) {
    return cursor.getPrev();
  }

  @Override
  public StatusCode updateRecord(Cursor cursor, String[] attrNames, Object[] attrValues) {
    return null;
  }

  @Override
  public StatusCode deleteRecord(Cursor cursor) {
    return cursor.deleteRecord();
  }

  @Override
  public StatusCode commitCursor(Cursor cursor) {
    return cursor.commit();
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
