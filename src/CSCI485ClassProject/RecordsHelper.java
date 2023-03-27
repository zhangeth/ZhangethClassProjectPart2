package CSCI485ClassProject;

import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;

import java.util.*;

public class RecordsHelper {
    public static boolean doesTableExists(Database db, String tableName)
    {
        boolean res = false;
        Transaction tx = FDBHelper.openTransaction(db);
        List<String> tableSubdirectory = new ArrayList<>();
        tableSubdirectory.add(tableName);

        if (FDBHelper.doesSubdirectoryExists(tx, tableSubdirectory))
        {
            res = true;
        }
        tx.close();

        return res;
    }

    public static boolean arePrimaryKeysValid(String[] primaryKeys, TableMetadata tbm)
    {
        // make into sets to negate order
        Set<String> primaryKeySet = new HashSet<>(Arrays.asList(primaryKeys));
        Set<String> tbmKeySet = new HashSet<>(tbm.getPrimaryKeys());

        // compare content of sets
        if (!primaryKeySet.equals(tbmKeySet)){
            return false;
        }
        return true;
    }

    public static boolean areAttributesValid(String[] attrNames, TableMetadata tbm)
    {
        Set<String> attrSet = new HashSet<>(Arrays.asList(attrNames));
        Set<String> tbmAttrSet = new HashSet<>(tbm.getAttributes().keySet());
        // remove primary keys from attr set
        tbmAttrSet.removeAll(new HashSet<>(tbm.getPrimaryKeys()));

        // compare content of sets
        if (!attrSet.equals(tbmAttrSet)){
            return false;
        }
        return true;
    }

    public static TableMetadata convertNameToTableMetaData(Database db, Transaction tx, String tableName)
    {
        TableMetadataTransformer transformer = new TableMetadataTransformer(tableName);
        List<String> tblAttributeDirPath = transformer.getTableAttributeStorePath();
        return transformer.convertBackToTableMetadata(FDBHelper.getAllKeyValuePairsOfSubdirectory(db, tx, tblAttributeDirPath));
    }

    public static List<String> getNonPrimaryAttrs(TableMetadata tbm)
    {
        // Linked Hash to preserve order of elements
        Set<String> nonPrimaryAttrSet = new LinkedHashSet<>(tbm.getAttributes().keySet());
        Set<String> primarySet = new LinkedHashSet<>(tbm.getPrimaryKeys());
        // remove primary keys from attr set
        nonPrimaryAttrSet.removeAll(primarySet);


        return new ArrayList<>(nonPrimaryAttrSet);
    }


}
