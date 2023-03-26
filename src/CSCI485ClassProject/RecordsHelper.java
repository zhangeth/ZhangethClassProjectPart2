package CSCI485ClassProject;

import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RecordsHelper {
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

        /*
        System.out.println("attr Name set");
        for (String s : attrNames)
        {
            System.out.println(s);
        }
        System.out.println();

        for (String s : tbmAttrSet)
        {
            System.out.println(s);
        }

         */
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


}
