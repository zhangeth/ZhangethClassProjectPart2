package CSCI485ClassProject.models;

import CSCI485ClassProject.StatusCode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;


/**
 * TableMetadata defines the view of the table's metadata in system.
 */
public class TableMetadata {

  // Map from AttributeName to AttributeType
  private HashMap<String, AttributeType> attributes;

  // A list contains names of the primary key attribute.
  private Set<String> primaryKeys;

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TableMetadata table = (TableMetadata) o;
    return Objects.equals(attributes, table.attributes) && Objects.equals(primaryKeys, table.primaryKeys);
  }

  @Override
  public int hashCode() {
    return Objects.hash(attributes, primaryKeys);
  }

  public TableMetadata() {
    attributes = new HashMap<>();
    primaryKeys = new HashSet<>();
  }

  public TableMetadata(String[] attributeNames, AttributeType[] attributeTypes, String[] primaryKeys) {
    attributes = new HashMap<>();
    for (int i = 0; i < attributeTypes.length; i++) {
      attributes.put(attributeNames[i], attributeTypes[i]);
    }
    this.primaryKeys = new HashSet<>();
    this.primaryKeys.addAll(Arrays.asList(primaryKeys));
  }

  public boolean doesAttributeExist(String attributeName) {
    return attributes.containsKey(attributeName);
  }

  public void addAttribute(String attributeName, AttributeType attributeType) {
    attributes.put(attributeName, attributeType);
  }

  public HashMap<String, AttributeType> getAttributes() {
    return attributes;
  }

  public void setAttributes(HashMap<String, AttributeType> attributes) {
    this.attributes = attributes;
  }

  public List<String> getPrimaryKeys() {
    return new ArrayList<>(primaryKeys);
  }

  public Set<String> getPrimaryKeysAsSet(){ return primaryKeys;}

  public StatusCode setPrimaryKeys(List<String> primaryKeys) {
    for (String pk : primaryKeys) {
      if (!attributes.containsKey(pk)) {
        return StatusCode.ATTRIBUTE_NOT_FOUND;
      }
    }

    this.primaryKeys.addAll(primaryKeys);
    return StatusCode.SUCCESS;
  }

}