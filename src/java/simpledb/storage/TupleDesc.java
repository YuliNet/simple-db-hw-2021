package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public Type fieldType;
        
        /**
         * The name of the field
         * */
        public String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public void setFieldType(Type fieldType) {
            this.fieldType = fieldType;
        }

        public Type getFieldType() {
            return fieldType;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public String getFieldName() {
            return fieldName;
        }

        @Override
        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) 
                return true;
            if (o == null || o.getClass() != this.getClass()) 
                return false;
            TDItem obj = (TDItem) o;
            return new EqualsBuilder().append(fieldName, obj.getFieldName())
                                      .append(fieldType, obj.getFieldType())
                                      .isEquals();
                                      
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37).append(fieldType)
                                              .append(fieldName)
                                              .toHashCode();
        }
    }
    private List<TDItem> items;
    private Map<String, Integer> nameToIdxMap; 

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return items.iterator();
    }

    public List<TDItem> tdItems() {
        return this.items;
    }

    public void setTdItems(List<TDItem> items) {
        this.items = items;
    }

    public void setNameToIdxMap(Map<String, Integer> nameToIdxMap) {
        this.nameToIdxMap = nameToIdxMap;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        if (this.items == null) {
            this.items = new ArrayList<>();
        }
        if (this.nameToIdxMap == null) {
            this.nameToIdxMap = new HashMap<>();
        }
        for (int i = 0, n = typeAr.length; i < n; i ++) {
            if (typeAr[i] == null || fieldAr[i] == null) 
                continue;
            items.add(new TDItem(typeAr[i], fieldAr[i]));
            if (!nameToIdxMap.containsKey(fieldAr[i]))
                nameToIdxMap.put(fieldAr[i], i);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        if (this.items == null) {
            this.items = new ArrayList<>();
        }
        if (this.nameToIdxMap == null) {
            this.nameToIdxMap = new HashMap<>();
        }
        nameToIdxMap.put("", 0);
        for (int i = 0, n = typeAr.length; i < n; i ++) {
            if (typeAr[i] == null)
                continue;
            items.add(new TDItem(typeAr[i], ""));
        }
    }

    public TupleDesc(List<TDItem> items, Map<String, Integer> nameToIdxMap) {
        this.items = items;
        this.nameToIdxMap = nameToIdxMap;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return items.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= items.size())
            throw new NoSuchElementException("no such field, please check the index");
        return items.get(i).getFieldName();
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= items.size())
            throw new NoSuchElementException("no such field, please check the index");
        return items.get(i).getFieldType();
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null || !nameToIdxMap.containsKey(name))
            throw new NoSuchElementException(String.format("no such field name, witch is {}", name));
        return nameToIdxMap.get(name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        return items.stream()
                    .map(item -> item.getFieldType().getLen())
                    .reduce(0, (a, b) -> a + b);
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        Type[] types = new Type[td1.numFields() + td2.numFields()];
        String[] names = new String[td1.numFields() + td2.numFields()];
        int index = 0;
        for (int i = 0; i < td1.numFields(); i ++) {
            types[index] = td1.getFieldType(i);
            names[index ++] = td1.getFieldName(i);
        }
        for (int i = 0; i < td2.numFields(); i ++) {
            types[index] = td2.getFieldType(i);
            names[index ++] = td2.getFieldName(i);
        }
        return new TupleDesc(types, names);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    @Override
    public boolean equals(Object o) {
        // some code goes here
        if (o == this)
            return true;
        if (o == null || this.getClass() != o.getClass())
            return false;
        TupleDesc obj = (TupleDesc) o;
        return new EqualsBuilder().append(items, obj.items)
                                  .append(nameToIdxMap, obj.nameToIdxMap)
                                  .isEquals();
    }

    @Override
    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return new HashCodeBuilder(17, 37).append(items)
                                          .append(nameToIdxMap)
                                          .toHashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    @Override
    public String toString() {
        // some code goes here
        return new ToStringBuilder(this).append(items)
                                        .append(nameToIdxMap)
                                        .toString();
    }
}
