/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.qunar.hive.hbase.avro;
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class Employee extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Employee\",\"namespace\":\"org.apache.hadoop.hive.hbase.avro\",\"fields\":[{\"name\":\"employeeName\",\"type\":\"string\"},{\"name\":\"employeeID\",\"type\":\"long\"},{\"name\":\"age\",\"type\":\"long\"},{\"name\":\"gender\",\"type\":{\"type\":\"enum\",\"name\":\"Gender\",\"symbols\":[\"MALE\",\"FEMALE\"]}},{\"name\":\"contactInfo\",\"type\":{\"type\":\"record\",\"name\":\"ContactInfo\",\"fields\":[{\"name\":\"address\",\"type\":[{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"Address\",\"fields\":[{\"name\":\"address1\",\"type\":\"string\"},{\"name\":\"address2\",\"type\":\"string\"},{\"name\":\"city\",\"type\":\"string\"},{\"name\":\"zipcode\",\"type\":\"long\"},{\"name\":\"county\",\"type\":[{\"type\":\"record\",\"name\":\"HomePhone\",\"fields\":[{\"name\":\"areaCode\",\"type\":\"long\"},{\"name\":\"number\",\"type\":\"long\"}]},{\"type\":\"record\",\"name\":\"OfficePhone\",\"fields\":[{\"name\":\"areaCode\",\"type\":\"long\"},{\"name\":\"number\",\"type\":\"long\"}]},\"string\",\"null\"]},{\"name\":\"aliases\",\"type\":[{\"type\":\"array\",\"items\":\"string\"},\"null\"]},{\"name\":\"metadata\",\"type\":[\"null\",{\"type\":\"map\",\"values\":\"string\"}]}]}},\"null\"]},{\"name\":\"homePhone\",\"type\":\"HomePhone\"},{\"name\":\"officePhone\",\"type\":\"OfficePhone\"}]}}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.CharSequence employeeName;
  @Deprecated public long employeeID;
  @Deprecated public long age;
  @Deprecated public Gender gender;
  @Deprecated public ContactInfo contactInfo;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>. 
   */
  public Employee() {}

  /**
   * All-args constructor.
   */
  public Employee(java.lang.CharSequence employeeName, java.lang.Long employeeID, java.lang.Long age, Gender gender, ContactInfo contactInfo) {
    this.employeeName = employeeName;
    this.employeeID = employeeID;
    this.age = age;
    this.gender = gender;
    this.contactInfo = contactInfo;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return employeeName;
    case 1: return employeeID;
    case 2: return age;
    case 3: return gender;
    case 4: return contactInfo;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: employeeName = (java.lang.CharSequence)value$; break;
    case 1: employeeID = (java.lang.Long)value$; break;
    case 2: age = (java.lang.Long)value$; break;
    case 3: gender = (Gender)value$; break;
    case 4: contactInfo = (ContactInfo)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'employeeName' field.
   */
  public java.lang.CharSequence getEmployeeName() {
    return employeeName;
  }

  /**
   * Sets the value of the 'employeeName' field.
   * @param value the value to set.
   */
  public void setEmployeeName(java.lang.CharSequence value) {
    this.employeeName = value;
  }

  /**
   * Gets the value of the 'employeeID' field.
   */
  public java.lang.Long getEmployeeID() {
    return employeeID;
  }

  /**
   * Sets the value of the 'employeeID' field.
   * @param value the value to set.
   */
  public void setEmployeeID(java.lang.Long value) {
    this.employeeID = value;
  }

  /**
   * Gets the value of the 'age' field.
   */
  public java.lang.Long getAge() {
    return age;
  }

  /**
   * Sets the value of the 'age' field.
   * @param value the value to set.
   */
  public void setAge(java.lang.Long value) {
    this.age = value;
  }

  /**
   * Gets the value of the 'gender' field.
   */
  public Gender getGender() {
    return gender;
  }

  /**
   * Sets the value of the 'gender' field.
   * @param value the value to set.
   */
  public void setGender(Gender value) {
    this.gender = value;
  }

  /**
   * Gets the value of the 'contactInfo' field.
   */
  public ContactInfo getContactInfo() {
    return contactInfo;
  }

  /**
   * Sets the value of the 'contactInfo' field.
   * @param value the value to set.
   */
  public void setContactInfo(ContactInfo value) {
    this.contactInfo = value;
  }

  /** Creates a new Employee RecordBuilder */
  public static Employee.Builder newBuilder() {
    return new Employee.Builder();
  }
  
  /** Creates a new Employee RecordBuilder by copying an existing Builder */
  public static Employee.Builder newBuilder(Employee.Builder other) {
    return new Employee.Builder(other);
  }
  
  /** Creates a new Employee RecordBuilder by copying an existing Employee instance */
  public static Employee.Builder newBuilder(Employee other) {
    return new Employee.Builder(other);
  }
  
  /**
   * RecordBuilder for Employee instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Employee>
    implements org.apache.avro.data.RecordBuilder<Employee> {

    private java.lang.CharSequence employeeName;
    private long employeeID;
    private long age;
    private Gender gender;
    private ContactInfo contactInfo;

    /** Creates a new Builder */
    private Builder() {
      super(Employee.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(Employee.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.employeeName)) {
        this.employeeName = data().deepCopy(fields()[0].schema(), other.employeeName);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.employeeID)) {
        this.employeeID = data().deepCopy(fields()[1].schema(), other.employeeID);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.age)) {
        this.age = data().deepCopy(fields()[2].schema(), other.age);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.gender)) {
        this.gender = data().deepCopy(fields()[3].schema(), other.gender);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.contactInfo)) {
        this.contactInfo = data().deepCopy(fields()[4].schema(), other.contactInfo);
        fieldSetFlags()[4] = true;
      }
    }
    
    /** Creates a Builder by copying an existing Employee instance */
    private Builder(Employee other) {
            super(Employee.SCHEMA$);
      if (isValidValue(fields()[0], other.employeeName)) {
        this.employeeName = data().deepCopy(fields()[0].schema(), other.employeeName);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.employeeID)) {
        this.employeeID = data().deepCopy(fields()[1].schema(), other.employeeID);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.age)) {
        this.age = data().deepCopy(fields()[2].schema(), other.age);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.gender)) {
        this.gender = data().deepCopy(fields()[3].schema(), other.gender);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.contactInfo)) {
        this.contactInfo = data().deepCopy(fields()[4].schema(), other.contactInfo);
        fieldSetFlags()[4] = true;
      }
    }

    /** Gets the value of the 'employeeName' field */
    public java.lang.CharSequence getEmployeeName() {
      return employeeName;
    }
    
    /** Sets the value of the 'employeeName' field */
    public Employee.Builder setEmployeeName(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.employeeName = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'employeeName' field has been set */
    public boolean hasEmployeeName() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'employeeName' field */
    public Employee.Builder clearEmployeeName() {
      employeeName = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'employeeID' field */
    public java.lang.Long getEmployeeID() {
      return employeeID;
    }
    
    /** Sets the value of the 'employeeID' field */
    public Employee.Builder setEmployeeID(long value) {
      validate(fields()[1], value);
      this.employeeID = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'employeeID' field has been set */
    public boolean hasEmployeeID() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'employeeID' field */
    public Employee.Builder clearEmployeeID() {
      fieldSetFlags()[1] = false;
      return this;
    }

    /** Gets the value of the 'age' field */
    public java.lang.Long getAge() {
      return age;
    }
    
    /** Sets the value of the 'age' field */
    public Employee.Builder setAge(long value) {
      validate(fields()[2], value);
      this.age = value;
      fieldSetFlags()[2] = true;
      return this; 
    }
    
    /** Checks whether the 'age' field has been set */
    public boolean hasAge() {
      return fieldSetFlags()[2];
    }
    
    /** Clears the value of the 'age' field */
    public Employee.Builder clearAge() {
      fieldSetFlags()[2] = false;
      return this;
    }

    /** Gets the value of the 'gender' field */
    public Gender getGender() {
      return gender;
    }
    
    /** Sets the value of the 'gender' field */
    public Employee.Builder setGender(Gender value) {
      validate(fields()[3], value);
      this.gender = value;
      fieldSetFlags()[3] = true;
      return this; 
    }
    
    /** Checks whether the 'gender' field has been set */
    public boolean hasGender() {
      return fieldSetFlags()[3];
    }
    
    /** Clears the value of the 'gender' field */
    public Employee.Builder clearGender() {
      gender = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    /** Gets the value of the 'contactInfo' field */
    public ContactInfo getContactInfo() {
      return contactInfo;
    }
    
    /** Sets the value of the 'contactInfo' field */
    public Employee.Builder setContactInfo(ContactInfo value) {
      validate(fields()[4], value);
      this.contactInfo = value;
      fieldSetFlags()[4] = true;
      return this; 
    }
    
    /** Checks whether the 'contactInfo' field has been set */
    public boolean hasContactInfo() {
      return fieldSetFlags()[4];
    }
    
    /** Clears the value of the 'contactInfo' field */
    public Employee.Builder clearContactInfo() {
      contactInfo = null;
      fieldSetFlags()[4] = false;
      return this;
    }

    @Override
    public Employee build() {
      try {
        Employee record = new Employee();
        record.employeeName = fieldSetFlags()[0] ? this.employeeName : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.employeeID = fieldSetFlags()[1] ? this.employeeID : (java.lang.Long) defaultValue(fields()[1]);
        record.age = fieldSetFlags()[2] ? this.age : (java.lang.Long) defaultValue(fields()[2]);
        record.gender = fieldSetFlags()[3] ? this.gender : (Gender) defaultValue(fields()[3]);
        record.contactInfo = fieldSetFlags()[4] ? this.contactInfo : (ContactInfo) defaultValue(fields()[4]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
