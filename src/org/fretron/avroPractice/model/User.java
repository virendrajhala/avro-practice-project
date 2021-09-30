/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package org.fretron.avroPractice.model;

import org.apache.avro.specific.SpecificData;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class User extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -1393761250954879940L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"org.fretron.avroPractice.model\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"aadharId\",\"type\":\"long\"},{\"name\":\"address\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"age\",\"type\":[\"null\",\"int\"],\"default\":null}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<User> ENCODER =
      new BinaryMessageEncoder<User>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<User> DECODER =
      new BinaryMessageDecoder<User>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   */
  public static BinaryMessageDecoder<User> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   */
  public static BinaryMessageDecoder<User> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<User>(MODEL$, SCHEMA$, resolver);
  }

  /** Serializes this User to a ByteBuffer. */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /** Deserializes a User from a ByteBuffer. */
  public static User fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  @Deprecated public long id;
  @Deprecated public java.lang.CharSequence name;
  @Deprecated public long aadharId;
  @Deprecated public java.lang.CharSequence address;
  @Deprecated public java.lang.CharSequence email;
  @Deprecated public java.lang.Integer age;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public User() {}

  /**
   * All-args constructor.
   * @param id The new value for id
   * @param name The new value for name
   * @param aadharId The new value for aadharId
   * @param address The new value for address
   * @param email The new value for email
   * @param age The new value for age
   */
  public User(java.lang.Long id, java.lang.CharSequence name, java.lang.Long aadharId, java.lang.CharSequence address, java.lang.CharSequence email, java.lang.Integer age) {
    this.id = id;
    this.name = name;
    this.aadharId = aadharId;
    this.address = address;
    this.email = email;
    this.age = age;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return id;
    case 1: return name;
    case 2: return aadharId;
    case 3: return address;
    case 4: return email;
    case 5: return age;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: id = (java.lang.Long)value$; break;
    case 1: name = (java.lang.CharSequence)value$; break;
    case 2: aadharId = (java.lang.Long)value$; break;
    case 3: address = (java.lang.CharSequence)value$; break;
    case 4: email = (java.lang.CharSequence)value$; break;
    case 5: age = (java.lang.Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'id' field.
   * @return The value of the 'id' field.
   */
  public java.lang.Long getId() {
    return id;
  }

  /**
   * Sets the value of the 'id' field.
   * @param value the value to set.
   */
  public void setId(java.lang.Long value) {
    this.id = value;
  }

  /**
   * Gets the value of the 'name' field.
   * @return The value of the 'name' field.
   */
  public java.lang.CharSequence getName() {
    return name;
  }

  /**
   * Sets the value of the 'name' field.
   * @param value the value to set.
   */
  public void setName(java.lang.CharSequence value) {
    this.name = value;
  }

  /**
   * Gets the value of the 'aadharId' field.
   * @return The value of the 'aadharId' field.
   */
  public java.lang.Long getAadharId() {
    return aadharId;
  }

  /**
   * Sets the value of the 'aadharId' field.
   * @param value the value to set.
   */
  public void setAadharId(java.lang.Long value) {
    this.aadharId = value;
  }

  /**
   * Gets the value of the 'address' field.
   * @return The value of the 'address' field.
   */
  public java.lang.CharSequence getAddress() {
    return address;
  }

  /**
   * Sets the value of the 'address' field.
   * @param value the value to set.
   */
  public void setAddress(java.lang.CharSequence value) {
    this.address = value;
  }

  /**
   * Gets the value of the 'email' field.
   * @return The value of the 'email' field.
   */
  public java.lang.CharSequence getEmail() {
    return email;
  }

  /**
   * Sets the value of the 'email' field.
   * @param value the value to set.
   */
  public void setEmail(java.lang.CharSequence value) {
    this.email = value;
  }

  /**
   * Gets the value of the 'age' field.
   * @return The value of the 'age' field.
   */
  public java.lang.Integer getAge() {
    return age;
  }

  /**
   * Sets the value of the 'age' field.
   * @param value the value to set.
   */
  public void setAge(java.lang.Integer value) {
    this.age = value;
  }

  /**
   * Creates a new User RecordBuilder.
   * @return A new User RecordBuilder
   */
  public static org.fretron.avroPractice.model.User.Builder newBuilder() {
    return new org.fretron.avroPractice.model.User.Builder();
  }

  /**
   * Creates a new User RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new User RecordBuilder
   */
  public static org.fretron.avroPractice.model.User.Builder newBuilder(org.fretron.avroPractice.model.User.Builder other) {
    return new org.fretron.avroPractice.model.User.Builder(other);
  }

  /**
   * Creates a new User RecordBuilder by copying an existing User instance.
   * @param other The existing instance to copy.
   * @return A new User RecordBuilder
   */
  public static org.fretron.avroPractice.model.User.Builder newBuilder(org.fretron.avroPractice.model.User other) {
    return new org.fretron.avroPractice.model.User.Builder(other);
  }

  /**
   * RecordBuilder for User instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<User>
    implements org.apache.avro.data.RecordBuilder<User> {

    private long id;
    private java.lang.CharSequence name;
    private long aadharId;
    private java.lang.CharSequence address;
    private java.lang.CharSequence email;
    private java.lang.Integer age;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(org.fretron.avroPractice.model.User.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.id)) {
        this.id = data().deepCopy(fields()[0].schema(), other.id);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.name)) {
        this.name = data().deepCopy(fields()[1].schema(), other.name);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.aadharId)) {
        this.aadharId = data().deepCopy(fields()[2].schema(), other.aadharId);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.address)) {
        this.address = data().deepCopy(fields()[3].schema(), other.address);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.email)) {
        this.email = data().deepCopy(fields()[4].schema(), other.email);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.age)) {
        this.age = data().deepCopy(fields()[5].schema(), other.age);
        fieldSetFlags()[5] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing User instance
     * @param other The existing instance to copy.
     */
    private Builder(org.fretron.avroPractice.model.User other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.id)) {
        this.id = data().deepCopy(fields()[0].schema(), other.id);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.name)) {
        this.name = data().deepCopy(fields()[1].schema(), other.name);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.aadharId)) {
        this.aadharId = data().deepCopy(fields()[2].schema(), other.aadharId);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.address)) {
        this.address = data().deepCopy(fields()[3].schema(), other.address);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.email)) {
        this.email = data().deepCopy(fields()[4].schema(), other.email);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.age)) {
        this.age = data().deepCopy(fields()[5].schema(), other.age);
        fieldSetFlags()[5] = true;
      }
    }

    /**
      * Gets the value of the 'id' field.
      * @return The value.
      */
    public java.lang.Long getId() {
      return id;
    }

    /**
      * Sets the value of the 'id' field.
      * @param value The value of 'id'.
      * @return This builder.
      */
    public org.fretron.avroPractice.model.User.Builder setId(long value) {
      validate(fields()[0], value);
      this.id = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'id' field has been set.
      * @return True if the 'id' field has been set, false otherwise.
      */
    public boolean hasId() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'id' field.
      * @return This builder.
      */
    public org.fretron.avroPractice.model.User.Builder clearId() {
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'name' field.
      * @return The value.
      */
    public java.lang.CharSequence getName() {
      return name;
    }

    /**
      * Sets the value of the 'name' field.
      * @param value The value of 'name'.
      * @return This builder.
      */
    public org.fretron.avroPractice.model.User.Builder setName(java.lang.CharSequence value) {
      validate(fields()[1], value);
      this.name = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'name' field has been set.
      * @return True if the 'name' field has been set, false otherwise.
      */
    public boolean hasName() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'name' field.
      * @return This builder.
      */
    public org.fretron.avroPractice.model.User.Builder clearName() {
      name = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'aadharId' field.
      * @return The value.
      */
    public java.lang.Long getAadharId() {
      return aadharId;
    }

    /**
      * Sets the value of the 'aadharId' field.
      * @param value The value of 'aadharId'.
      * @return This builder.
      */
    public org.fretron.avroPractice.model.User.Builder setAadharId(long value) {
      validate(fields()[2], value);
      this.aadharId = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'aadharId' field has been set.
      * @return True if the 'aadharId' field has been set, false otherwise.
      */
    public boolean hasAadharId() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'aadharId' field.
      * @return This builder.
      */
    public org.fretron.avroPractice.model.User.Builder clearAadharId() {
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'address' field.
      * @return The value.
      */
    public java.lang.CharSequence getAddress() {
      return address;
    }

    /**
      * Sets the value of the 'address' field.
      * @param value The value of 'address'.
      * @return This builder.
      */
    public org.fretron.avroPractice.model.User.Builder setAddress(java.lang.CharSequence value) {
      validate(fields()[3], value);
      this.address = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'address' field has been set.
      * @return True if the 'address' field has been set, false otherwise.
      */
    public boolean hasAddress() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'address' field.
      * @return This builder.
      */
    public org.fretron.avroPractice.model.User.Builder clearAddress() {
      address = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    /**
      * Gets the value of the 'email' field.
      * @return The value.
      */
    public java.lang.CharSequence getEmail() {
      return email;
    }

    /**
      * Sets the value of the 'email' field.
      * @param value The value of 'email'.
      * @return This builder.
      */
    public org.fretron.avroPractice.model.User.Builder setEmail(java.lang.CharSequence value) {
      validate(fields()[4], value);
      this.email = value;
      fieldSetFlags()[4] = true;
      return this;
    }

    /**
      * Checks whether the 'email' field has been set.
      * @return True if the 'email' field has been set, false otherwise.
      */
    public boolean hasEmail() {
      return fieldSetFlags()[4];
    }


    /**
      * Clears the value of the 'email' field.
      * @return This builder.
      */
    public org.fretron.avroPractice.model.User.Builder clearEmail() {
      email = null;
      fieldSetFlags()[4] = false;
      return this;
    }

    /**
      * Gets the value of the 'age' field.
      * @return The value.
      */
    public java.lang.Integer getAge() {
      return age;
    }

    /**
      * Sets the value of the 'age' field.
      * @param value The value of 'age'.
      * @return This builder.
      */
    public org.fretron.avroPractice.model.User.Builder setAge(java.lang.Integer value) {
      validate(fields()[5], value);
      this.age = value;
      fieldSetFlags()[5] = true;
      return this;
    }

    /**
      * Checks whether the 'age' field has been set.
      * @return True if the 'age' field has been set, false otherwise.
      */
    public boolean hasAge() {
      return fieldSetFlags()[5];
    }


    /**
      * Clears the value of the 'age' field.
      * @return This builder.
      */
    public org.fretron.avroPractice.model.User.Builder clearAge() {
      age = null;
      fieldSetFlags()[5] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public User build() {
      try {
        User record = new User();
        record.id = fieldSetFlags()[0] ? this.id : (java.lang.Long) defaultValue(fields()[0]);
        record.name = fieldSetFlags()[1] ? this.name : (java.lang.CharSequence) defaultValue(fields()[1]);
        record.aadharId = fieldSetFlags()[2] ? this.aadharId : (java.lang.Long) defaultValue(fields()[2]);
        record.address = fieldSetFlags()[3] ? this.address : (java.lang.CharSequence) defaultValue(fields()[3]);
        record.email = fieldSetFlags()[4] ? this.email : (java.lang.CharSequence) defaultValue(fields()[4]);
        record.age = fieldSetFlags()[5] ? this.age : (java.lang.Integer) defaultValue(fields()[5]);
        return record;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<User>
    WRITER$ = (org.apache.avro.io.DatumWriter<User>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<User>
    READER$ = (org.apache.avro.io.DatumReader<User>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}
