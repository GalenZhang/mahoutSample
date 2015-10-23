// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: MessageName.proto

package com.achievo.protobuf.examples;

public final class MessageName {
  private MessageName() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public interface helloworldOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // required int32 id = 1;
    /**
     * <code>required int32 id = 1;</code>
     */
    boolean hasId();
    /**
     * <code>required int32 id = 1;</code>
     */
    int getId();

    // required string str = 2;
    /**
     * <code>required string str = 2;</code>
     */
    boolean hasStr();
    /**
     * <code>required string str = 2;</code>
     */
    java.lang.String getStr();
    /**
     * <code>required string str = 2;</code>
     */
    com.google.protobuf.ByteString
        getStrBytes();

    // optional int32 opt = 3;
    /**
     * <code>optional int32 opt = 3;</code>
     */
    boolean hasOpt();
    /**
     * <code>optional int32 opt = 3;</code>
     */
    int getOpt();
  }
  /**
   * Protobuf type {@code com.achievo.protobuf.examples.helloworld}
   */
  public static final class helloworld extends
      com.google.protobuf.GeneratedMessage
      implements helloworldOrBuilder {
    // Use helloworld.newBuilder() to construct.
    private helloworld(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private helloworld(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final helloworld defaultInstance;
    public static helloworld getDefaultInstance() {
      return defaultInstance;
    }

    public helloworld getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private helloworld(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 8: {
              bitField0_ |= 0x00000001;
              id_ = input.readInt32();
              break;
            }
            case 18: {
              bitField0_ |= 0x00000002;
              str_ = input.readBytes();
              break;
            }
            case 24: {
              bitField0_ |= 0x00000004;
              opt_ = input.readInt32();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.achievo.protobuf.examples.MessageName.internal_static_com_achievo_protobuf_examples_helloworld_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.achievo.protobuf.examples.MessageName.internal_static_com_achievo_protobuf_examples_helloworld_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.achievo.protobuf.examples.MessageName.helloworld.class, com.achievo.protobuf.examples.MessageName.helloworld.Builder.class);
    }

    public static com.google.protobuf.Parser<helloworld> PARSER =
        new com.google.protobuf.AbstractParser<helloworld>() {
      public helloworld parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new helloworld(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<helloworld> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // required int32 id = 1;
    public static final int ID_FIELD_NUMBER = 1;
    private int id_;
    /**
     * <code>required int32 id = 1;</code>
     */
    public boolean hasId() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>required int32 id = 1;</code>
     */
    public int getId() {
      return id_;
    }

    // required string str = 2;
    public static final int STR_FIELD_NUMBER = 2;
    private java.lang.Object str_;
    /**
     * <code>required string str = 2;</code>
     */
    public boolean hasStr() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>required string str = 2;</code>
     */
    public java.lang.String getStr() {
      java.lang.Object ref = str_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          str_ = s;
        }
        return s;
      }
    }
    /**
     * <code>required string str = 2;</code>
     */
    public com.google.protobuf.ByteString
        getStrBytes() {
      java.lang.Object ref = str_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        str_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    // optional int32 opt = 3;
    public static final int OPT_FIELD_NUMBER = 3;
    private int opt_;
    /**
     * <code>optional int32 opt = 3;</code>
     */
    public boolean hasOpt() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>optional int32 opt = 3;</code>
     */
    public int getOpt() {
      return opt_;
    }

    private void initFields() {
      id_ = 0;
      str_ = "";
      opt_ = 0;
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      if (!hasId()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasStr()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeInt32(1, id_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeBytes(2, getStrBytes());
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        output.writeInt32(3, opt_);
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(1, id_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(2, getStrBytes());
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(3, opt_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    public static com.achievo.protobuf.examples.MessageName.helloworld parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.achievo.protobuf.examples.MessageName.helloworld parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.achievo.protobuf.examples.MessageName.helloworld parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.achievo.protobuf.examples.MessageName.helloworld parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.achievo.protobuf.examples.MessageName.helloworld parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.achievo.protobuf.examples.MessageName.helloworld parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.achievo.protobuf.examples.MessageName.helloworld parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.achievo.protobuf.examples.MessageName.helloworld parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.achievo.protobuf.examples.MessageName.helloworld parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.achievo.protobuf.examples.MessageName.helloworld parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.achievo.protobuf.examples.MessageName.helloworld prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code com.achievo.protobuf.examples.helloworld}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements com.achievo.protobuf.examples.MessageName.helloworldOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.achievo.protobuf.examples.MessageName.internal_static_com_achievo_protobuf_examples_helloworld_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.achievo.protobuf.examples.MessageName.internal_static_com_achievo_protobuf_examples_helloworld_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.achievo.protobuf.examples.MessageName.helloworld.class, com.achievo.protobuf.examples.MessageName.helloworld.Builder.class);
      }

      // Construct using com.achievo.protobuf.examples.MessageName.helloworld.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        id_ = 0;
        bitField0_ = (bitField0_ & ~0x00000001);
        str_ = "";
        bitField0_ = (bitField0_ & ~0x00000002);
        opt_ = 0;
        bitField0_ = (bitField0_ & ~0x00000004);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.achievo.protobuf.examples.MessageName.internal_static_com_achievo_protobuf_examples_helloworld_descriptor;
      }

      public com.achievo.protobuf.examples.MessageName.helloworld getDefaultInstanceForType() {
        return com.achievo.protobuf.examples.MessageName.helloworld.getDefaultInstance();
      }

      public com.achievo.protobuf.examples.MessageName.helloworld build() {
        com.achievo.protobuf.examples.MessageName.helloworld result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.achievo.protobuf.examples.MessageName.helloworld buildPartial() {
        com.achievo.protobuf.examples.MessageName.helloworld result = new com.achievo.protobuf.examples.MessageName.helloworld(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.id_ = id_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.str_ = str_;
        if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
          to_bitField0_ |= 0x00000004;
        }
        result.opt_ = opt_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.achievo.protobuf.examples.MessageName.helloworld) {
          return mergeFrom((com.achievo.protobuf.examples.MessageName.helloworld)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.achievo.protobuf.examples.MessageName.helloworld other) {
        if (other == com.achievo.protobuf.examples.MessageName.helloworld.getDefaultInstance()) return this;
        if (other.hasId()) {
          setId(other.getId());
        }
        if (other.hasStr()) {
          bitField0_ |= 0x00000002;
          str_ = other.str_;
          onChanged();
        }
        if (other.hasOpt()) {
          setOpt(other.getOpt());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        if (!hasId()) {
          
          return false;
        }
        if (!hasStr()) {
          
          return false;
        }
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.achievo.protobuf.examples.MessageName.helloworld parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.achievo.protobuf.examples.MessageName.helloworld) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // required int32 id = 1;
      private int id_ ;
      /**
       * <code>required int32 id = 1;</code>
       */
      public boolean hasId() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>required int32 id = 1;</code>
       */
      public int getId() {
        return id_;
      }
      /**
       * <code>required int32 id = 1;</code>
       */
      public Builder setId(int value) {
        bitField0_ |= 0x00000001;
        id_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required int32 id = 1;</code>
       */
      public Builder clearId() {
        bitField0_ = (bitField0_ & ~0x00000001);
        id_ = 0;
        onChanged();
        return this;
      }

      // required string str = 2;
      private java.lang.Object str_ = "";
      /**
       * <code>required string str = 2;</code>
       */
      public boolean hasStr() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>required string str = 2;</code>
       */
      public java.lang.String getStr() {
        java.lang.Object ref = str_;
        if (!(ref instanceof java.lang.String)) {
          java.lang.String s = ((com.google.protobuf.ByteString) ref)
              .toStringUtf8();
          str_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>required string str = 2;</code>
       */
      public com.google.protobuf.ByteString
          getStrBytes() {
        java.lang.Object ref = str_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          str_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>required string str = 2;</code>
       */
      public Builder setStr(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        str_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required string str = 2;</code>
       */
      public Builder clearStr() {
        bitField0_ = (bitField0_ & ~0x00000002);
        str_ = getDefaultInstance().getStr();
        onChanged();
        return this;
      }
      /**
       * <code>required string str = 2;</code>
       */
      public Builder setStrBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        str_ = value;
        onChanged();
        return this;
      }

      // optional int32 opt = 3;
      private int opt_ ;
      /**
       * <code>optional int32 opt = 3;</code>
       */
      public boolean hasOpt() {
        return ((bitField0_ & 0x00000004) == 0x00000004);
      }
      /**
       * <code>optional int32 opt = 3;</code>
       */
      public int getOpt() {
        return opt_;
      }
      /**
       * <code>optional int32 opt = 3;</code>
       */
      public Builder setOpt(int value) {
        bitField0_ |= 0x00000004;
        opt_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int32 opt = 3;</code>
       */
      public Builder clearOpt() {
        bitField0_ = (bitField0_ & ~0x00000004);
        opt_ = 0;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:com.achievo.protobuf.examples.helloworld)
    }

    static {
      defaultInstance = new helloworld(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:com.achievo.protobuf.examples.helloworld)
  }

  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_com_achievo_protobuf_examples_helloworld_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_com_achievo_protobuf_examples_helloworld_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\021MessageName.proto\022\035com.achievo.protobu" +
      "f.examples\"2\n\nhelloworld\022\n\n\002id\030\001 \002(\005\022\013\n\003" +
      "str\030\002 \002(\t\022\013\n\003opt\030\003 \001(\005"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          internal_static_com_achievo_protobuf_examples_helloworld_descriptor =
            getDescriptor().getMessageTypes().get(0);
          internal_static_com_achievo_protobuf_examples_helloworld_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_com_achievo_protobuf_examples_helloworld_descriptor,
              new java.lang.String[] { "Id", "Str", "Opt", });
          return null;
        }
      };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
  }

  // @@protoc_insertion_point(outer_class_scope)
}
