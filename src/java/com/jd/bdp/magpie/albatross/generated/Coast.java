/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.jd.bdp.magpie.albatross.generated;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)", date = "2015-11-23")
public class Coast {

  public interface Iface {

    public String heartbeat(String uuid, String jobid) throws org.apache.thrift.TException;

  }

  public interface AsyncIface {

    public void heartbeat(String uuid, String jobid, org.apache.thrift.async.AsyncMethodCallback resultHandler) throws org.apache.thrift.TException;

  }

  public static class Client extends org.apache.thrift.TServiceClient implements Iface {
    public static class Factory implements org.apache.thrift.TServiceClientFactory<Client> {
      public Factory() {}
      public Client getClient(org.apache.thrift.protocol.TProtocol prot) {
        return new Client(prot);
      }
      public Client getClient(org.apache.thrift.protocol.TProtocol iprot, org.apache.thrift.protocol.TProtocol oprot) {
        return new Client(iprot, oprot);
      }
    }

    public Client(org.apache.thrift.protocol.TProtocol prot)
    {
      super(prot, prot);
    }

    public Client(org.apache.thrift.protocol.TProtocol iprot, org.apache.thrift.protocol.TProtocol oprot) {
      super(iprot, oprot);
    }

    public String heartbeat(String uuid, String jobid) throws org.apache.thrift.TException
    {
      send_heartbeat(uuid, jobid);
      return recv_heartbeat();
    }

    public void send_heartbeat(String uuid, String jobid) throws org.apache.thrift.TException
    {
      heartbeat_args args = new heartbeat_args();
      args.set_uuid(uuid);
      args.set_jobid(jobid);
      sendBase("heartbeat", args);
    }

    public String recv_heartbeat() throws org.apache.thrift.TException
    {
      heartbeat_result result = new heartbeat_result();
      receiveBase(result, "heartbeat");
      if (result.is_set_success()) {
        return result.success;
      }
      throw new org.apache.thrift.TApplicationException(org.apache.thrift.TApplicationException.MISSING_RESULT, "heartbeat failed: unknown result");
    }

  }
  public static class AsyncClient extends org.apache.thrift.async.TAsyncClient implements AsyncIface {
    public static class Factory implements org.apache.thrift.async.TAsyncClientFactory<AsyncClient> {
      private org.apache.thrift.async.TAsyncClientManager clientManager;
      private org.apache.thrift.protocol.TProtocolFactory protocolFactory;
      public Factory(org.apache.thrift.async.TAsyncClientManager clientManager, org.apache.thrift.protocol.TProtocolFactory protocolFactory) {
        this.clientManager = clientManager;
        this.protocolFactory = protocolFactory;
      }
      public AsyncClient getAsyncClient(org.apache.thrift.transport.TNonblockingTransport transport) {
        return new AsyncClient(protocolFactory, clientManager, transport);
      }
    }

    public AsyncClient(org.apache.thrift.protocol.TProtocolFactory protocolFactory, org.apache.thrift.async.TAsyncClientManager clientManager, org.apache.thrift.transport.TNonblockingTransport transport) {
      super(protocolFactory, clientManager, transport);
    }

    public void heartbeat(String uuid, String jobid, org.apache.thrift.async.AsyncMethodCallback resultHandler) throws org.apache.thrift.TException {
      checkReady();
      heartbeat_call method_call = new heartbeat_call(uuid, jobid, resultHandler, this, ___protocolFactory, ___transport);
      this.___currentMethod = method_call;
      ___manager.call(method_call);
    }

    public static class heartbeat_call extends org.apache.thrift.async.TAsyncMethodCall {
      private String uuid;
      private String jobid;
      public heartbeat_call(String uuid, String jobid, org.apache.thrift.async.AsyncMethodCallback resultHandler, org.apache.thrift.async.TAsyncClient client, org.apache.thrift.protocol.TProtocolFactory protocolFactory, org.apache.thrift.transport.TNonblockingTransport transport) throws org.apache.thrift.TException {
        super(client, protocolFactory, transport, resultHandler, false);
        this.uuid = uuid;
        this.jobid = jobid;
      }

      public void write_args(org.apache.thrift.protocol.TProtocol prot) throws org.apache.thrift.TException {
        prot.writeMessageBegin(new org.apache.thrift.protocol.TMessage("heartbeat", org.apache.thrift.protocol.TMessageType.CALL, 0));
        heartbeat_args args = new heartbeat_args();
        args.set_uuid(uuid);
        args.set_jobid(jobid);
        args.write(prot);
        prot.writeMessageEnd();
      }

      public String getResult() throws org.apache.thrift.TException {
        if (getState() != org.apache.thrift.async.TAsyncMethodCall.State.RESPONSE_READ) {
          throw new IllegalStateException("Method call not finished!");
        }
        org.apache.thrift.transport.TMemoryInputTransport memoryTransport = new org.apache.thrift.transport.TMemoryInputTransport(getFrameBuffer().array());
        org.apache.thrift.protocol.TProtocol prot = client.getProtocolFactory().getProtocol(memoryTransport);
        return (new Client(prot)).recv_heartbeat();
      }
    }

  }

  public static class Processor<I extends Iface> extends org.apache.thrift.TBaseProcessor<I> implements org.apache.thrift.TProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(Processor.class.getName());
    public Processor(I iface) {
      super(iface, getProcessMap(new HashMap<String, org.apache.thrift.ProcessFunction<I, ? extends org.apache.thrift.TBase>>()));
    }

    protected Processor(I iface, Map<String,  org.apache.thrift.ProcessFunction<I, ? extends  org.apache.thrift.TBase>> processMap) {
      super(iface, getProcessMap(processMap));
    }

    private static <I extends Iface> Map<String,  org.apache.thrift.ProcessFunction<I, ? extends  org.apache.thrift.TBase>> getProcessMap(Map<String,  org.apache.thrift.ProcessFunction<I, ? extends  org.apache.thrift.TBase>> processMap) {
      processMap.put("heartbeat", new heartbeat());
      return processMap;
    }

    public static class heartbeat<I extends Iface> extends org.apache.thrift.ProcessFunction<I, heartbeat_args> {
      public heartbeat() {
        super("heartbeat");
      }

      public heartbeat_args getEmptyArgsInstance() {
        return new heartbeat_args();
      }

      protected boolean isOneway() {
        return false;
      }

      public heartbeat_result getResult(I iface, heartbeat_args args) throws org.apache.thrift.TException {
        heartbeat_result result = new heartbeat_result();
        result.success = iface.heartbeat(args.uuid, args.jobid);
        return result;
      }
    }

  }

  public static class AsyncProcessor<I extends AsyncIface> extends org.apache.thrift.TBaseAsyncProcessor<I> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncProcessor.class.getName());
    public AsyncProcessor(I iface) {
      super(iface, getProcessMap(new HashMap<String, org.apache.thrift.AsyncProcessFunction<I, ? extends org.apache.thrift.TBase, ?>>()));
    }

    protected AsyncProcessor(I iface, Map<String,  org.apache.thrift.AsyncProcessFunction<I, ? extends  org.apache.thrift.TBase, ?>> processMap) {
      super(iface, getProcessMap(processMap));
    }

    private static <I extends AsyncIface> Map<String,  org.apache.thrift.AsyncProcessFunction<I, ? extends  org.apache.thrift.TBase,?>> getProcessMap(Map<String,  org.apache.thrift.AsyncProcessFunction<I, ? extends  org.apache.thrift.TBase, ?>> processMap) {
      processMap.put("heartbeat", new heartbeat());
      return processMap;
    }

    public static class heartbeat<I extends AsyncIface> extends org.apache.thrift.AsyncProcessFunction<I, heartbeat_args, String> {
      public heartbeat() {
        super("heartbeat");
      }

      public heartbeat_args getEmptyArgsInstance() {
        return new heartbeat_args();
      }

      public AsyncMethodCallback<String> getResultHandler(final AsyncFrameBuffer fb, final int seqid) {
        final org.apache.thrift.AsyncProcessFunction fcall = this;
        return new AsyncMethodCallback<String>() { 
          public void onComplete(String o) {
            heartbeat_result result = new heartbeat_result();
            result.success = o;
            try {
              fcall.sendResponse(fb,result, org.apache.thrift.protocol.TMessageType.REPLY,seqid);
              return;
            } catch (Exception e) {
              LOGGER.error("Exception writing to internal frame buffer", e);
            }
            fb.close();
          }
          public void onError(Exception e) {
            byte msgType = org.apache.thrift.protocol.TMessageType.REPLY;
            org.apache.thrift.TBase msg;
            heartbeat_result result = new heartbeat_result();
            {
              msgType = org.apache.thrift.protocol.TMessageType.EXCEPTION;
              msg = (org.apache.thrift.TBase)new org.apache.thrift.TApplicationException(org.apache.thrift.TApplicationException.INTERNAL_ERROR, e.getMessage());
            }
            try {
              fcall.sendResponse(fb,msg,msgType,seqid);
              return;
            } catch (Exception ex) {
              LOGGER.error("Exception writing to internal frame buffer", ex);
            }
            fb.close();
          }
        };
      }

      protected boolean isOneway() {
        return false;
      }

      public void start(I iface, heartbeat_args args, org.apache.thrift.async.AsyncMethodCallback<String> resultHandler) throws TException {
        iface.heartbeat(args.uuid, args.jobid,resultHandler);
      }
    }

  }

  public static class heartbeat_args implements org.apache.thrift.TBase<heartbeat_args, heartbeat_args._Fields>, java.io.Serializable, Cloneable, Comparable<heartbeat_args>   {
    private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("heartbeat_args");

    private static final org.apache.thrift.protocol.TField UUID_FIELD_DESC = new org.apache.thrift.protocol.TField("uuid", org.apache.thrift.protocol.TType.STRING, (short)1);
    private static final org.apache.thrift.protocol.TField JOBID_FIELD_DESC = new org.apache.thrift.protocol.TField("jobid", org.apache.thrift.protocol.TType.STRING, (short)2);

    private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
    static {
      schemes.put(StandardScheme.class, new heartbeat_argsStandardSchemeFactory());
      schemes.put(TupleScheme.class, new heartbeat_argsTupleSchemeFactory());
    }

    private String uuid; // required
    private String jobid; // required

    /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
    public enum _Fields implements org.apache.thrift.TFieldIdEnum {
      UUID((short)1, "uuid"),
      JOBID((short)2, "jobid");

      private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

      static {
        for (_Fields field : EnumSet.allOf(_Fields.class)) {
          byName.put(field.getFieldName(), field);
        }
      }

      /**
       * Find the _Fields constant that matches fieldId, or null if its not found.
       */
      public static _Fields findByThriftId(int fieldId) {
        switch(fieldId) {
          case 1: // UUID
            return UUID;
          case 2: // JOBID
            return JOBID;
          default:
            return null;
        }
      }

      /**
       * Find the _Fields constant that matches fieldId, throwing an exception
       * if it is not found.
       */
      public static _Fields findByThriftIdOrThrow(int fieldId) {
        _Fields fields = findByThriftId(fieldId);
        if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
        return fields;
      }

      /**
       * Find the _Fields constant that matches name, or null if its not found.
       */
      public static _Fields findByName(String name) {
        return byName.get(name);
      }

      private final short _thriftId;
      private final String _fieldName;

      _Fields(short thriftId, String fieldName) {
        _thriftId = thriftId;
        _fieldName = fieldName;
      }

      public short getThriftFieldId() {
        return _thriftId;
      }

      public String getFieldName() {
        return _fieldName;
      }
    }

    // isset id assignments
    public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
    static {
      Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
      tmpMap.put(_Fields.UUID, new org.apache.thrift.meta_data.FieldMetaData("uuid", org.apache.thrift.TFieldRequirementType.DEFAULT, 
          new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
      tmpMap.put(_Fields.JOBID, new org.apache.thrift.meta_data.FieldMetaData("jobid", org.apache.thrift.TFieldRequirementType.DEFAULT, 
          new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
      metaDataMap = Collections.unmodifiableMap(tmpMap);
      org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(heartbeat_args.class, metaDataMap);
    }

    public heartbeat_args() {
    }

    public heartbeat_args(
      String uuid,
      String jobid)
    {
      this();
      this.uuid = uuid;
      this.jobid = jobid;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public heartbeat_args(heartbeat_args other) {
      if (other.is_set_uuid()) {
        this.uuid = other.uuid;
      }
      if (other.is_set_jobid()) {
        this.jobid = other.jobid;
      }
    }

    public heartbeat_args deepCopy() {
      return new heartbeat_args(this);
    }

    @Override
    public void clear() {
      this.uuid = null;
      this.jobid = null;
    }

    public String get_uuid() {
      return this.uuid;
    }

    public void set_uuid(String uuid) {
      this.uuid = uuid;
    }

    public void unset_uuid() {
      this.uuid = null;
    }

    /** Returns true if field uuid is set (has been assigned a value) and false otherwise */
    public boolean is_set_uuid() {
      return this.uuid != null;
    }

    public void set_uuid_isSet(boolean value) {
      if (!value) {
        this.uuid = null;
      }
    }

    public String get_jobid() {
      return this.jobid;
    }

    public void set_jobid(String jobid) {
      this.jobid = jobid;
    }

    public void unset_jobid() {
      this.jobid = null;
    }

    /** Returns true if field jobid is set (has been assigned a value) and false otherwise */
    public boolean is_set_jobid() {
      return this.jobid != null;
    }

    public void set_jobid_isSet(boolean value) {
      if (!value) {
        this.jobid = null;
      }
    }

    public void setFieldValue(_Fields field, Object value) {
      switch (field) {
      case UUID:
        if (value == null) {
          unset_uuid();
        } else {
          set_uuid((String)value);
        }
        break;

      case JOBID:
        if (value == null) {
          unset_jobid();
        } else {
          set_jobid((String)value);
        }
        break;

      }
    }

    public Object getFieldValue(_Fields field) {
      switch (field) {
      case UUID:
        return get_uuid();

      case JOBID:
        return get_jobid();

      }
      throw new IllegalStateException();
    }

    /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
    public boolean isSet(_Fields field) {
      if (field == null) {
        throw new IllegalArgumentException();
      }

      switch (field) {
      case UUID:
        return is_set_uuid();
      case JOBID:
        return is_set_jobid();
      }
      throw new IllegalStateException();
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof heartbeat_args)
        return this.equals((heartbeat_args)that);
      return false;
    }

    public boolean equals(heartbeat_args that) {
      if (that == null)
        return false;

      boolean this_present_uuid = true && this.is_set_uuid();
      boolean that_present_uuid = true && that.is_set_uuid();
      if (this_present_uuid || that_present_uuid) {
        if (!(this_present_uuid && that_present_uuid))
          return false;
        if (!this.uuid.equals(that.uuid))
          return false;
      }

      boolean this_present_jobid = true && this.is_set_jobid();
      boolean that_present_jobid = true && that.is_set_jobid();
      if (this_present_jobid || that_present_jobid) {
        if (!(this_present_jobid && that_present_jobid))
          return false;
        if (!this.jobid.equals(that.jobid))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      List<Object> list = new ArrayList<Object>();

      boolean present_uuid = true && (is_set_uuid());
      list.add(present_uuid);
      if (present_uuid)
        list.add(uuid);

      boolean present_jobid = true && (is_set_jobid());
      list.add(present_jobid);
      if (present_jobid)
        list.add(jobid);

      return list.hashCode();
    }

    @Override
    public int compareTo(heartbeat_args other) {
      if (!getClass().equals(other.getClass())) {
        return getClass().getName().compareTo(other.getClass().getName());
      }

      int lastComparison = 0;

      lastComparison = Boolean.valueOf(is_set_uuid()).compareTo(other.is_set_uuid());
      if (lastComparison != 0) {
        return lastComparison;
      }
      if (is_set_uuid()) {
        lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.uuid, other.uuid);
        if (lastComparison != 0) {
          return lastComparison;
        }
      }
      lastComparison = Boolean.valueOf(is_set_jobid()).compareTo(other.is_set_jobid());
      if (lastComparison != 0) {
        return lastComparison;
      }
      if (is_set_jobid()) {
        lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.jobid, other.jobid);
        if (lastComparison != 0) {
          return lastComparison;
        }
      }
      return 0;
    }

    public _Fields fieldForId(int fieldId) {
      return _Fields.findByThriftId(fieldId);
    }

    public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
      schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
      schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("heartbeat_args(");
      boolean first = true;

      sb.append("uuid:");
      if (this.uuid == null) {
        sb.append("null");
      } else {
        sb.append(this.uuid);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("jobid:");
      if (this.jobid == null) {
        sb.append("null");
      } else {
        sb.append(this.jobid);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws org.apache.thrift.TException {
      // check for required fields
      // check for sub-struct validity
    }

    private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
      try {
        write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
      } catch (org.apache.thrift.TException te) {
        throw new java.io.IOException(te);
      }
    }

    private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
      try {
        read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
      } catch (org.apache.thrift.TException te) {
        throw new java.io.IOException(te);
      }
    }

    private static class heartbeat_argsStandardSchemeFactory implements SchemeFactory {
      public heartbeat_argsStandardScheme getScheme() {
        return new heartbeat_argsStandardScheme();
      }
    }

    private static class heartbeat_argsStandardScheme extends StandardScheme<heartbeat_args> {

      public void read(org.apache.thrift.protocol.TProtocol iprot, heartbeat_args struct) throws org.apache.thrift.TException {
        org.apache.thrift.protocol.TField schemeField;
        iprot.readStructBegin();
        while (true)
        {
          schemeField = iprot.readFieldBegin();
          if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
            break;
          }
          switch (schemeField.id) {
            case 1: // UUID
              if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
                struct.uuid = iprot.readString();
                struct.set_uuid_isSet(true);
              } else { 
                org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
              }
              break;
            case 2: // JOBID
              if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
                struct.jobid = iprot.readString();
                struct.set_jobid_isSet(true);
              } else { 
                org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
              }
              break;
            default:
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
          }
          iprot.readFieldEnd();
        }
        iprot.readStructEnd();
        struct.validate();
      }

      public void write(org.apache.thrift.protocol.TProtocol oprot, heartbeat_args struct) throws org.apache.thrift.TException {
        struct.validate();

        oprot.writeStructBegin(STRUCT_DESC);
        if (struct.uuid != null) {
          oprot.writeFieldBegin(UUID_FIELD_DESC);
          oprot.writeString(struct.uuid);
          oprot.writeFieldEnd();
        }
        if (struct.jobid != null) {
          oprot.writeFieldBegin(JOBID_FIELD_DESC);
          oprot.writeString(struct.jobid);
          oprot.writeFieldEnd();
        }
        oprot.writeFieldStop();
        oprot.writeStructEnd();
      }

    }

    private static class heartbeat_argsTupleSchemeFactory implements SchemeFactory {
      public heartbeat_argsTupleScheme getScheme() {
        return new heartbeat_argsTupleScheme();
      }
    }

    private static class heartbeat_argsTupleScheme extends TupleScheme<heartbeat_args> {

      @Override
      public void write(org.apache.thrift.protocol.TProtocol prot, heartbeat_args struct) throws org.apache.thrift.TException {
        TTupleProtocol oprot = (TTupleProtocol) prot;
        BitSet optionals = new BitSet();
        if (struct.is_set_uuid()) {
          optionals.set(0);
        }
        if (struct.is_set_jobid()) {
          optionals.set(1);
        }
        oprot.writeBitSet(optionals, 2);
        if (struct.is_set_uuid()) {
          oprot.writeString(struct.uuid);
        }
        if (struct.is_set_jobid()) {
          oprot.writeString(struct.jobid);
        }
      }

      @Override
      public void read(org.apache.thrift.protocol.TProtocol prot, heartbeat_args struct) throws org.apache.thrift.TException {
        TTupleProtocol iprot = (TTupleProtocol) prot;
        BitSet incoming = iprot.readBitSet(2);
        if (incoming.get(0)) {
          struct.uuid = iprot.readString();
          struct.set_uuid_isSet(true);
        }
        if (incoming.get(1)) {
          struct.jobid = iprot.readString();
          struct.set_jobid_isSet(true);
        }
      }
    }

  }

  public static class heartbeat_result implements org.apache.thrift.TBase<heartbeat_result, heartbeat_result._Fields>, java.io.Serializable, Cloneable, Comparable<heartbeat_result>   {
    private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("heartbeat_result");

    private static final org.apache.thrift.protocol.TField SUCCESS_FIELD_DESC = new org.apache.thrift.protocol.TField("success", org.apache.thrift.protocol.TType.STRING, (short)0);

    private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
    static {
      schemes.put(StandardScheme.class, new heartbeat_resultStandardSchemeFactory());
      schemes.put(TupleScheme.class, new heartbeat_resultTupleSchemeFactory());
    }

    private String success; // required

    /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
    public enum _Fields implements org.apache.thrift.TFieldIdEnum {
      SUCCESS((short)0, "success");

      private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

      static {
        for (_Fields field : EnumSet.allOf(_Fields.class)) {
          byName.put(field.getFieldName(), field);
        }
      }

      /**
       * Find the _Fields constant that matches fieldId, or null if its not found.
       */
      public static _Fields findByThriftId(int fieldId) {
        switch(fieldId) {
          case 0: // SUCCESS
            return SUCCESS;
          default:
            return null;
        }
      }

      /**
       * Find the _Fields constant that matches fieldId, throwing an exception
       * if it is not found.
       */
      public static _Fields findByThriftIdOrThrow(int fieldId) {
        _Fields fields = findByThriftId(fieldId);
        if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
        return fields;
      }

      /**
       * Find the _Fields constant that matches name, or null if its not found.
       */
      public static _Fields findByName(String name) {
        return byName.get(name);
      }

      private final short _thriftId;
      private final String _fieldName;

      _Fields(short thriftId, String fieldName) {
        _thriftId = thriftId;
        _fieldName = fieldName;
      }

      public short getThriftFieldId() {
        return _thriftId;
      }

      public String getFieldName() {
        return _fieldName;
      }
    }

    // isset id assignments
    public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
    static {
      Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
      tmpMap.put(_Fields.SUCCESS, new org.apache.thrift.meta_data.FieldMetaData("success", org.apache.thrift.TFieldRequirementType.DEFAULT, 
          new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
      metaDataMap = Collections.unmodifiableMap(tmpMap);
      org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(heartbeat_result.class, metaDataMap);
    }

    public heartbeat_result() {
    }

    public heartbeat_result(
      String success)
    {
      this();
      this.success = success;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public heartbeat_result(heartbeat_result other) {
      if (other.is_set_success()) {
        this.success = other.success;
      }
    }

    public heartbeat_result deepCopy() {
      return new heartbeat_result(this);
    }

    @Override
    public void clear() {
      this.success = null;
    }

    public String get_success() {
      return this.success;
    }

    public void set_success(String success) {
      this.success = success;
    }

    public void unset_success() {
      this.success = null;
    }

    /** Returns true if field success is set (has been assigned a value) and false otherwise */
    public boolean is_set_success() {
      return this.success != null;
    }

    public void set_success_isSet(boolean value) {
      if (!value) {
        this.success = null;
      }
    }

    public void setFieldValue(_Fields field, Object value) {
      switch (field) {
      case SUCCESS:
        if (value == null) {
          unset_success();
        } else {
          set_success((String)value);
        }
        break;

      }
    }

    public Object getFieldValue(_Fields field) {
      switch (field) {
      case SUCCESS:
        return get_success();

      }
      throw new IllegalStateException();
    }

    /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
    public boolean isSet(_Fields field) {
      if (field == null) {
        throw new IllegalArgumentException();
      }

      switch (field) {
      case SUCCESS:
        return is_set_success();
      }
      throw new IllegalStateException();
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof heartbeat_result)
        return this.equals((heartbeat_result)that);
      return false;
    }

    public boolean equals(heartbeat_result that) {
      if (that == null)
        return false;

      boolean this_present_success = true && this.is_set_success();
      boolean that_present_success = true && that.is_set_success();
      if (this_present_success || that_present_success) {
        if (!(this_present_success && that_present_success))
          return false;
        if (!this.success.equals(that.success))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      List<Object> list = new ArrayList<Object>();

      boolean present_success = true && (is_set_success());
      list.add(present_success);
      if (present_success)
        list.add(success);

      return list.hashCode();
    }

    @Override
    public int compareTo(heartbeat_result other) {
      if (!getClass().equals(other.getClass())) {
        return getClass().getName().compareTo(other.getClass().getName());
      }

      int lastComparison = 0;

      lastComparison = Boolean.valueOf(is_set_success()).compareTo(other.is_set_success());
      if (lastComparison != 0) {
        return lastComparison;
      }
      if (is_set_success()) {
        lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.success, other.success);
        if (lastComparison != 0) {
          return lastComparison;
        }
      }
      return 0;
    }

    public _Fields fieldForId(int fieldId) {
      return _Fields.findByThriftId(fieldId);
    }

    public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
      schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
      schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
      }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("heartbeat_result(");
      boolean first = true;

      sb.append("success:");
      if (this.success == null) {
        sb.append("null");
      } else {
        sb.append(this.success);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws org.apache.thrift.TException {
      // check for required fields
      // check for sub-struct validity
    }

    private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
      try {
        write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
      } catch (org.apache.thrift.TException te) {
        throw new java.io.IOException(te);
      }
    }

    private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
      try {
        read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
      } catch (org.apache.thrift.TException te) {
        throw new java.io.IOException(te);
      }
    }

    private static class heartbeat_resultStandardSchemeFactory implements SchemeFactory {
      public heartbeat_resultStandardScheme getScheme() {
        return new heartbeat_resultStandardScheme();
      }
    }

    private static class heartbeat_resultStandardScheme extends StandardScheme<heartbeat_result> {

      public void read(org.apache.thrift.protocol.TProtocol iprot, heartbeat_result struct) throws org.apache.thrift.TException {
        org.apache.thrift.protocol.TField schemeField;
        iprot.readStructBegin();
        while (true)
        {
          schemeField = iprot.readFieldBegin();
          if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
            break;
          }
          switch (schemeField.id) {
            case 0: // SUCCESS
              if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
                struct.success = iprot.readString();
                struct.set_success_isSet(true);
              } else { 
                org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
              }
              break;
            default:
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
          }
          iprot.readFieldEnd();
        }
        iprot.readStructEnd();
        struct.validate();
      }

      public void write(org.apache.thrift.protocol.TProtocol oprot, heartbeat_result struct) throws org.apache.thrift.TException {
        struct.validate();

        oprot.writeStructBegin(STRUCT_DESC);
        if (struct.success != null) {
          oprot.writeFieldBegin(SUCCESS_FIELD_DESC);
          oprot.writeString(struct.success);
          oprot.writeFieldEnd();
        }
        oprot.writeFieldStop();
        oprot.writeStructEnd();
      }

    }

    private static class heartbeat_resultTupleSchemeFactory implements SchemeFactory {
      public heartbeat_resultTupleScheme getScheme() {
        return new heartbeat_resultTupleScheme();
      }
    }

    private static class heartbeat_resultTupleScheme extends TupleScheme<heartbeat_result> {

      @Override
      public void write(org.apache.thrift.protocol.TProtocol prot, heartbeat_result struct) throws org.apache.thrift.TException {
        TTupleProtocol oprot = (TTupleProtocol) prot;
        BitSet optionals = new BitSet();
        if (struct.is_set_success()) {
          optionals.set(0);
        }
        oprot.writeBitSet(optionals, 1);
        if (struct.is_set_success()) {
          oprot.writeString(struct.success);
        }
      }

      @Override
      public void read(org.apache.thrift.protocol.TProtocol prot, heartbeat_result struct) throws org.apache.thrift.TException {
        TTupleProtocol iprot = (TTupleProtocol) prot;
        BitSet incoming = iprot.readBitSet(1);
        if (incoming.get(0)) {
          struct.success = iprot.readString();
          struct.set_success_isSet(true);
        }
      }
    }

  }

}