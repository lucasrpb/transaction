// Generated by the Scala Plugin for the Protocol Buffer Compiler.
// Do not edit!
//
// Protofile syntax: PROTO3

package transaction.protocol

@SerialVersionUID(0L)
final case class Transaction(
    id: _root_.scala.Predef.String = "",
    rs: _root_.scala.Seq[transaction.protocol.MVCCVersion] = _root_.scala.Seq.empty,
    ws: _root_.scala.Seq[transaction.protocol.MVCCVersion] = _root_.scala.Seq.empty,
    partitions: _root_.scala.collection.immutable.Map[_root_.scala.Predef.String, transaction.protocol.KeyList] = _root_.scala.collection.immutable.Map.empty,
    keys: _root_.scala.Seq[_root_.scala.Predef.String] = _root_.scala.Seq.empty
    ) extends scalapb.GeneratedMessage with scalapb.Message[Transaction] with scalapb.lenses.Updatable[Transaction] with transaction.Command {
    @transient
    private[this] var __serializedSizeCachedValue: _root_.scala.Int = 0
    private[this] def __computeSerializedValue(): _root_.scala.Int = {
      var __size = 0
      
      {
        val __value = id
        if (__value != "") {
          __size += _root_.com.google.protobuf.CodedOutputStream.computeStringSize(1, __value)
        }
      };
      rs.foreach { __item =>
        val __value = __item
        __size += 1 + _root_.com.google.protobuf.CodedOutputStream.computeUInt32SizeNoTag(__value.serializedSize) + __value.serializedSize
      }
      ws.foreach { __item =>
        val __value = __item
        __size += 1 + _root_.com.google.protobuf.CodedOutputStream.computeUInt32SizeNoTag(__value.serializedSize) + __value.serializedSize
      }
      partitions.foreach { __item =>
        val __value = transaction.protocol.Transaction._typemapper_partitions.toBase(__item)
        __size += 1 + _root_.com.google.protobuf.CodedOutputStream.computeUInt32SizeNoTag(__value.serializedSize) + __value.serializedSize
      }
      keys.foreach { __item =>
        val __value = __item
        __size += _root_.com.google.protobuf.CodedOutputStream.computeStringSize(5, __value)
      }
      __size
    }
    final override def serializedSize: _root_.scala.Int = {
      var read = __serializedSizeCachedValue
      if (read == 0) {
        read = __computeSerializedValue()
        __serializedSizeCachedValue = read
      }
      read
    }
    def writeTo(`_output__`: _root_.com.google.protobuf.CodedOutputStream): _root_.scala.Unit = {
      {
        val __v = id
        if (__v != "") {
          _output__.writeString(1, __v)
        }
      };
      rs.foreach { __v =>
        val __m = __v
        _output__.writeTag(2, 2)
        _output__.writeUInt32NoTag(__m.serializedSize)
        __m.writeTo(_output__)
      };
      ws.foreach { __v =>
        val __m = __v
        _output__.writeTag(3, 2)
        _output__.writeUInt32NoTag(__m.serializedSize)
        __m.writeTo(_output__)
      };
      partitions.foreach { __v =>
        val __m = transaction.protocol.Transaction._typemapper_partitions.toBase(__v)
        _output__.writeTag(4, 2)
        _output__.writeUInt32NoTag(__m.serializedSize)
        __m.writeTo(_output__)
      };
      keys.foreach { __v =>
        val __m = __v
        _output__.writeString(5, __m)
      };
    }
    def mergeFrom(`_input__`: _root_.com.google.protobuf.CodedInputStream): transaction.protocol.Transaction = {
      var __id = this.id
      val __rs = (_root_.scala.collection.immutable.Vector.newBuilder[transaction.protocol.MVCCVersion] ++= this.rs)
      val __ws = (_root_.scala.collection.immutable.Vector.newBuilder[transaction.protocol.MVCCVersion] ++= this.ws)
      val __partitions = (_root_.scala.collection.immutable.Map.newBuilder[_root_.scala.Predef.String, transaction.protocol.KeyList] ++= this.partitions)
      val __keys = (_root_.scala.collection.immutable.Vector.newBuilder[_root_.scala.Predef.String] ++= this.keys)
      var _done__ = false
      while (!_done__) {
        val _tag__ = _input__.readTag()
        _tag__ match {
          case 0 => _done__ = true
          case 10 =>
            __id = _input__.readString()
          case 18 =>
            __rs += _root_.scalapb.LiteParser.readMessage(_input__, transaction.protocol.MVCCVersion.defaultInstance)
          case 26 =>
            __ws += _root_.scalapb.LiteParser.readMessage(_input__, transaction.protocol.MVCCVersion.defaultInstance)
          case 34 =>
            __partitions += transaction.protocol.Transaction._typemapper_partitions.toCustom(_root_.scalapb.LiteParser.readMessage(_input__, transaction.protocol.Transaction.PartitionsEntry.defaultInstance))
          case 42 =>
            __keys += _input__.readString()
          case tag => _input__.skipField(tag)
        }
      }
      transaction.protocol.Transaction(
          id = __id,
          rs = __rs.result(),
          ws = __ws.result(),
          partitions = __partitions.result(),
          keys = __keys.result()
      )
    }
    def withId(__v: _root_.scala.Predef.String): Transaction = copy(id = __v)
    def clearRs = copy(rs = _root_.scala.Seq.empty)
    def addRs(__vs: transaction.protocol.MVCCVersion*): Transaction = addAllRs(__vs)
    def addAllRs(__vs: Iterable[transaction.protocol.MVCCVersion]): Transaction = copy(rs = rs ++ __vs)
    def withRs(__v: _root_.scala.Seq[transaction.protocol.MVCCVersion]): Transaction = copy(rs = __v)
    def clearWs = copy(ws = _root_.scala.Seq.empty)
    def addWs(__vs: transaction.protocol.MVCCVersion*): Transaction = addAllWs(__vs)
    def addAllWs(__vs: Iterable[transaction.protocol.MVCCVersion]): Transaction = copy(ws = ws ++ __vs)
    def withWs(__v: _root_.scala.Seq[transaction.protocol.MVCCVersion]): Transaction = copy(ws = __v)
    def clearPartitions = copy(partitions = _root_.scala.collection.immutable.Map.empty)
    def addPartitions(__vs: (_root_.scala.Predef.String, transaction.protocol.KeyList)*): Transaction = addAllPartitions(__vs)
    def addAllPartitions(__vs: Iterable[(_root_.scala.Predef.String, transaction.protocol.KeyList)]): Transaction = copy(partitions = partitions ++ __vs)
    def withPartitions(__v: _root_.scala.collection.immutable.Map[_root_.scala.Predef.String, transaction.protocol.KeyList]): Transaction = copy(partitions = __v)
    def clearKeys = copy(keys = _root_.scala.Seq.empty)
    def addKeys(__vs: _root_.scala.Predef.String*): Transaction = addAllKeys(__vs)
    def addAllKeys(__vs: Iterable[_root_.scala.Predef.String]): Transaction = copy(keys = keys ++ __vs)
    def withKeys(__v: _root_.scala.Seq[_root_.scala.Predef.String]): Transaction = copy(keys = __v)
    def getFieldByNumber(__fieldNumber: _root_.scala.Int): _root_.scala.Any = {
      (__fieldNumber: @_root_.scala.unchecked) match {
        case 1 => {
          val __t = id
          if (__t != "") __t else null
        }
        case 2 => rs
        case 3 => ws
        case 4 => partitions.iterator.map(transaction.protocol.Transaction._typemapper_partitions.toBase).toSeq
        case 5 => keys
      }
    }
    def getField(__field: _root_.scalapb.descriptors.FieldDescriptor): _root_.scalapb.descriptors.PValue = {
      _root_.scala.Predef.require(__field.containingMessage eq companion.scalaDescriptor)
      (__field.number: @_root_.scala.unchecked) match {
        case 1 => _root_.scalapb.descriptors.PString(id)
        case 2 => _root_.scalapb.descriptors.PRepeated(rs.iterator.map(_.toPMessage).toVector)
        case 3 => _root_.scalapb.descriptors.PRepeated(ws.iterator.map(_.toPMessage).toVector)
        case 4 => _root_.scalapb.descriptors.PRepeated(partitions.iterator.map(transaction.protocol.Transaction._typemapper_partitions.toBase(_).toPMessage).toVector)
        case 5 => _root_.scalapb.descriptors.PRepeated(keys.iterator.map(_root_.scalapb.descriptors.PString).toVector)
      }
    }
    def toProtoString: _root_.scala.Predef.String = _root_.scalapb.TextFormat.printToUnicodeString(this)
    def companion = transaction.protocol.Transaction
}

object Transaction extends scalapb.GeneratedMessageCompanion[transaction.protocol.Transaction] with transaction.Command {
  implicit def messageCompanion: scalapb.GeneratedMessageCompanion[transaction.protocol.Transaction] with transaction.Command = this
  def fromFieldsMap(__fieldsMap: scala.collection.immutable.Map[_root_.com.google.protobuf.Descriptors.FieldDescriptor, _root_.scala.Any]): transaction.protocol.Transaction = {
    _root_.scala.Predef.require(__fieldsMap.keys.forall(_.getContainingType() == javaDescriptor), "FieldDescriptor does not match message type.")
    val __fields = javaDescriptor.getFields
    transaction.protocol.Transaction(
      __fieldsMap.getOrElse(__fields.get(0), "").asInstanceOf[_root_.scala.Predef.String],
      __fieldsMap.getOrElse(__fields.get(1), Nil).asInstanceOf[_root_.scala.Seq[transaction.protocol.MVCCVersion]],
      __fieldsMap.getOrElse(__fields.get(2), Nil).asInstanceOf[_root_.scala.Seq[transaction.protocol.MVCCVersion]],
      __fieldsMap.getOrElse(__fields.get(3), Nil).asInstanceOf[_root_.scala.Seq[transaction.protocol.Transaction.PartitionsEntry]].iterator.map(transaction.protocol.Transaction._typemapper_partitions.toCustom).toMap,
      __fieldsMap.getOrElse(__fields.get(4), Nil).asInstanceOf[_root_.scala.Seq[_root_.scala.Predef.String]]
    )
  }
  implicit def messageReads: _root_.scalapb.descriptors.Reads[transaction.protocol.Transaction] = _root_.scalapb.descriptors.Reads{
    case _root_.scalapb.descriptors.PMessage(__fieldsMap) =>
      _root_.scala.Predef.require(__fieldsMap.keys.forall(_.containingMessage == scalaDescriptor), "FieldDescriptor does not match message type.")
      transaction.protocol.Transaction(
        __fieldsMap.get(scalaDescriptor.findFieldByNumber(1).get).map(_.as[_root_.scala.Predef.String]).getOrElse(""),
        __fieldsMap.get(scalaDescriptor.findFieldByNumber(2).get).map(_.as[_root_.scala.Seq[transaction.protocol.MVCCVersion]]).getOrElse(_root_.scala.Seq.empty),
        __fieldsMap.get(scalaDescriptor.findFieldByNumber(3).get).map(_.as[_root_.scala.Seq[transaction.protocol.MVCCVersion]]).getOrElse(_root_.scala.Seq.empty),
        __fieldsMap.get(scalaDescriptor.findFieldByNumber(4).get).map(_.as[_root_.scala.Seq[transaction.protocol.Transaction.PartitionsEntry]]).getOrElse(_root_.scala.Seq.empty).iterator.map(transaction.protocol.Transaction._typemapper_partitions.toCustom).toMap,
        __fieldsMap.get(scalaDescriptor.findFieldByNumber(5).get).map(_.as[_root_.scala.Seq[_root_.scala.Predef.String]]).getOrElse(_root_.scala.Seq.empty)
      )
    case _ => throw new RuntimeException("Expected PMessage")
  }
  def javaDescriptor: _root_.com.google.protobuf.Descriptors.Descriptor = TransactionProto.javaDescriptor.getMessageTypes.get(6)
  def scalaDescriptor: _root_.scalapb.descriptors.Descriptor = TransactionProto.scalaDescriptor.messages(6)
  def messageCompanionForFieldNumber(__number: _root_.scala.Int): _root_.scalapb.GeneratedMessageCompanion[_] = {
    var __out: _root_.scalapb.GeneratedMessageCompanion[_] = null
    (__number: @_root_.scala.unchecked) match {
      case 2 => __out = transaction.protocol.MVCCVersion
      case 3 => __out = transaction.protocol.MVCCVersion
      case 4 => __out = transaction.protocol.Transaction.PartitionsEntry
    }
    __out
  }
  lazy val nestedMessagesCompanions: Seq[_root_.scalapb.GeneratedMessageCompanion[_ <: _root_.scalapb.GeneratedMessage]] =
    Seq[_root_.scalapb.GeneratedMessageCompanion[_ <: _root_.scalapb.GeneratedMessage]](
      _root_.transaction.protocol.Transaction.PartitionsEntry
    )
  def enumCompanionForFieldNumber(__fieldNumber: _root_.scala.Int): _root_.scalapb.GeneratedEnumCompanion[_] = throw new MatchError(__fieldNumber)
  lazy val defaultInstance = transaction.protocol.Transaction(
  )
  @SerialVersionUID(0L)
  final case class PartitionsEntry(
      key: _root_.scala.Predef.String = "",
      value: _root_.scala.Option[transaction.protocol.KeyList] = _root_.scala.None
      ) extends scalapb.GeneratedMessage with scalapb.Message[PartitionsEntry] with scalapb.lenses.Updatable[PartitionsEntry] {
      @transient
      private[this] var __serializedSizeCachedValue: _root_.scala.Int = 0
      private[this] def __computeSerializedValue(): _root_.scala.Int = {
        var __size = 0
        
        {
          val __value = key
          if (__value != "") {
            __size += _root_.com.google.protobuf.CodedOutputStream.computeStringSize(1, __value)
          }
        };
        if (value.isDefined) {
          val __value = value.get
          __size += 1 + _root_.com.google.protobuf.CodedOutputStream.computeUInt32SizeNoTag(__value.serializedSize) + __value.serializedSize
        };
        __size
      }
      final override def serializedSize: _root_.scala.Int = {
        var read = __serializedSizeCachedValue
        if (read == 0) {
          read = __computeSerializedValue()
          __serializedSizeCachedValue = read
        }
        read
      }
      def writeTo(`_output__`: _root_.com.google.protobuf.CodedOutputStream): _root_.scala.Unit = {
        {
          val __v = key
          if (__v != "") {
            _output__.writeString(1, __v)
          }
        };
        value.foreach { __v =>
          val __m = __v
          _output__.writeTag(2, 2)
          _output__.writeUInt32NoTag(__m.serializedSize)
          __m.writeTo(_output__)
        };
      }
      def mergeFrom(`_input__`: _root_.com.google.protobuf.CodedInputStream): transaction.protocol.Transaction.PartitionsEntry = {
        var __key = this.key
        var __value = this.value
        var _done__ = false
        while (!_done__) {
          val _tag__ = _input__.readTag()
          _tag__ match {
            case 0 => _done__ = true
            case 10 =>
              __key = _input__.readString()
            case 18 =>
              __value = Option(_root_.scalapb.LiteParser.readMessage(_input__, __value.getOrElse(transaction.protocol.KeyList.defaultInstance)))
            case tag => _input__.skipField(tag)
          }
        }
        transaction.protocol.Transaction.PartitionsEntry(
            key = __key,
            value = __value
        )
      }
      def withKey(__v: _root_.scala.Predef.String): PartitionsEntry = copy(key = __v)
      def getValue: transaction.protocol.KeyList = value.getOrElse(transaction.protocol.KeyList.defaultInstance)
      def clearValue: PartitionsEntry = copy(value = _root_.scala.None)
      def withValue(__v: transaction.protocol.KeyList): PartitionsEntry = copy(value = Option(__v))
      def getFieldByNumber(__fieldNumber: _root_.scala.Int): _root_.scala.Any = {
        (__fieldNumber: @_root_.scala.unchecked) match {
          case 1 => {
            val __t = key
            if (__t != "") __t else null
          }
          case 2 => value.orNull
        }
      }
      def getField(__field: _root_.scalapb.descriptors.FieldDescriptor): _root_.scalapb.descriptors.PValue = {
        _root_.scala.Predef.require(__field.containingMessage eq companion.scalaDescriptor)
        (__field.number: @_root_.scala.unchecked) match {
          case 1 => _root_.scalapb.descriptors.PString(key)
          case 2 => value.map(_.toPMessage).getOrElse(_root_.scalapb.descriptors.PEmpty)
        }
      }
      def toProtoString: _root_.scala.Predef.String = _root_.scalapb.TextFormat.printToUnicodeString(this)
      def companion = transaction.protocol.Transaction.PartitionsEntry
  }
  
  object PartitionsEntry extends scalapb.GeneratedMessageCompanion[transaction.protocol.Transaction.PartitionsEntry] {
    implicit def messageCompanion: scalapb.GeneratedMessageCompanion[transaction.protocol.Transaction.PartitionsEntry] = this
    def fromFieldsMap(__fieldsMap: scala.collection.immutable.Map[_root_.com.google.protobuf.Descriptors.FieldDescriptor, _root_.scala.Any]): transaction.protocol.Transaction.PartitionsEntry = {
      _root_.scala.Predef.require(__fieldsMap.keys.forall(_.getContainingType() == javaDescriptor), "FieldDescriptor does not match message type.")
      val __fields = javaDescriptor.getFields
      transaction.protocol.Transaction.PartitionsEntry(
        __fieldsMap.getOrElse(__fields.get(0), "").asInstanceOf[_root_.scala.Predef.String],
        __fieldsMap.get(__fields.get(1)).asInstanceOf[_root_.scala.Option[transaction.protocol.KeyList]]
      )
    }
    implicit def messageReads: _root_.scalapb.descriptors.Reads[transaction.protocol.Transaction.PartitionsEntry] = _root_.scalapb.descriptors.Reads{
      case _root_.scalapb.descriptors.PMessage(__fieldsMap) =>
        _root_.scala.Predef.require(__fieldsMap.keys.forall(_.containingMessage == scalaDescriptor), "FieldDescriptor does not match message type.")
        transaction.protocol.Transaction.PartitionsEntry(
          __fieldsMap.get(scalaDescriptor.findFieldByNumber(1).get).map(_.as[_root_.scala.Predef.String]).getOrElse(""),
          __fieldsMap.get(scalaDescriptor.findFieldByNumber(2).get).flatMap(_.as[_root_.scala.Option[transaction.protocol.KeyList]])
        )
      case _ => throw new RuntimeException("Expected PMessage")
    }
    def javaDescriptor: _root_.com.google.protobuf.Descriptors.Descriptor = transaction.protocol.Transaction.javaDescriptor.getNestedTypes.get(0)
    def scalaDescriptor: _root_.scalapb.descriptors.Descriptor = transaction.protocol.Transaction.scalaDescriptor.nestedMessages(0)
    def messageCompanionForFieldNumber(__number: _root_.scala.Int): _root_.scalapb.GeneratedMessageCompanion[_] = {
      var __out: _root_.scalapb.GeneratedMessageCompanion[_] = null
      (__number: @_root_.scala.unchecked) match {
        case 2 => __out = transaction.protocol.KeyList
      }
      __out
    }
    lazy val nestedMessagesCompanions: Seq[_root_.scalapb.GeneratedMessageCompanion[_ <: _root_.scalapb.GeneratedMessage]] = Seq.empty
    def enumCompanionForFieldNumber(__fieldNumber: _root_.scala.Int): _root_.scalapb.GeneratedEnumCompanion[_] = throw new MatchError(__fieldNumber)
    lazy val defaultInstance = transaction.protocol.Transaction.PartitionsEntry(
    )
    implicit class PartitionsEntryLens[UpperPB](_l: _root_.scalapb.lenses.Lens[UpperPB, transaction.protocol.Transaction.PartitionsEntry]) extends _root_.scalapb.lenses.ObjectLens[UpperPB, transaction.protocol.Transaction.PartitionsEntry](_l) {
      def key: _root_.scalapb.lenses.Lens[UpperPB, _root_.scala.Predef.String] = field(_.key)((c_, f_) => c_.copy(key = f_))
      def value: _root_.scalapb.lenses.Lens[UpperPB, transaction.protocol.KeyList] = field(_.getValue)((c_, f_) => c_.copy(value = Option(f_)))
      def optionalValue: _root_.scalapb.lenses.Lens[UpperPB, _root_.scala.Option[transaction.protocol.KeyList]] = field(_.value)((c_, f_) => c_.copy(value = f_))
    }
    final val KEY_FIELD_NUMBER = 1
    final val VALUE_FIELD_NUMBER = 2
    implicit val keyValueMapper: _root_.scalapb.TypeMapper[transaction.protocol.Transaction.PartitionsEntry, (_root_.scala.Predef.String, transaction.protocol.KeyList)] =
      _root_.scalapb.TypeMapper[transaction.protocol.Transaction.PartitionsEntry, (_root_.scala.Predef.String, transaction.protocol.KeyList)](__m => (__m.key, __m.getValue))(__p => transaction.protocol.Transaction.PartitionsEntry(__p._1, Some(__p._2)))
    def of(
      key: _root_.scala.Predef.String,
      value: _root_.scala.Option[transaction.protocol.KeyList]
    ): _root_.transaction.protocol.Transaction.PartitionsEntry = _root_.transaction.protocol.Transaction.PartitionsEntry(
      key,
      value
    )
  }
  
  implicit class TransactionLens[UpperPB](_l: _root_.scalapb.lenses.Lens[UpperPB, transaction.protocol.Transaction]) extends _root_.scalapb.lenses.ObjectLens[UpperPB, transaction.protocol.Transaction](_l) {
    def id: _root_.scalapb.lenses.Lens[UpperPB, _root_.scala.Predef.String] = field(_.id)((c_, f_) => c_.copy(id = f_))
    def rs: _root_.scalapb.lenses.Lens[UpperPB, _root_.scala.Seq[transaction.protocol.MVCCVersion]] = field(_.rs)((c_, f_) => c_.copy(rs = f_))
    def ws: _root_.scalapb.lenses.Lens[UpperPB, _root_.scala.Seq[transaction.protocol.MVCCVersion]] = field(_.ws)((c_, f_) => c_.copy(ws = f_))
    def partitions: _root_.scalapb.lenses.Lens[UpperPB, _root_.scala.collection.immutable.Map[_root_.scala.Predef.String, transaction.protocol.KeyList]] = field(_.partitions)((c_, f_) => c_.copy(partitions = f_))
    def keys: _root_.scalapb.lenses.Lens[UpperPB, _root_.scala.Seq[_root_.scala.Predef.String]] = field(_.keys)((c_, f_) => c_.copy(keys = f_))
  }
  final val ID_FIELD_NUMBER = 1
  final val RS_FIELD_NUMBER = 2
  final val WS_FIELD_NUMBER = 3
  final val PARTITIONS_FIELD_NUMBER = 4
  final val KEYS_FIELD_NUMBER = 5
  @transient
  private val _typemapper_partitions: _root_.scalapb.TypeMapper[transaction.protocol.Transaction.PartitionsEntry, (_root_.scala.Predef.String, transaction.protocol.KeyList)] = implicitly[_root_.scalapb.TypeMapper[transaction.protocol.Transaction.PartitionsEntry, (_root_.scala.Predef.String, transaction.protocol.KeyList)]]
  def of(
    id: _root_.scala.Predef.String,
    rs: _root_.scala.Seq[transaction.protocol.MVCCVersion],
    ws: _root_.scala.Seq[transaction.protocol.MVCCVersion],
    partitions: _root_.scala.collection.immutable.Map[_root_.scala.Predef.String, transaction.protocol.KeyList],
    keys: _root_.scala.Seq[_root_.scala.Predef.String]
  ): _root_.transaction.protocol.Transaction = _root_.transaction.protocol.Transaction(
    id,
    rs,
    ws,
    partitions,
    keys
  )
}
