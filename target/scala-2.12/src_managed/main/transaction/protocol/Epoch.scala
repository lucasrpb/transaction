// Generated by the Scala Plugin for the Protocol Buffer Compiler.
// Do not edit!
//
// Protofile syntax: PROTO3

package transaction.protocol

@SerialVersionUID(0L)
final case class Epoch(
    id: _root_.scala.Predef.String = "",
    batches: _root_.scala.Seq[transaction.protocol.BatchInfo] = _root_.scala.Seq.empty
    ) extends scalapb.GeneratedMessage with scalapb.Message[Epoch] with scalapb.lenses.Updatable[Epoch] with transaction.Command {
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
      batches.foreach { __item =>
        val __value = __item
        __size += 1 + _root_.com.google.protobuf.CodedOutputStream.computeUInt32SizeNoTag(__value.serializedSize) + __value.serializedSize
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
      batches.foreach { __v =>
        val __m = __v
        _output__.writeTag(2, 2)
        _output__.writeUInt32NoTag(__m.serializedSize)
        __m.writeTo(_output__)
      };
    }
    def mergeFrom(`_input__`: _root_.com.google.protobuf.CodedInputStream): transaction.protocol.Epoch = {
      var __id = this.id
      val __batches = (_root_.scala.collection.immutable.Vector.newBuilder[transaction.protocol.BatchInfo] ++= this.batches)
      var _done__ = false
      while (!_done__) {
        val _tag__ = _input__.readTag()
        _tag__ match {
          case 0 => _done__ = true
          case 10 =>
            __id = _input__.readString()
          case 18 =>
            __batches += _root_.scalapb.LiteParser.readMessage(_input__, transaction.protocol.BatchInfo.defaultInstance)
          case tag => _input__.skipField(tag)
        }
      }
      transaction.protocol.Epoch(
          id = __id,
          batches = __batches.result()
      )
    }
    def withId(__v: _root_.scala.Predef.String): Epoch = copy(id = __v)
    def clearBatches = copy(batches = _root_.scala.Seq.empty)
    def addBatches(__vs: transaction.protocol.BatchInfo*): Epoch = addAllBatches(__vs)
    def addAllBatches(__vs: Iterable[transaction.protocol.BatchInfo]): Epoch = copy(batches = batches ++ __vs)
    def withBatches(__v: _root_.scala.Seq[transaction.protocol.BatchInfo]): Epoch = copy(batches = __v)
    def getFieldByNumber(__fieldNumber: _root_.scala.Int): _root_.scala.Any = {
      (__fieldNumber: @_root_.scala.unchecked) match {
        case 1 => {
          val __t = id
          if (__t != "") __t else null
        }
        case 2 => batches
      }
    }
    def getField(__field: _root_.scalapb.descriptors.FieldDescriptor): _root_.scalapb.descriptors.PValue = {
      _root_.scala.Predef.require(__field.containingMessage eq companion.scalaDescriptor)
      (__field.number: @_root_.scala.unchecked) match {
        case 1 => _root_.scalapb.descriptors.PString(id)
        case 2 => _root_.scalapb.descriptors.PRepeated(batches.iterator.map(_.toPMessage).toVector)
      }
    }
    def toProtoString: _root_.scala.Predef.String = _root_.scalapb.TextFormat.printToUnicodeString(this)
    def companion = transaction.protocol.Epoch
}

object Epoch extends scalapb.GeneratedMessageCompanion[transaction.protocol.Epoch] with transaction.Command {
  implicit def messageCompanion: scalapb.GeneratedMessageCompanion[transaction.protocol.Epoch] with transaction.Command = this
  def fromFieldsMap(__fieldsMap: scala.collection.immutable.Map[_root_.com.google.protobuf.Descriptors.FieldDescriptor, _root_.scala.Any]): transaction.protocol.Epoch = {
    _root_.scala.Predef.require(__fieldsMap.keys.forall(_.getContainingType() == javaDescriptor), "FieldDescriptor does not match message type.")
    val __fields = javaDescriptor.getFields
    transaction.protocol.Epoch(
      __fieldsMap.getOrElse(__fields.get(0), "").asInstanceOf[_root_.scala.Predef.String],
      __fieldsMap.getOrElse(__fields.get(1), Nil).asInstanceOf[_root_.scala.Seq[transaction.protocol.BatchInfo]]
    )
  }
  implicit def messageReads: _root_.scalapb.descriptors.Reads[transaction.protocol.Epoch] = _root_.scalapb.descriptors.Reads{
    case _root_.scalapb.descriptors.PMessage(__fieldsMap) =>
      _root_.scala.Predef.require(__fieldsMap.keys.forall(_.containingMessage == scalaDescriptor), "FieldDescriptor does not match message type.")
      transaction.protocol.Epoch(
        __fieldsMap.get(scalaDescriptor.findFieldByNumber(1).get).map(_.as[_root_.scala.Predef.String]).getOrElse(""),
        __fieldsMap.get(scalaDescriptor.findFieldByNumber(2).get).map(_.as[_root_.scala.Seq[transaction.protocol.BatchInfo]]).getOrElse(_root_.scala.Seq.empty)
      )
    case _ => throw new RuntimeException("Expected PMessage")
  }
  def javaDescriptor: _root_.com.google.protobuf.Descriptors.Descriptor = TransactionProto.javaDescriptor.getMessageTypes.get(13)
  def scalaDescriptor: _root_.scalapb.descriptors.Descriptor = TransactionProto.scalaDescriptor.messages(13)
  def messageCompanionForFieldNumber(__number: _root_.scala.Int): _root_.scalapb.GeneratedMessageCompanion[_] = {
    var __out: _root_.scalapb.GeneratedMessageCompanion[_] = null
    (__number: @_root_.scala.unchecked) match {
      case 2 => __out = transaction.protocol.BatchInfo
    }
    __out
  }
  lazy val nestedMessagesCompanions: Seq[_root_.scalapb.GeneratedMessageCompanion[_ <: _root_.scalapb.GeneratedMessage]] = Seq.empty
  def enumCompanionForFieldNumber(__fieldNumber: _root_.scala.Int): _root_.scalapb.GeneratedEnumCompanion[_] = throw new MatchError(__fieldNumber)
  lazy val defaultInstance = transaction.protocol.Epoch(
  )
  implicit class EpochLens[UpperPB](_l: _root_.scalapb.lenses.Lens[UpperPB, transaction.protocol.Epoch]) extends _root_.scalapb.lenses.ObjectLens[UpperPB, transaction.protocol.Epoch](_l) {
    def id: _root_.scalapb.lenses.Lens[UpperPB, _root_.scala.Predef.String] = field(_.id)((c_, f_) => c_.copy(id = f_))
    def batches: _root_.scalapb.lenses.Lens[UpperPB, _root_.scala.Seq[transaction.protocol.BatchInfo]] = field(_.batches)((c_, f_) => c_.copy(batches = f_))
  }
  final val ID_FIELD_NUMBER = 1
  final val BATCHES_FIELD_NUMBER = 2
  def of(
    id: _root_.scala.Predef.String,
    batches: _root_.scala.Seq[transaction.protocol.BatchInfo]
  ): _root_.transaction.protocol.Epoch = _root_.transaction.protocol.Epoch(
    id,
    batches
  )
}
