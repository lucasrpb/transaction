// Generated by the Scala Plugin for the Protocol Buffer Compiler.
// Do not edit!
//
// Protofile syntax: PROTO3

package transaction.protocol

@SerialVersionUID(0L)
final case class ReadRequest(
    keys: _root_.scala.Seq[_root_.scala.Predef.String] = _root_.scala.Seq.empty
    ) extends scalapb.GeneratedMessage with scalapb.Message[ReadRequest] with scalapb.lenses.Updatable[ReadRequest] with transaction.Command {
    @transient
    private[this] var __serializedSizeCachedValue: _root_.scala.Int = 0
    private[this] def __computeSerializedValue(): _root_.scala.Int = {
      var __size = 0
      keys.foreach { __item =>
        val __value = __item
        __size += _root_.com.google.protobuf.CodedOutputStream.computeStringSize(1, __value)
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
      keys.foreach { __v =>
        val __m = __v
        _output__.writeString(1, __m)
      };
    }
    def mergeFrom(`_input__`: _root_.com.google.protobuf.CodedInputStream): transaction.protocol.ReadRequest = {
      val __keys = (_root_.scala.collection.immutable.Vector.newBuilder[_root_.scala.Predef.String] ++= this.keys)
      var _done__ = false
      while (!_done__) {
        val _tag__ = _input__.readTag()
        _tag__ match {
          case 0 => _done__ = true
          case 10 =>
            __keys += _input__.readString()
          case tag => _input__.skipField(tag)
        }
      }
      transaction.protocol.ReadRequest(
          keys = __keys.result()
      )
    }
    def clearKeys = copy(keys = _root_.scala.Seq.empty)
    def addKeys(__vs: _root_.scala.Predef.String*): ReadRequest = addAllKeys(__vs)
    def addAllKeys(__vs: Iterable[_root_.scala.Predef.String]): ReadRequest = copy(keys = keys ++ __vs)
    def withKeys(__v: _root_.scala.Seq[_root_.scala.Predef.String]): ReadRequest = copy(keys = __v)
    def getFieldByNumber(__fieldNumber: _root_.scala.Int): _root_.scala.Any = {
      (__fieldNumber: @_root_.scala.unchecked) match {
        case 1 => keys
      }
    }
    def getField(__field: _root_.scalapb.descriptors.FieldDescriptor): _root_.scalapb.descriptors.PValue = {
      _root_.scala.Predef.require(__field.containingMessage eq companion.scalaDescriptor)
      (__field.number: @_root_.scala.unchecked) match {
        case 1 => _root_.scalapb.descriptors.PRepeated(keys.iterator.map(_root_.scalapb.descriptors.PString).toVector)
      }
    }
    def toProtoString: _root_.scala.Predef.String = _root_.scalapb.TextFormat.printToUnicodeString(this)
    def companion = transaction.protocol.ReadRequest
}

object ReadRequest extends scalapb.GeneratedMessageCompanion[transaction.protocol.ReadRequest] with transaction.Command {
  implicit def messageCompanion: scalapb.GeneratedMessageCompanion[transaction.protocol.ReadRequest] with transaction.Command = this
  def fromFieldsMap(__fieldsMap: scala.collection.immutable.Map[_root_.com.google.protobuf.Descriptors.FieldDescriptor, _root_.scala.Any]): transaction.protocol.ReadRequest = {
    _root_.scala.Predef.require(__fieldsMap.keys.forall(_.getContainingType() == javaDescriptor), "FieldDescriptor does not match message type.")
    val __fields = javaDescriptor.getFields
    transaction.protocol.ReadRequest(
      __fieldsMap.getOrElse(__fields.get(0), Nil).asInstanceOf[_root_.scala.Seq[_root_.scala.Predef.String]]
    )
  }
  implicit def messageReads: _root_.scalapb.descriptors.Reads[transaction.protocol.ReadRequest] = _root_.scalapb.descriptors.Reads{
    case _root_.scalapb.descriptors.PMessage(__fieldsMap) =>
      _root_.scala.Predef.require(__fieldsMap.keys.forall(_.containingMessage == scalaDescriptor), "FieldDescriptor does not match message type.")
      transaction.protocol.ReadRequest(
        __fieldsMap.get(scalaDescriptor.findFieldByNumber(1).get).map(_.as[_root_.scala.Seq[_root_.scala.Predef.String]]).getOrElse(_root_.scala.Seq.empty)
      )
    case _ => throw new RuntimeException("Expected PMessage")
  }
  def javaDescriptor: _root_.com.google.protobuf.Descriptors.Descriptor = TransactionProto.javaDescriptor.getMessageTypes.get(3)
  def scalaDescriptor: _root_.scalapb.descriptors.Descriptor = TransactionProto.scalaDescriptor.messages(3)
  def messageCompanionForFieldNumber(__number: _root_.scala.Int): _root_.scalapb.GeneratedMessageCompanion[_] = throw new MatchError(__number)
  lazy val nestedMessagesCompanions: Seq[_root_.scalapb.GeneratedMessageCompanion[_ <: _root_.scalapb.GeneratedMessage]] = Seq.empty
  def enumCompanionForFieldNumber(__fieldNumber: _root_.scala.Int): _root_.scalapb.GeneratedEnumCompanion[_] = throw new MatchError(__fieldNumber)
  lazy val defaultInstance = transaction.protocol.ReadRequest(
  )
  implicit class ReadRequestLens[UpperPB](_l: _root_.scalapb.lenses.Lens[UpperPB, transaction.protocol.ReadRequest]) extends _root_.scalapb.lenses.ObjectLens[UpperPB, transaction.protocol.ReadRequest](_l) {
    def keys: _root_.scalapb.lenses.Lens[UpperPB, _root_.scala.Seq[_root_.scala.Predef.String]] = field(_.keys)((c_, f_) => c_.copy(keys = f_))
  }
  final val KEYS_FIELD_NUMBER = 1
  def of(
    keys: _root_.scala.Seq[_root_.scala.Predef.String]
  ): _root_.transaction.protocol.ReadRequest = _root_.transaction.protocol.ReadRequest(
    keys
  )
}