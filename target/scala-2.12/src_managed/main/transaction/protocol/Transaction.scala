// Generated by the Scala Plugin for the Protocol Buffer Compiler.
// Do not edit!
//
// Protofile syntax: PROTO3

package transaction.protocol

@SerialVersionUID(0L)
final case class Transaction(
    id: _root_.scala.Predef.String = "",
    rs: _root_.scala.Seq[_root_.scala.Predef.String] = _root_.scala.Seq.empty,
    ws: _root_.scala.Seq[_root_.scala.Predef.String] = _root_.scala.Seq.empty
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
        __size += _root_.com.google.protobuf.CodedOutputStream.computeStringSize(2, __value)
      }
      ws.foreach { __item =>
        val __value = __item
        __size += _root_.com.google.protobuf.CodedOutputStream.computeStringSize(3, __value)
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
        _output__.writeString(2, __m)
      };
      ws.foreach { __v =>
        val __m = __v
        _output__.writeString(3, __m)
      };
    }
    def mergeFrom(`_input__`: _root_.com.google.protobuf.CodedInputStream): transaction.protocol.Transaction = {
      var __id = this.id
      val __rs = (_root_.scala.collection.immutable.Vector.newBuilder[_root_.scala.Predef.String] ++= this.rs)
      val __ws = (_root_.scala.collection.immutable.Vector.newBuilder[_root_.scala.Predef.String] ++= this.ws)
      var _done__ = false
      while (!_done__) {
        val _tag__ = _input__.readTag()
        _tag__ match {
          case 0 => _done__ = true
          case 10 =>
            __id = _input__.readString()
          case 18 =>
            __rs += _input__.readString()
          case 26 =>
            __ws += _input__.readString()
          case tag => _input__.skipField(tag)
        }
      }
      transaction.protocol.Transaction(
          id = __id,
          rs = __rs.result(),
          ws = __ws.result()
      )
    }
    def withId(__v: _root_.scala.Predef.String): Transaction = copy(id = __v)
    def clearRs = copy(rs = _root_.scala.Seq.empty)
    def addRs(__vs: _root_.scala.Predef.String*): Transaction = addAllRs(__vs)
    def addAllRs(__vs: Iterable[_root_.scala.Predef.String]): Transaction = copy(rs = rs ++ __vs)
    def withRs(__v: _root_.scala.Seq[_root_.scala.Predef.String]): Transaction = copy(rs = __v)
    def clearWs = copy(ws = _root_.scala.Seq.empty)
    def addWs(__vs: _root_.scala.Predef.String*): Transaction = addAllWs(__vs)
    def addAllWs(__vs: Iterable[_root_.scala.Predef.String]): Transaction = copy(ws = ws ++ __vs)
    def withWs(__v: _root_.scala.Seq[_root_.scala.Predef.String]): Transaction = copy(ws = __v)
    def getFieldByNumber(__fieldNumber: _root_.scala.Int): _root_.scala.Any = {
      (__fieldNumber: @_root_.scala.unchecked) match {
        case 1 => {
          val __t = id
          if (__t != "") __t else null
        }
        case 2 => rs
        case 3 => ws
      }
    }
    def getField(__field: _root_.scalapb.descriptors.FieldDescriptor): _root_.scalapb.descriptors.PValue = {
      _root_.scala.Predef.require(__field.containingMessage eq companion.scalaDescriptor)
      (__field.number: @_root_.scala.unchecked) match {
        case 1 => _root_.scalapb.descriptors.PString(id)
        case 2 => _root_.scalapb.descriptors.PRepeated(rs.iterator.map(_root_.scalapb.descriptors.PString).toVector)
        case 3 => _root_.scalapb.descriptors.PRepeated(ws.iterator.map(_root_.scalapb.descriptors.PString).toVector)
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
      __fieldsMap.getOrElse(__fields.get(1), Nil).asInstanceOf[_root_.scala.Seq[_root_.scala.Predef.String]],
      __fieldsMap.getOrElse(__fields.get(2), Nil).asInstanceOf[_root_.scala.Seq[_root_.scala.Predef.String]]
    )
  }
  implicit def messageReads: _root_.scalapb.descriptors.Reads[transaction.protocol.Transaction] = _root_.scalapb.descriptors.Reads{
    case _root_.scalapb.descriptors.PMessage(__fieldsMap) =>
      _root_.scala.Predef.require(__fieldsMap.keys.forall(_.containingMessage == scalaDescriptor), "FieldDescriptor does not match message type.")
      transaction.protocol.Transaction(
        __fieldsMap.get(scalaDescriptor.findFieldByNumber(1).get).map(_.as[_root_.scala.Predef.String]).getOrElse(""),
        __fieldsMap.get(scalaDescriptor.findFieldByNumber(2).get).map(_.as[_root_.scala.Seq[_root_.scala.Predef.String]]).getOrElse(_root_.scala.Seq.empty),
        __fieldsMap.get(scalaDescriptor.findFieldByNumber(3).get).map(_.as[_root_.scala.Seq[_root_.scala.Predef.String]]).getOrElse(_root_.scala.Seq.empty)
      )
    case _ => throw new RuntimeException("Expected PMessage")
  }
  def javaDescriptor: _root_.com.google.protobuf.Descriptors.Descriptor = TransactionProto.javaDescriptor.getMessageTypes.get(5)
  def scalaDescriptor: _root_.scalapb.descriptors.Descriptor = TransactionProto.scalaDescriptor.messages(5)
  def messageCompanionForFieldNumber(__number: _root_.scala.Int): _root_.scalapb.GeneratedMessageCompanion[_] = throw new MatchError(__number)
  lazy val nestedMessagesCompanions: Seq[_root_.scalapb.GeneratedMessageCompanion[_ <: _root_.scalapb.GeneratedMessage]] = Seq.empty
  def enumCompanionForFieldNumber(__fieldNumber: _root_.scala.Int): _root_.scalapb.GeneratedEnumCompanion[_] = throw new MatchError(__fieldNumber)
  lazy val defaultInstance = transaction.protocol.Transaction(
  )
  implicit class TransactionLens[UpperPB](_l: _root_.scalapb.lenses.Lens[UpperPB, transaction.protocol.Transaction]) extends _root_.scalapb.lenses.ObjectLens[UpperPB, transaction.protocol.Transaction](_l) {
    def id: _root_.scalapb.lenses.Lens[UpperPB, _root_.scala.Predef.String] = field(_.id)((c_, f_) => c_.copy(id = f_))
    def rs: _root_.scalapb.lenses.Lens[UpperPB, _root_.scala.Seq[_root_.scala.Predef.String]] = field(_.rs)((c_, f_) => c_.copy(rs = f_))
    def ws: _root_.scalapb.lenses.Lens[UpperPB, _root_.scala.Seq[_root_.scala.Predef.String]] = field(_.ws)((c_, f_) => c_.copy(ws = f_))
  }
  final val ID_FIELD_NUMBER = 1
  final val RS_FIELD_NUMBER = 2
  final val WS_FIELD_NUMBER = 3
  def of(
    id: _root_.scala.Predef.String,
    rs: _root_.scala.Seq[_root_.scala.Predef.String],
    ws: _root_.scala.Seq[_root_.scala.Predef.String]
  ): _root_.transaction.protocol.Transaction = _root_.transaction.protocol.Transaction(
    id,
    rs,
    ws
  )
}
