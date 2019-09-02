// Generated by the Scala Plugin for the Protocol Buffer Compiler.
// Do not edit!
//
// Protofile syntax: PROTO3

package transaction.protocol

object TransactionProto extends _root_.scalapb.GeneratedFileObject {
  lazy val dependencies: Seq[_root_.scalapb.GeneratedFileObject] = Seq(
    scalapb.options.ScalapbProto
  )
  lazy val messagesCompanions: Seq[_root_.scalapb.GeneratedMessageCompanion[_ <: _root_.scalapb.GeneratedMessage]] =
    Seq[_root_.scalapb.GeneratedMessageCompanion[_ <: _root_.scalapb.GeneratedMessage]](
      transaction.protocol.Transaction,
      transaction.protocol.Partition,
      transaction.protocol.PartitionResponse,
      transaction.protocol.PartitionRequest,
      transaction.protocol.PartitionRelease,
      transaction.protocol.Batch,
      transaction.protocol.Epoch,
      transaction.protocol.GetOffset,
      transaction.protocol.OffsetResult,
      transaction.protocol.Ack,
      transaction.protocol.Read,
      transaction.protocol.VersionedValue,
      transaction.protocol.ReadResult,
      transaction.protocol.Nack
    )
  private lazy val ProtoBytes: Array[Byte] =
      scalapb.Encoding.fromBase64(scala.collection.immutable.Seq(
  """ChF0cmFuc2FjdGlvbi5wcm90bxoVc2NhbGFwYi9zY2FsYXBiLnByb3RvIu8CCgtUcmFuc2FjdGlvbhIXCgJpZBgBIAEoCUIH4
  j8EEgJpZFICaWQSLQoCcnMYAiADKAsyFC5UcmFuc2FjdGlvbi5Sc0VudHJ5QgfiPwQSAnJzUgJycxItCgJ3cxgDIAMoCzIULlRyY
  W5zYWN0aW9uLldzRW50cnlCB+I/BBICd3NSAndzGlwKB1JzRW50cnkSGgoDa2V5GAEgASgJQgjiPwUSA2tleVIDa2V5EjEKBXZhb
  HVlGAIgASgLMg8uVmVyc2lvbmVkVmFsdWVCCuI/BxIFdmFsdWVSBXZhbHVlOgI4ARpcCgdXc0VudHJ5EhoKA2tleRgBIAEoCUII4
  j8FEgNrZXlSA2tleRIxCgV2YWx1ZRgCIAEoCzIPLlZlcnNpb25lZFZhbHVlQgriPwcSBXZhbHVlUgV2YWx1ZToCOAE6LeI/KgoTd
  HJhbnNhY3Rpb24uQ29tbWFuZBITdHJhbnNhY3Rpb24uQ29tbWFuZCJvCglQYXJ0aXRpb24SFwoCaWQYASABKAlCB+I/BBICaWRSA
  mlkEhoKA3R4cxgCIAMoCUII4j8FEgN0eHNSA3R4czot4j8qChN0cmFuc2FjdGlvbi5Db21tYW5kEhN0cmFuc2FjdGlvbi5Db21tY
  W5kIrQBChFQYXJ0aXRpb25SZXNwb25zZRIXCgJpZBgBIAEoCUIH4j8EEgJpZFICaWQSLwoKY29uZmxpY3RlZBgCIAMoCUIP4j8ME
  gpjb25mbGljdGVkUgpjb25mbGljdGVkEiYKB2FwcGxpZWQYAyADKAlCDOI/CRIHYXBwbGllZFIHYXBwbGllZDot4j8qChN0cmFuc
  2FjdGlvbi5Db21tYW5kEhN0cmFuc2FjdGlvbi5Db21tYW5kIosBChBQYXJ0aXRpb25SZXF1ZXN0EhcKAmlkGAEgASgJQgfiPwQSA
  mlkUgJpZBIvCgpwYXJ0aXRpb25zGAIgAygJQg/iPwwSCnBhcnRpdGlvbnNSCnBhcnRpdGlvbnM6LeI/KgoTdHJhbnNhY3Rpb24uQ
  29tbWFuZBITdHJhbnNhY3Rpb24uQ29tbWFuZCKLAQoQUGFydGl0aW9uUmVsZWFzZRIXCgJpZBgBIAEoCUIH4j8EEgJpZFICaWQSL
  woKcGFydGl0aW9ucxgCIAMoCUIP4j8MEgpwYXJ0aXRpb25zUgpwYXJ0aXRpb25zOi3iPyoKE3RyYW5zYWN0aW9uLkNvbW1hbmQSE
  3RyYW5zYWN0aW9uLkNvbW1hbmQihQQKBUJhdGNoEhcKAmlkGAEgASgJQgfiPwQSAmlkUgJpZBIyCgtjb29yZGluYXRvchgCIAEoC
  UIQ4j8NEgtjb29yZGluYXRvclILY29vcmRpbmF0b3ISTwoMdHJhbnNhY3Rpb25zGAMgAygLMhguQmF0Y2guVHJhbnNhY3Rpb25zR
  W50cnlCEeI/DhIMdHJhbnNhY3Rpb25zUgx0cmFuc2FjdGlvbnMSRwoKcGFydGl0aW9ucxgEIAMoCzIWLkJhdGNoLlBhcnRpdGlvb
  nNFbnRyeUIP4j8MEgpwYXJ0aXRpb25zUgpwYXJ0aXRpb25zEiAKBXRvdGFsGAUgASgNQgriPwcSBXRvdGFsUgV0b3RhbBpjChFUc
  mFuc2FjdGlvbnNFbnRyeRIaCgNrZXkYASABKAlCCOI/BRIDa2V5UgNrZXkSLgoFdmFsdWUYAiABKAsyDC5UcmFuc2FjdGlvbkIK4
  j8HEgV2YWx1ZVIFdmFsdWU6AjgBGl8KD1BhcnRpdGlvbnNFbnRyeRIaCgNrZXkYASABKAlCCOI/BRIDa2V5UgNrZXkSLAoFdmFsd
  WUYAiABKAsyCi5QYXJ0aXRpb25CCuI/BxIFdmFsdWVSBXZhbHVlOgI4ATot4j8qChN0cmFuc2FjdGlvbi5Db21tYW5kEhN0cmFuc
  2FjdGlvbi5Db21tYW5kIncKBUVwb2NoEhcKAmlkGAEgASgJQgfiPwQSAmlkUgJpZBImCgdiYXRjaGVzGAIgAygJQgziPwkSB2Jhd
  GNoZXNSB2JhdGNoZXM6LeI/KgoTdHJhbnNhY3Rpb24uQ29tbWFuZBITdHJhbnNhY3Rpb24uQ29tbWFuZCI6CglHZXRPZmZzZXQ6L
  eI/KgoTdHJhbnNhY3Rpb24uQ29tbWFuZBITdHJhbnNhY3Rpb24uQ29tbWFuZCJiCgxPZmZzZXRSZXN1bHQSIwoGb2Zmc2V0GAEgA
  SgEQgviPwgSBm9mZnNldFIGb2Zmc2V0Oi3iPyoKE3RyYW5zYWN0aW9uLkNvbW1hbmQSE3RyYW5zYWN0aW9uLkNvbW1hbmQiNAoDQ
  WNrOi3iPyoKE3RyYW5zYWN0aW9uLkNvbW1hbmQSE3RyYW5zYWN0aW9uLkNvbW1hbmQiVAoEUmVhZBIdCgRrZXlzGAEgAygJQgniP
  wYSBGtleXNSBGtleXM6LeI/KgoTdHJhbnNhY3Rpb24uQ29tbWFuZBITdHJhbnNhY3Rpb24uQ29tbWFuZCKJAQoOVmVyc2lvbmVkV
  mFsdWUSJgoHdmVyc2lvbhgBIAEoCUIM4j8JEgd2ZXJzaW9uUgd2ZXJzaW9uEiAKBXZhbHVlGAIgASgEQgriPwcSBXZhbHVlUgV2Y
  Wx1ZTot4j8qChN0cmFuc2FjdGlvbi5Db21tYW5kEhN0cmFuc2FjdGlvbi5Db21tYW5kItsBCgpSZWFkUmVzdWx0EjwKBnZhbHVlc
  xgBIAMoCzIXLlJlYWRSZXN1bHQuVmFsdWVzRW50cnlCC+I/CBIGdmFsdWVzUgZ2YWx1ZXMaYAoLVmFsdWVzRW50cnkSGgoDa2V5G
  AEgASgJQgjiPwUSA2tleVIDa2V5EjEKBXZhbHVlGAIgASgLMg8uVmVyc2lvbmVkVmFsdWVCCuI/BxIFdmFsdWVSBXZhbHVlOgI4A
  Tot4j8qChN0cmFuc2FjdGlvbi5Db21tYW5kEhN0cmFuc2FjdGlvbi5Db21tYW5kIjUKBE5hY2s6LeI/KgoTdHJhbnNhY3Rpb24uQ
  29tbWFuZBITdHJhbnNhY3Rpb24uQ29tbWFuZEId4j8aChR0cmFuc2FjdGlvbi5wcm90b2NvbBABWABiBnByb3RvMw=="""
      ).mkString)
  lazy val scalaDescriptor: _root_.scalapb.descriptors.FileDescriptor = {
    val scalaProto = com.google.protobuf.descriptor.FileDescriptorProto.parseFrom(ProtoBytes)
    _root_.scalapb.descriptors.FileDescriptor.buildFrom(scalaProto, dependencies.map(_.scalaDescriptor))
  }
  lazy val javaDescriptor: com.google.protobuf.Descriptors.FileDescriptor = {
    val javaProto = com.google.protobuf.DescriptorProtos.FileDescriptorProto.parseFrom(ProtoBytes)
    com.google.protobuf.Descriptors.FileDescriptor.buildFrom(javaProto, Array(
      scalapb.options.ScalapbProto.javaDescriptor
    ))
  }
  @deprecated("Use javaDescriptor instead. In a future version this will refer to scalaDescriptor.", "ScalaPB 0.5.47")
  def descriptor: com.google.protobuf.Descriptors.FileDescriptor = javaDescriptor
}