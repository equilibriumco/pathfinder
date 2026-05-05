use std::collections::HashMap;

use p2p_proto::{ProtobufSerializable, ToProtobuf};
use prost::Message;

use crate::consensus::height_and_round::HeightAndRound;

/// The type of the stream id.
pub type StreamId = HeightAndRound;

/// A message sent as part of a stream.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct StreamMessage<T> {
    /// The stream this message belongs to.
    pub stream_id: StreamId,
    /// The id of the message in the stream.
    pub message_id: u64,
    /// The body of the message.
    pub message: StreamMessageBody<T>,
}

impl<T: ProtobufSerializable> ProtobufSerializable for StreamMessage<T> {
    /// Convert the stream message to a byte vector that can be sent over the
    /// network.
    fn to_protobuf_bytes(&self) -> Vec<u8> {
        let proto_message = p2p_proto::consensus::StreamMessage {
            stream_id: self.stream_id.into(),
            message_id: self.message_id,
            message: match &self.message {
                StreamMessageBody::Content(content) => {
                    p2p_proto::consensus::StreamMessageVariant::Content(content.to_protobuf_bytes())
                }
                StreamMessageBody::Fin => p2p_proto::consensus::StreamMessageVariant::Fin,
            },
        };
        proto_message.to_protobuf().encode_to_vec()
    }

    /// Convert a byte vector to a stream message.
    fn from_protobuf_bytes(bytes: &[u8]) -> Result<Self, std::io::Error> {
        let proto_message = p2p_proto::consensus::StreamMessage::try_from_protobuf_bytes(bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let message = match proto_message.message {
            p2p_proto::consensus::StreamMessageVariant::Content(content) => {
                StreamMessageBody::Content(T::from_protobuf_bytes(&content)?)
            }
            p2p_proto::consensus::StreamMessageVariant::Fin => StreamMessageBody::Fin,
        };

        Ok(StreamMessage {
            stream_id: proto_message.stream_id.try_into()?,
            message_id: proto_message.message_id,
            message,
        })
    }
}

/// Body of the message in the stream.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum StreamMessageBody<T> {
    /// A message with content.
    Content(T),
    /// A message indicating the end of the stream.
    Fin,
}

/// State of an ongoing stream.
///
/// This is used to track the state of a stream and the messages
/// that have been sent and received.
#[derive(Debug)]
pub enum StreamState<T> {
    /// A stream that is receiving messages.
    Incoming(IncomingStreamState<T>),
    /// A stream that is sending messages.
    Outgoing(OutgoingStreamState<T>),
}

impl<T> StreamState<T> {
    /// Create a new incoming stream state.
    pub fn new_incoming() -> Self {
        Self::Incoming(IncomingStreamState::new())
    }

    /// Create a new outgoing stream state.
    pub fn new_outgoing() -> Self {
        Self::Outgoing(OutgoingStreamState::new())
    }
}

/// State of a stream that is receiving messages.
#[derive(Debug)]
pub struct IncomingStreamState<T> {
    /// The next message id that is expected.
    pub next_message_id: u64,
    /// The messages that have been received.
    pub received_messages: HashMap<u64, StreamMessage<T>>,
    /// The id of the message that indicates the end of the stream.
    pub fin_message_id: Option<u64>,
}

impl<T> IncomingStreamState<T> {
    fn new() -> Self {
        Self {
            next_message_id: 0,
            received_messages: HashMap::new(),
            fin_message_id: None,
        }
    }
}

/// State of a stream that is sending messages.
#[derive(Debug)]
pub struct OutgoingStreamState<T> {
    /// The id of the last message that was sent, if any.
    pub last_sent_message_id: Option<u64>,
    /// The messages that have been sent.
    pub sent_messages: HashMap<u64, T>,
    /// Whether the Fin message has been sent.
    pub fin_sent: bool,
}

impl<T> OutgoingStreamState<T> {
    fn new() -> Self {
        Self {
            last_sent_message_id: None,
            sent_messages: HashMap::new(),
            fin_sent: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use p2p_proto::common::{Address, L1DataAvailabilityMode};
    use pathfinder_common::StarknetVersion;
    use pathfinder_crypto::Felt;

    use super::*;

    #[test]
    fn test_encode_decode() {
        // Create a sample ProposalPart
        let proposal_init = p2p_proto::consensus::ProposalInit {
            height: 100,
            round: 2,
            valid_round: Some(1),
            proposer: Address(Felt::from_hex_str("0x123").unwrap()),
            timestamp: 1234567890,
            builder: Address(Felt::from_hex_str("0x456").unwrap()),
            l1_da_mode: L1DataAvailabilityMode::Calldata,
            l2_gas_price_fri: 1000,
            l1_gas_price_fri: 2000,
            l1_data_gas_price_fri: 3000,
            l1_gas_price_wei: 4000,
            l1_data_gas_price_wei: 5000,
            starknet_version: StarknetVersion::V_0_14_0.to_string(),
            version_constant_commitment: Default::default(),
        };
        let proposal = p2p_proto::consensus::ProposalPart::Init(proposal_init);

        // Create a StreamMessage with the ProposalPart
        let stream_message = StreamMessage {
            stream_id: (1, 2).into(), // HeightAndRound
            message_id: 42,
            message: StreamMessageBody::Content(proposal.clone()),
        };

        // Encode to bytes
        let bytes = stream_message.clone().to_protobuf_bytes();

        // Decode back
        let decoded =
            StreamMessage::<p2p_proto::consensus::ProposalPart>::from_protobuf_bytes(&bytes)
                .unwrap();

        // Verify the round trip
        assert_eq!(stream_message, decoded);
    }
}
