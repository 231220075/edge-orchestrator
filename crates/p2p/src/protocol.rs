//! Custom request-response protocol for exchanging [`NodeDescriptor`]s.
//!
//! Protocol: `/edge-orch/descriptor/1.0.0`
//! - Request: `DescriptorRequest` (who is asking)
//! - Response: `DescriptorResponse` (serialized `NodeDescriptor`)
//!
//! Uses JSON with a 4-byte big-endian length prefix for framing.

use eo_core::types::NodeDescriptor;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::request_response;
use libp2p::StreamProtocol;
use serde::{Deserialize, Serialize};

/// The protocol name for descriptor exchange.
const DESCRIPTOR_PROTOCOL: &str = "/edge-orch/descriptor/1.0.0";

/// Maximum size for a descriptor request (128 bytes).
const REQUEST_MAX_SIZE: usize = 128;

/// Maximum size for a descriptor response (8 KiB).
const RESPONSE_MAX_SIZE: usize = 8 * 1024;

/// A request for a peer's [`NodeDescriptor`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescriptorRequest {
    /// The node ID of the requester.
    pub requester_id: String,
}

/// Response containing this node's [`NodeDescriptor`] as JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescriptorResponse {
    /// The serialized descriptor (JSON-encoded).
    pub descriptor_json: String,
}

impl DescriptorResponse {
    /// Create a response from a [`NodeDescriptor`] by serializing it to JSON.
    pub fn from_descriptor(desc: &NodeDescriptor) -> Result<Self, serde_json::Error> {
        Ok(Self {
            descriptor_json: serde_json::to_string(desc)?,
        })
    }

    /// Deserialize the contained JSON back into a [`NodeDescriptor`].
    pub fn to_descriptor(&self) -> Result<NodeDescriptor, serde_json::Error> {
        serde_json::from_str(&self.descriptor_json)
    }
}

/// The codec for the descriptor exchange protocol.
#[derive(Debug, Clone, Default)]
pub struct DescriptorCodec;

impl DescriptorCodec {
    /// Return the protocol name as a [`StreamProtocol`].
    pub fn protocol() -> StreamProtocol {
        StreamProtocol::new(DESCRIPTOR_PROTOCOL)
    }
}

#[async_trait::async_trait]
impl request_response::Codec for DescriptorCodec {
    type Protocol = StreamProtocol;
    type Request = DescriptorRequest;
    type Response = DescriptorResponse;

    async fn read_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let data = read_length_prefixed(io, REQUEST_MAX_SIZE).await?;
        serde_json::from_slice(&data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    async fn read_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let data = read_length_prefixed(io, RESPONSE_MAX_SIZE).await?;
        serde_json::from_slice(&data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    async fn write_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> std::io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let data = serde_json::to_vec(&req)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        write_length_prefixed(io, &data).await
    }

    async fn write_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        resp: Self::Response,
    ) -> std::io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let data = serde_json::to_vec(&resp)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        write_length_prefixed(io, &data).await
    }
}

// ---------------------------------------------------------------------------
// Length-prefixed framing helpers
// ---------------------------------------------------------------------------

async fn read_length_prefixed<T: AsyncRead + Unpin + Send>(
    io: &mut T,
    max_size: usize,
) -> std::io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    io.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > max_size {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("payload size {len} exceeds max {max_size}"),
        ));
    }
    let mut buf = vec![0u8; len];
    io.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn write_length_prefixed<T: AsyncWrite + Unpin + Send>(
    io: &mut T,
    data: &[u8],
) -> std::io::Result<()> {
    let len = data.len() as u32;
    io.write_all(&len.to_be_bytes()).await?;
    io.write_all(data).await?;
    Ok(())
}
