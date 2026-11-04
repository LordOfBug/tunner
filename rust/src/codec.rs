use crate::block::{Block, PADDING_BYTE};
use bytes::{Bytes, BytesMut};
use std::io::{Error, ErrorKind};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub async fn read_block<R: AsyncRead + Unpin>(reader: &mut R) -> std::io::Result<Option<Block>> {
    let mut header = [0u8; 8];
    match reader.read_exact(&mut header).await {
        Ok(_) => {}
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }

    let type_seq = u16::from_be_bytes([header[0], header[1]]);
    let len = u16::from_be_bytes([header[2], header[3]]) as usize;
    let conn_id = u32::from_be_bytes([header[4], header[5], header[6], header[7]]);

    let data = if len > 0 {
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf).await?;

        // Read 8-byte alignment padding if present
        let rem = len % 8;
        if rem > 0 {
            let pad_len = 8 - rem;
            let mut pad = [0u8; 8];
            reader.read_exact(&mut pad[..pad_len]).await?;
        }
        Bytes::from(buf)
    } else {
        Bytes::new()
    };

    Ok(Some(Block {
        type_seq,
        conn_id,
        data,
    }))
}

pub async fn write_block<W: AsyncWrite + Unpin>(
    writer: &mut W,
    block: &Block,
) -> std::io::Result<()> {
    let len = block.data.len();
    if len > u16::MAX as usize {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "Block data exceeds max size",
        ));
    }

    let rem = len % 8;
    let pad_len = if rem == 0 { 0 } else { 8 - rem };
    let total_len = 8 + len + pad_len;

    let mut buf = BytesMut::with_capacity(total_len);
    buf.extend_from_slice(&block.type_seq.to_be_bytes());
    buf.extend_from_slice(&(len as u16).to_be_bytes());
    buf.extend_from_slice(&block.conn_id.to_be_bytes());

    if len > 0 {
        buf.extend_from_slice(&block.data);
    }

    if pad_len > 0 {
        buf.resize(total_len, PADDING_BYTE);
    }

    writer.write_all(&buf).await?;
    writer.flush().await?;
    Ok(())
}

