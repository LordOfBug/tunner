use bytes::Bytes;

pub const BLOCK_DATA: u16 = 0x4000;
pub const BLOCK_CONTROL: u16 = 0x8000;

pub const CODE_ECHO: u16 = 0;
pub const CODE_START: u16 = 1;
/// Receiver allows sender to resume reading its local socket for this conn.
pub const CODE_RESUME: u16 = 2;
pub const CODE_NOISE: u16 = 3;
pub const CODE_EOF: u16 = 4;
/// Receiver asks sender to stop reading its local socket for this conn (backpressure).
pub const CODE_PAUSE: u16 = 5;
/// Keepalive frame (conn_id 0); any received frame refreshes liveness.
pub const CODE_PING: u16 = 6;
/// Keepalive / RTT measurement reply echoing the 8-byte timestamp payload of `CODE_PING`.
pub const CODE_PONG: u16 = 7;
pub const CODE_ABORT: u16 = 0x00FF;

pub const MAX_LENGTH: usize = 0xFFFF;
pub const MAX_NO_PADDING_LENGTH: usize = 0xFFF8;
pub const PADDING_BYTE: u8 = 0xFF;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockType {
    Data { sequence: u16 },
    Control { code: u16 },
    Unknown(u16),
}

#[derive(Debug, Clone)]
pub struct Block {
    pub type_seq: u16,
    pub conn_id: u32,
    pub data: Bytes,
}

impl Block {
    pub fn new_control(conn_id: u32, code: u16) -> Self {
        Self {
            type_seq: BLOCK_CONTROL | (code & 0x3FFF),
            conn_id,
            data: Bytes::new(),
        }
    }

    pub fn new_control_with_data(conn_id: u32, code: u16, data: Bytes) -> Self {
        Self {
            type_seq: BLOCK_CONTROL | (code & 0x3FFF),
            conn_id,
            data,
        }
    }

    pub fn new_data(conn_id: u32, seq: u16, data: Bytes) -> Self {
        Self {
            type_seq: BLOCK_DATA | (seq & 0x3FFF),
            conn_id,
            data,
        }
    }

    pub fn block_type(&self) -> BlockType {
        let tag = self.type_seq & 0xC000;
        if tag == BLOCK_DATA {
            BlockType::Data {
                sequence: self.type_seq & 0x3FFF,
            }
        } else if tag == BLOCK_CONTROL {
            BlockType::Control {
                code: self.type_seq & 0x3FFF,
            }
        } else {
            BlockType::Unknown(self.type_seq)
        }
    }

    pub fn is_data(&self) -> bool {
        (self.type_seq & 0xC000) == BLOCK_DATA
    }

    pub fn sequence(&self) -> u16 {
        self.type_seq & 0x3FFF
    }

    pub fn control_code(&self) -> u16 {
        self.type_seq & 0x3FFF
    }

    pub fn length(&self) -> usize {
        self.data.len()
    }
}

pub fn next_sequence(seq: u16) -> u16 {
    if seq >= 0x3FFF {
        0
    } else {
        seq + 1
    }
}

