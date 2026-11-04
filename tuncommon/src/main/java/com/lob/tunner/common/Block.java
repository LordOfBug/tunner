package com.lob.tunner.common;

import java.nio.ByteBuffer;

/**
 * Block is a piece of data with 8 bytes header
 */
public class Block {
    /**
     * highest two bits of first byte, Type of block, could be
     *   01xx xxxx xxxx xxxx  - data
     *   10xx xxxx xxxx xxxx - control
     */
    public final static short BLOCK_DATA = (short)0x4000;
    public final static short BLOCK_CONTROL = (short)0x8000;

    public final static short CODE_ECHO = 0; // with random 8*x bytes payload (echo back)
    public final static short CODE_START = 1; // start a connection
    public final static short CODE_RESUME = 2;
    public final static short CODE_NOISE = 3; // noise/dummy data, discard without echo
    public final static short CODE_ABORT = (short)0x00FF;

    /**
     * If _type is CONTROl, the following 14 bits will be control code
     * If _type is DATA, the following 14 bits will be the sequence #
     */
    private final short _typeSeq;

    /**
     * Length, take 2 bytes, we use int here for unsigned integer conversion!!!
     */
    public final static int MAX_LENGTH = 0xFFFF;
    public final static int MAX_NO_PADDING_LENGTH = 0xFFF8;
    private final int _length;

    /**
     * Connection ID, byte 5 - 8
     */
    private final int _conn;

    private final ByteBuffer _data;

    /**
     * Create a control block for a connection
     *
     * @param conn - connection ID
     * @param seqType - control code
     */
    public Block(int conn, short seqType) {
        this._conn = conn;
        this._typeSeq = seqType;
        this._length = 0;
        this._data = null;
    }

    public Block(int conn, short seqType, int len, ByteBuffer data) {
        this._conn = conn;
        this._typeSeq = seqType;
        this._length = len;
        this._data = data;
    }

    public Block(Block b) {
        this._conn = b._conn;
        this._typeSeq = b._typeSeq;
        this._length = b._length;
        this._data = ByteBuffer.allocate(b._data.limit());
        this._data.put(b._data);
        this._data.rewind();
    }

    public final short getTypeSeq() {
        return _typeSeq;
    }

    public final int connection() {
        return _conn;
    }

    public final int length() {
        return _length;
    }

    public final ByteBuffer data() {
        return _data;
    }

    /**
     * Highest two bits of _typeSeq
     * @return
     */
    public final short type() {
        return (short)(_typeSeq & 0xC000);
    }

    /**
     * Lowest 14 bits if data block
     * @return
     */
    public final short sequence() {
        return (short)(_typeSeq & 0x3FFF);
    }

    /**
     * Lowest 14 bits if control block, currently we only use lowest 8 bits
     * @return
     */
    public final short control() {
        return (short)(_typeSeq & 0xFF);
    }

    public boolean read(ByteBuffer data) {
        if(_length == 0) {
            return true;
        }

        while(_data.hasRemaining() && data.hasRemaining()) {
            _data.put(data.get());
        }

        // ready for processing!!!
        _data.rewind();

        return true;
    }
}
