package org.example.Transport;

/** Fixed framing shared by the client and the server ingress transport. */
public final class KvWireProtocol {

    public static final byte VERSION = 1;
    public static final byte REQUEST = 1;
    public static final byte RESPONSE = 2;
    public static final byte REJECTED = 3;
    public static final byte DEADLINE_EXCEEDED = 4;

    public static final int LENGTH_FIELD_BYTES = Integer.BYTES;
    public static final int REQUEST_HEADER_BYTES = 2 + (3 * Long.BYTES);
    public static final int TERMINAL_HEADER_BYTES = 2 + (2 * Long.BYTES);
    public static final int MAX_FRAME_BYTES = 1 << 20;

    private KvWireProtocol() {
    }
}
