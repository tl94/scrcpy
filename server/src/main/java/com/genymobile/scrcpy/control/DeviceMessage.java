package com.genymobile.scrcpy.control;

import com.genymobile.scrcpy.util.StringUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

// TODO
public abstract class DeviceMessage {

    public static final int TYPE_CLIPBOARD = 0;
    public static final int TYPE_ACK_CLIPBOARD = 1;
    public static final int TYPE_UHID_OUTPUT = 2;
    public static final int MAX_EVENT_SIZE = 4096;
    public static final int TYPE_PUSH_RESPONSE = 101;
    private static final int MESSAGE_MAX_SIZE = 1 << 18; // 256k
    private int type;
    private String text;
    private long sequence;
    private int id;
    private byte[] data;

    private DeviceMessage(int type) {
        this.type = type;
    }

    public static DeviceMessage createClipboard(String text) {
        DeviceMessage msg = new ClipboardMessage(text);
        msg.text = text;
        return msg;
    }

    public static DeviceMessage createAckClipboard(long sequence) {
        DeviceMessage msg = new AckClipboardMessage(sequence);
        msg.sequence = sequence;
        return msg;
    }

    public static DeviceMessage createUhidOutput(int id, byte[] data) {
        DeviceMessage msg = new UhidOutputMessage(id, data);
        msg.id = id;
        msg.data = data;
        return msg;
    }

    public static DeviceMessage createPushResponse(short id, int result) {
        return new FilePushResponseMessage(id, result);
    }

    public int getType() {
        return type;
    }

    public String getText() {
        return text;
    }

    public long getSequence() {
        return sequence;
    }

    public int getId() {
        return id;
    }

    public byte[] getData() {
        return data;
    }

    public void writeToByteArray(byte[] array) {
        writeToByteArray(array, 0);
    }

    public byte[] writeToByteArray(int offset) {
        byte[] temp = new byte[offset + this.getLen()];
        writeToByteArray(temp, offset);
        return temp;
    }

    public abstract void writeToByteArray(byte[] array, int offset);

    public abstract int getLen();

    private static final class ClipboardMessage extends DeviceMessage {
        public static final int CLIPBOARD_TEXT_MAX_LENGTH = MESSAGE_MAX_SIZE - 5; // type: 1 byte; length: 4 bytes
        private byte[] raw;
        private int len;

        private ClipboardMessage(String text) {
            super(TYPE_CLIPBOARD);
            this.raw = text.getBytes(StandardCharsets.UTF_8);
            this.len = StringUtils.getUtf8TruncationIndex(raw, CLIPBOARD_TEXT_MAX_LENGTH);
        }

        public void writeToByteArray(byte[] array, int offset) {
            ByteBuffer buffer = ByteBuffer.wrap(array, offset, array.length - offset);
            buffer.put((byte) this.getType());
            buffer.putInt(len);
            buffer.put(raw, 0, len);
        }

        public int getLen() {
            return 1 + 4 + len;
        }
    }

    private static final class FilePushResponseMessage extends DeviceMessage {
        private short id;
        private int result;

        private FilePushResponseMessage(short id, int result) {
            super(TYPE_PUSH_RESPONSE);
            this.id = id;
            this.result = result;
        }

        @Override
        public void writeToByteArray(byte[] array, int offset) {
            ByteBuffer buffer = ByteBuffer.wrap(array, offset, array.length - offset);
            buffer.put((byte) getType());
            buffer.putShort(id);
            buffer.put((byte) result);
        }

        @Override
        public int getLen() {
            return 1 + 2 + 1;
        }
    }

    private static final class AckClipboardMessage extends DeviceMessage {
        private final long sequence;

        private AckClipboardMessage(long sequence) {
            super(TYPE_ACK_CLIPBOARD);
            this.sequence = sequence;
        }

        @Override
        public void writeToByteArray(byte[] array, int offset) {
            ByteBuffer buffer = ByteBuffer.wrap(array, offset, array.length - offset);
            buffer.put((byte) getType());
            buffer.putLong(sequence);
        }

        @Override
        public int getLen() {
            return 1 + 8;
        }
    }

    private static final class UhidOutputMessage extends DeviceMessage {
        private final int id;
        private final byte[] data;

        private UhidOutputMessage(int id, byte[] data) {
            super(TYPE_UHID_OUTPUT);
            this.id = id;
            this.data = data;
        }

        @Override
        public void writeToByteArray(byte[] array, int offset) {
            ByteBuffer buffer = ByteBuffer.wrap(array, offset, array.length - offset);
            buffer.put((byte) getType());
            buffer.putInt(id);
            buffer.putInt(data.length);
            buffer.put(data);
        }

        @Override
        public int getLen() {
            return 1 + 4 + 4 + data.length;
        }
    }
}
