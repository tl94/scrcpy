package com.genymobile.scrcpy.ws;

import android.os.ParcelFileDescriptor;

import com.genymobile.scrcpy.Options;
import com.genymobile.scrcpy.control.ControlChannel;
import com.genymobile.scrcpy.control.DeviceMessage;
import com.genymobile.scrcpy.device.Connection;
import com.genymobile.scrcpy.device.Device;
import com.genymobile.scrcpy.util.Ln;
import com.genymobile.scrcpy.video.VideoSettings;

import org.java_websocket.WebSocket;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;

public class WebSocketConnection extends Connection {
    private static final byte CHANNEL_VIDEO = 1;
    private static final byte CHANNEL_AUDIO = 2;
    private static final byte CHANNEL_CONTROL = 3;
    private static final int PIPE_SIZE = 64 * 1024;
    private static final byte[] DEVICE_NAME_BYTES = Device.getDeviceName().getBytes(StandardCharsets.UTF_8);
    private static final long PACKET_FLAG_CONFIG = 1L << 63;
    private static final long PACKET_FLAG_KEY_FRAME = 1L << 62;
    private final boolean wsAggregateStream;
    private final HashSet<WebSocket> sockets = new HashSet<>();

    private final ParcelFileDescriptor videoPipeRead, videoPipeWrite;
    private final ParcelFileDescriptor audioPipeRead, audioPipeWrite;
    private final ParcelFileDescriptor controlPipeReverseRead, controlPipeReverseWrite;
    private final PipedOutputStream controlOutputStream = new PipedOutputStream();
    private final PipedInputStream controlInputStream = new PipedInputStream(controlOutputStream, PIPE_SIZE);
    private final ControlChannel controlChannel;

    private final VideoSettings videoSettings;

    // stream cache
    private byte[] cachedVideoCodecHeader;
    private ByteBuffer cachedVideoConfigPacket;
    private ByteBuffer cachedVideoKeyFrame;
    private ByteBuffer cachedAudioCodecHeader;
    private ByteBuffer cachedAudioStreamPacket;


    public WebSocketConnection(Options options, VideoSettings videoSettings) throws IOException {
        this.videoSettings = videoSettings;

        this.wsAggregateStream = options.getWsAggregateStream();

        if (options.getVideo()) {
            ParcelFileDescriptor[] videoPipe = ParcelFileDescriptor.createPipe();
            videoPipeRead = videoPipe[0];
            videoPipeWrite = videoPipe[1];
        } else {
            videoPipeRead = null;
            videoPipeWrite = null;
        }

        if (options.getAudio()) {
            ParcelFileDescriptor[] audioPipe = ParcelFileDescriptor.createPipe();
            audioPipeRead = audioPipe[0];
            audioPipeWrite = audioPipe[1];
        } else {
            audioPipeRead = null;
            audioPipeWrite = null;
        }

        if (options.getControl()) {
            ParcelFileDescriptor[] controlPipeReverse = ParcelFileDescriptor.createPipe();
            controlPipeReverseRead = controlPipeReverse[0];
            controlPipeReverseWrite = controlPipeReverse[1];

            controlChannel = new ControlChannel(controlInputStream, new FileOutputStream(controlPipeReverseWrite.getFileDescriptor()));
        } else {
            controlPipeReverseRead = null;
            controlPipeReverseWrite = null;
            controlChannel = null;
        }

        startBroadcastThreads();
    }

    public static ByteBuffer deviceMessageToByteBuffer(DeviceMessage msg) {
        byte[] raw = msg.writeToByteArray(1);
        ByteBuffer buffer = ByteBuffer.wrap(raw);
        buffer.put(CHANNEL_CONTROL);
        buffer.rewind();
        return buffer;
    }

    private void startBroadcastThreads() {
        if (videoPipeRead != null) {
            startVideoBroadcastThread(videoPipeRead, "ws-video");
        }
        if (audioPipeRead != null) {
            startAudioBroadcastThread(audioPipeRead, "ws-audio");
        }
        if (controlPipeReverseRead != null) {
            startControlBroadcastThread(controlPipeReverseRead, "ws-control");
        }
    }

    private void startVideoBroadcastThread(ParcelFileDescriptor pfd, String name) {
        new Thread(() -> {
            try (DataInputStream dis = new DataInputStream(new FileInputStream(pfd.getFileDescriptor()))) {
                // 1. Read the Initial Codec Header (12 bytes)
                // [Codec ID (4)][Width (4)][Height (4)]
                byte[] codecHeader = new byte[12];
                dis.readFully(codecHeader);

                // cache video codec header
                synchronized (sockets) {
                    cachedVideoCodecHeader = codecHeader;
                }

                while (!Thread.currentThread().isInterrupted()) {
                    // 2. Read the Frame Metadata (12 bytes)
                    // [PTS (8)][Size (4)]
                    long pts = dis.readLong();
                    int size = dis.readInt();

                    // SAFETY CHECK: If size is > 10MB, we've definitely lost sync.
                    if (size < 0 || size > 10 * 1024 * 1024) {
                        throw new IOException("Invalid video packet size: " + size + ". Stream desynchronized!");
                    }

                    // 3. Read the exact number of bytes for the video packet
                    byte[] packetData = new byte[size];
                    dis.readFully(packetData);

                    ByteBuffer buffer;
                    // 4. Add channel type byte
                    if (wsAggregateStream) {
                        buffer = ByteBuffer.allocate(1 + 8 + size);
                        buffer.put(CHANNEL_VIDEO);
                        buffer.putLong(pts);
                        buffer.put(packetData);
                        buffer.rewind();
                    } else {
                        buffer = ByteBuffer.allocate(1 + size);
                        buffer.put(CHANNEL_VIDEO);
                        buffer.put(packetData);
                        buffer.rewind();
                    }

                    // cache video config packet
                    if ((pts & PACKET_FLAG_CONFIG) != 0) {
                        synchronized (sockets) {
                            Ln.d("updated cached video config");
                            cachedVideoConfigPacket = buffer;
                        }
                    }
//                    else if ((pts & PACKET_FLAG_KEY_FRAME) != 0) {
//                        synchronized (sockets) {
//                            Ln.d("updated cached key frame");
//                            cachedVideoKeyFrame = buffer;
//                        }
//                    }

                    // broadcast video packet
                    broadcast(buffer);
                }
            } catch (IOException e) {
                Ln.d("Video stream closed");
                Ln.e(e.getMessage());
            }
        }, name).start();
    }

    private void startAudioBroadcastThread(ParcelFileDescriptor pfd, String name) {
        new Thread(() -> {
            try (DataInputStream dis = new DataInputStream(new FileInputStream(pfd.getFileDescriptor()))) {
                // 1. Read initial codec header (4 bytes for audio)
                // [Codec ID (4)]
                byte[] codecHeader = new byte[4];
                dis.readFully(codecHeader);
                Ln.d("codecHeader: " + Arrays.toString(codecHeader));

                ByteBuffer headerBuffer = ByteBuffer.allocate(1 + 4);
                headerBuffer.put(CHANNEL_AUDIO);
                headerBuffer.put(codecHeader);
                headerBuffer.rewind();

                // cache audio header
                synchronized (sockets) {
                    cachedAudioCodecHeader = headerBuffer;
                }

                // broadcast audio header
                broadcast(headerBuffer);

                while (!Thread.currentThread().isInterrupted()) {
                    // 2. Read frame metadata (12 bytes)
                    long pts = dis.readLong();
                    int size = dis.readInt();

                    // 3. Read packet
                    byte[] packetData = new byte[size];
                    dis.readFully(packetData);

                    // 4. Wrap with tag and broadcast
                    ByteBuffer buffer = ByteBuffer.allocate(1 + 8 + 4 + size);
                    buffer.put(CHANNEL_AUDIO);
                    buffer.putLong(pts);
                    buffer.putInt(size);
                    buffer.put(packetData);
                    buffer.rewind();

                    // cache last audio stream packet
                    synchronized (sockets) {
                        cachedAudioStreamPacket = buffer;
                    }

                    // broadcast audio packet
                    broadcast(buffer);
                }

            } catch (IOException e) {
                Ln.d("Audio stream closed");
                Ln.e(e.getMessage());
            }
        }, name).start();
    }

    private void startControlBroadcastThread(ParcelFileDescriptor pfd, String name) {
        new Thread(() -> {
            // Use DataInputStream to read typed data from the pipe
            try (DataInputStream dis = new DataInputStream(new FileInputStream(pfd.getFileDescriptor()))) {
                while (!Thread.currentThread().isInterrupted()) {
                    // 1. Read the 'type' (the first byte the Controller always writes)
                    int type = dis.readUnsignedByte();

                    Ln.d("Broadcasting control message: " + type);

                    // 2. Based on the type, we know how many more bytes to read to complete the message
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    DataOutputStream dos = new DataOutputStream(bos);
                    dos.writeByte(type);

                    if (type == DeviceMessage.TYPE_CLIPBOARD) {
                        int len = dis.readInt();
                        byte[] text = new byte[len];
                        dis.readFully(text);
                        dos.writeInt(len);
                        dos.write(text);
                    } else if (type == DeviceMessage.TYPE_ACK_CLIPBOARD) {
                        dos.writeLong(dis.readLong()); // The sequence number
                    } else if (type == DeviceMessage.TYPE_UHID_OUTPUT) {
                        dos.writeShort(dis.readShort()); // The ID
                        int len = dis.readUnsignedShort();
                        byte[] data = new byte[len];
                        dis.readFully(data);
                        dos.writeShort(len);
                        dos.write(data);
                    } else if (type == DeviceMessage.TYPE_PUSH_RESPONSE) {
                        dos.writeShort(dis.readShort()); // The ID
                        dos.writeByte(dis.readByte());   // The result
                    }

                    // 3. Now we have a single, complete DeviceMessage in 'bos'
                    byte[] messageBytes = bos.toByteArray();

                    // 4. Add channel type byte and broadcast as one WebSocket frame
                    ByteBuffer buffer = ByteBuffer.allocate(1 + messageBytes.length);
                    buffer.put(CHANNEL_CONTROL);
                    buffer.put(messageBytes);
                    buffer.rewind();

                    broadcast(buffer);
                }
            } catch (IOException e) {
                Ln.d("Control stream closed");
                Ln.e(e.getMessage());
            }
        }, name).start();
    }

    private void broadcast(ByteBuffer data) {
        synchronized (sockets) {
            for (WebSocket socket : sockets) {
                if (socket.isOpen()) {
                    socket.send(data.duplicate());
                }
            }
        }
    }

    public void addSocket(WebSocket socket) {
        Ln.d("Adding new socket");
        synchronized (sockets) {
            // Send the most recent SPS/PPS so the browser can start immediately
            if (cachedVideoConfigPacket != null) {
                socket.send(cachedVideoConfigPacket.duplicate());
            }
            if (cachedVideoKeyFrame != null) {
                socket.send(cachedVideoKeyFrame.duplicate());
            }
            if (cachedAudioCodecHeader != null) {
                socket.send(cachedAudioCodecHeader.duplicate());
            }
            if (cachedAudioStreamPacket != null) {
                socket.send(cachedAudioStreamPacket.duplicate());
            }
            sockets.add(socket);
        }
    }

    public void removeSocket(WebSocket socket) {
        synchronized (sockets) {
            sockets.remove(socket);
        }
    }

    public int getSocketsAmount() {
        return sockets.size();
    }

    public void handleIncomingControlMessage(ByteBuffer data) throws IOException {
        byte[] bytes = new byte[data.remaining()];
        data.get(bytes);
        controlOutputStream.write(bytes);
        controlOutputStream.flush();
    }

    @Override
    public void sendDeviceMeta(String deviceName) throws IOException {
        // Implementation for sending initial device name if required by protocol
    }

    @Override
    public FileDescriptor getVideoFd() {
        return videoPipeWrite.getFileDescriptor();
    }

    @Override
    public FileDescriptor getAudioFd() {
        return audioPipeWrite.getFileDescriptor();
    }

    @Override
    public ControlChannel getControlChannel() {
        return controlChannel;
    }

    void send(ByteBuffer data) {
        if (sockets.isEmpty()) {
            return;
        }
        synchronized (sockets) {
            for (WebSocket webSocket : sockets) {
                WSServer.SocketInfo info = webSocket.getAttachment();
                if (!webSocket.isOpen() || info == null) {
                    continue;
                }
                webSocket.send(data);
            }
        }
    }

    public void sendDeviceMessage(DeviceMessage msg) {
        ByteBuffer buffer = deviceMessageToByteBuffer(msg);
        send(buffer);
    }

    public boolean hasConnections() {
        synchronized (sockets) {
            return !sockets.isEmpty();
        }
    }

    @Override
    public void close() throws IOException {
        videoPipeWrite.close();
        audioPipeWrite.close();
        controlOutputStream.close();
        controlPipeReverseWrite.close();
        videoPipeRead.close();
        audioPipeRead.close();
        controlPipeReverseRead.close();
    }

    public VideoSettings getVideoSettings() {
        return videoSettings;
    }

    public boolean setVideoSettings(VideoSettings videoSettings) {
        return false;
    }

    private void release() {
        WSServer.releaseConnectionForDisplay(this.videoSettings.getDisplayId());
        // encoder will stop itself after checking .hasConnections()
    }
}
