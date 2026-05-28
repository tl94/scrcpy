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
import java.util.HashSet;

public class WebSocketConnection extends Connection {
    private static final byte[] MAGIC_BYTES_INITIAL = "scrcpy_initial".getBytes(StandardCharsets.UTF_8);
    private static final byte[] MAGIC_BYTES_MESSAGE = "scrcpy_message".getBytes(StandardCharsets.UTF_8);
    private static final byte[] DEVICE_NAME_BYTES = Device.getDeviceName().getBytes(StandardCharsets.UTF_8);
    private static final long PACKET_FLAG_CONFIG = 1L << 63;
    private final HashSet<WebSocket> sockets = new HashSet<>();
    private final ParcelFileDescriptor videoPipeRead, videoPipeWrite;
    private final ParcelFileDescriptor audioPipeRead, audioPipeWrite;
    private final ParcelFileDescriptor controlPipeReverseRead, controlPipeReverseWrite;
    private final PipedOutputStream controlOutputStream = new PipedOutputStream();
    private final PipedInputStream controlInputStream = new PipedInputStream(controlOutputStream);
    private final ControlChannel controlChannel;
    private final VideoSettings videoSettings;
    private byte[] cachedCodecHeader;
    private ByteBuffer cachedConfigPacket;

    public WebSocketConnection(Options options, VideoSettings videoSettings) throws IOException {
        this.videoSettings = videoSettings;

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
        byte[] raw = msg.writeToByteArray(MAGIC_BYTES_MESSAGE.length);
        ByteBuffer buffer = ByteBuffer.wrap(raw);
        buffer.put(MAGIC_BYTES_MESSAGE);
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

    private void startAudioBroadcastThread(ParcelFileDescriptor pfd, String name) {
//        TODO
    }

    private void startVideoBroadcastThread(ParcelFileDescriptor pfd, String name) {
        new Thread(() -> {
            try (DataInputStream dis = new DataInputStream(new FileInputStream(pfd.getFileDescriptor()))) {
                // 1. Read the Initial Codec Header (12 bytes)
                // [Codec ID (4)][Width (4)][Height (4)]
                byte[] codecHeader = new byte[12];
                dis.readFully(codecHeader);

                // Cache initial codec header
                synchronized (sockets) {
                    cachedCodecHeader = codecHeader;
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

                    if ((pts & PACKET_FLAG_CONFIG) != 0) {
                        synchronized (sockets) {
                            cachedConfigPacket = ByteBuffer.wrap(packetData);
                        }
                    }

                    broadcast(ByteBuffer.wrap(packetData));
                }
            } catch (IOException e) {
                Ln.d("Video stream closed");
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

                    // 4. Prepend MAGIC_BYTES and broadcast as ONE WebSocket frame
                    ByteBuffer buffer = ByteBuffer.allocate(MAGIC_BYTES_MESSAGE.length + messageBytes.length);
                    buffer.put(MAGIC_BYTES_MESSAGE);
                    buffer.put(messageBytes);
                    buffer.rewind();

                    broadcast(buffer);
                }
            } catch (IOException e) {
                // Pipe closed when session stopped
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
        Ln.v("a lil extra");
        synchronized (sockets) {
//             Send the most recent SPS/PPS so the browser can start immediately
            if (cachedConfigPacket != null) {
                socket.send(cachedConfigPacket.duplicate());
            }
//            if (cachedCodecHeader != null) {
//                Ln.v("Sending video codec header to new client: " + Arrays.toString(cachedCodecHeader));
//                socket.send(ByteBuffer.wrap(cachedCodecHeader));
//            }
//            if (cachedConfigPacket != null) {
//                Ln.v("Sending most recent video config packet to new client: " + Arrays.toString(cachedConfigPacket.array()));
//                socket.send(cachedConfigPacket.duplicate());
//            }
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
