package com.genymobile.scrcpy.ws;

import android.graphics.Rect;
import android.media.MediaCodecInfo;
import android.media.MediaCodecList;
import android.media.MediaFormat;

import com.genymobile.scrcpy.Options;
import com.genymobile.scrcpy.control.ControlMessage;
import com.genymobile.scrcpy.control.ControlMessageReader;
import com.genymobile.scrcpy.device.Device;
import com.genymobile.scrcpy.device.DisplayInfo;
import com.genymobile.scrcpy.device.Size;
import com.genymobile.scrcpy.util.CodecUtils;
import com.genymobile.scrcpy.util.Ln;
import com.genymobile.scrcpy.video.VideoSettings;
import com.genymobile.scrcpy.wrappers.ServiceManager;

import org.java_websocket.WebSocket;
import org.java_websocket.framing.CloseFrame;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class WSServer extends WebSocketServer {
    public static final int DEVICE_NAME_FIELD_LENGTH = 64;
    private static final String PID_FILE_PATH = "/data/local/tmp/ws_scrcpy.pid";
    private static final HashMap<Integer, WsScrcpySession> SESSION_BY_DISPLAY_ID = new HashMap<>();
    private static final byte CHANNEL_SPECIAL_INITIAL = 0;
    private static final byte[] DEVICE_NAME_BYTES = Device.getDeviceName().getBytes(StandardCharsets.UTF_8);
    private final Options options;

    public WSServer(Options options) {
        super(new InetSocketAddress(options.getListenOnAllInterfaces() ? "0.0.0.0" : "127.0.0.1", options.getPort()));
        this.options = options;
        unlinkPidFile();
    }

    private static void unlinkPidFile() {
        try {
            File pidFile = new File(PID_FILE_PATH);
            if (pidFile.exists()) {
                if (!pidFile.delete()) {
                    Ln.e("Failed to delete PID file");
                }
            }
        } catch (Exception e) {
            Ln.e("Failed to delete PID file:", e);
        }
    }

    private static void writePidFile() {
        File file = new File(PID_FILE_PATH);
        FileOutputStream stream;
        try {
            stream = new FileOutputStream(file, false);
            stream.write(Integer.toString(android.os.Process.myPid()).getBytes(StandardCharsets.UTF_8));
            stream.close();
        } catch (IOException e) {
            Ln.e(e.getMessage());
        }
    }

    public static WebSocketConnection getConnectionForDisplay(int displayId) {
        WsScrcpySession session = SESSION_BY_DISPLAY_ID.get(displayId);
        return session != null ? session.getConnection() : null;
    }

    public static void releaseConnectionForDisplay(int displayId) {
        SESSION_BY_DISPLAY_ID.remove(displayId);
    }

    public static void sendInitialInfo(ByteBuffer initialInfo, WebSocket webSocket, int clientId) {
        initialInfo.position(initialInfo.capacity() - 4);
        initialInfo.putInt(clientId);
        initialInfo.rewind();
        webSocket.send(initialInfo);
    }

    @Override
    public void onOpen(WebSocket webSocket, ClientHandshake handshake) {
        if (webSocket.isOpen()) {
            short clientId = SocketInfo.getNextClientId();
            if (clientId == -1) {
                webSocket.close(CloseFrame.TRY_AGAIN_LATER);
                return;
            }
            SocketInfo socketInfo = new SocketInfo(clientId);
            webSocket.setAttachment(socketInfo);
            sendInitialInfo(getInitialInfo(), webSocket, clientId);
            Ln.d("Client entered the room!");
        }
    }

    @Override
    public void onClose(WebSocket webSocket, int code, String reason, boolean remote) {
        FilePushHandler.cancelAllForConnection(webSocket);
        SocketInfo socketInfo = webSocket.getAttachment();
        if (socketInfo != null) {
            WsScrcpySession session = socketInfo.getSession();
            WebSocketConnection connection = session.getConnection();
            if (connection != null) {
                session.leave(webSocket);
            }
            socketInfo.release();
            Ln.d("Client has left the room!");
        }
    }

    @Override
    public void onMessage(WebSocket webSocket, String message) {
        String address = webSocket.getRemoteSocketAddress().getAddress().getHostAddress();
        Ln.w("?  Client from " + address + " says: \"" + message + "\"");
    }

    @Override
    public void onMessage(WebSocket webSocket, ByteBuffer message) {
        if (!message.hasRemaining()) return;

        // 1. Peek at the type (first byte)
        int type = message.get() & 0xFF;
        message.rewind();

        // 2. Handle server-level messages
        if (type == ControlMessage.TYPE_CHANGE_STREAM_PARAMETERS || type == ControlMessage.TYPE_PUSH_FILE) {
            ControlMessage msg = ControlMessageReader.parse(message);
            if (msg == null) return;

            if (type == ControlMessage.TYPE_CHANGE_STREAM_PARAMETERS) {
                handleJoinRequest(webSocket, msg);
            } else {
                FilePushHandler.handlePush(webSocket, msg);
            }
            return;
        }

        // 3. Handle device-level messages (touch, key, etc.)
        SocketInfo socketInfo = webSocket.getAttachment();
        if (socketInfo == null) {
            Ln.e("No info attached to connection");
            return;
        }

        WsScrcpySession session = socketInfo.getSession();
        if (session != null) {
            try {
                WebSocketConnection connection = session.getConnection();
                connection.handleIncomingControlMessage(message);
            } catch (IOException e) {
                Ln.e("Failed to pipe control message", e);
            }
        }
    }

    @Override
    public void onError(WebSocket webSocket, Exception ex) {
        Ln.e("WebSocket error", ex);
        if (webSocket != null) {
            // some errors like port binding failed may not be assignable to a specific websocket
            FilePushHandler.cancelAllForConnection(webSocket);
        }
        if (ex instanceof BindException) {
            System.exit(1);
        }
    }

    @Override
    public void onStart() {
        Ln.d("Server started! " + this.getAddress().toString());
        this.setConnectionLostTimeout(0);
        this.setConnectionLostTimeout(100);
        writePidFile();
    }

    private void handleJoinRequest(WebSocket webSocket, ControlMessage msg) {
        VideoSettings videoSettings = msg.getVideoSettings();
        int displayId = videoSettings.getDisplayId();

        SocketInfo socketInfo = webSocket.getAttachment();
        if (socketInfo == null) return;

        WsScrcpySession session = socketInfo.getSession();
        if (session != null) {
            WebSocketConnection connection = session.getConnection();
            if (connection.getVideoSettings().getDisplayId() == displayId) {
                return;
            }

            session.leave(webSocket);
        }

        try {
            joinStreamForDisplayId(webSocket, videoSettings, options, displayId, this);
        } catch (IOException e) {
            Ln.e("Failed to join stream", e);
            webSocket.close(CloseFrame.PROTOCOL_ERROR);
        }
    }

    private void joinStreamForDisplayId(
            WebSocket webSocket, VideoSettings videoSettings, Options options, int displayId, WSServer wsServer) throws IOException {
        SocketInfo socketInfo = webSocket.getAttachment();
        WsScrcpySession session = SESSION_BY_DISPLAY_ID.get(displayId);

        if (session == null) {
            WebSocketConnection connection = new WebSocketConnection(options, videoSettings);
            session = new WsScrcpySession(options, connection);
            WsScrcpySession finalSession = session;
            session.setListener(new SessionListener() {
                                    @Override
                                    public void onSessionJoined(int displayId) {
                                        SESSION_BY_DISPLAY_ID.put(displayId, finalSession);
                                        sendInitialInfoToAll();
                                    }

                                    @Override
                                    public void onSessionClosed(int displayId) {
                                        releaseConnectionForDisplay(displayId);
                                        sendInitialInfoToAll();
                                    }

                                    @Override
                                    public void onConnectionChanged() {
                                        sendInitialInfoToAll();
                                    }
                                }
            );

            SESSION_BY_DISPLAY_ID.put(displayId, session);
        }

        socketInfo.setSession(session);
        session.join(webSocket, videoSettings);
    }

    public byte[] getDeviceScreenInfoBytes(int displayId) {
        ByteBuffer buffer = ByteBuffer.allocate(6 * 4 + 1);

        WsScrcpySession session = SESSION_BY_DISPLAY_ID.get(displayId);
        DisplayInfo displayInfo = ServiceManager.getDisplayManager().getDisplayInfo(displayId);

        Size size = displayInfo.getSize();
        Rect contentRect = size.toRect();

        assert session != null;
        Size videoSize = session.getVideoSize();

        if (videoSize == null) {
            videoSize = size;
        }

        int rotation = displayInfo.getRotation();

        buffer.putInt(contentRect.left);
        buffer.putInt(contentRect.top);
        buffer.putInt(contentRect.right);
        buffer.putInt(contentRect.bottom);

        buffer.putInt(videoSize.getWidth());
        buffer.putInt(videoSize.getHeight());

        buffer.put((byte) rotation);

        return buffer.array();
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    public ByteBuffer getInitialInfo() {
        int baseLength = 1 // CHANNEL_SPECIAL_INITIAL tag
                + DEVICE_NAME_FIELD_LENGTH
                + 4                          // displays count
                + 4;                         // client id
        int additionalLength = 0;
        int[] displayIds = ServiceManager.getDisplayManager().getDisplayIds();
        HashMap<Integer, DisplayInfo> displayInfoHashMap = new HashMap<>();
        HashMap<Integer, Integer> connectionsCount = new HashMap<>();
        HashMap<Integer, byte[]> displayInfoMap = new HashMap<>();
        HashMap<Integer, byte[]> videoSettingsBytesMap = new HashMap<>();
        HashMap<Integer, byte[]> screenInfoBytesMap = new HashMap<>();

        for (int displayId : displayIds) {
            DisplayInfo displayInfo = ServiceManager.getDisplayManager().getDisplayInfo(displayId);
            displayInfoHashMap.put(displayId, displayInfo);
            byte[] displayInfoBytes = displayInfo.toWsByteArray();
            additionalLength += displayInfoBytes.length;
            displayInfoMap.put(displayId, displayInfoBytes);
            WebSocketConnection connection = WSServer.getConnectionForDisplay(displayId);
            additionalLength += 4; // for connection.connections.size()
            additionalLength += 4; // for screenInfoBytes.length
            additionalLength += 4; // for videoSettingsBytes.length
            if (connection != null) {
                connectionsCount.put(displayId, connection.getSocketsAmount());
                byte[] screenInfoBytes = getDeviceScreenInfoBytes(displayId);
                additionalLength += screenInfoBytes.length;
                screenInfoBytesMap.put(displayId, screenInfoBytes);
                byte[] videoSettingsBytes = connection.getVideoSettings().toByteArray();
                additionalLength += videoSettingsBytes.length;
                videoSettingsBytesMap.put(displayId, videoSettingsBytes);
            }
        }

//      TODO:  hardcoded encoders as before for now
        MediaCodecInfo[] encoders = CodecUtils.getEncoders(new MediaCodecList(MediaCodecList.REGULAR_CODECS), MediaFormat.MIMETYPE_VIDEO_AVC);
        List<byte[]> encodersNames = new ArrayList<>();
        if (encoders != null && encoders.length > 0) {
            additionalLength += 4;
            for (MediaCodecInfo encoder : encoders) {
                byte[] nameBytes = encoder.getName().getBytes(StandardCharsets.UTF_8);
                additionalLength += 4 + nameBytes.length;
                encodersNames.add(nameBytes);
            }
        }

        byte[] fullBytes = new byte[baseLength + additionalLength];
        ByteBuffer initialInfo = ByteBuffer.wrap(fullBytes);
        initialInfo.put(CHANNEL_SPECIAL_INITIAL);
        initialInfo.put(DEVICE_NAME_BYTES, 0, Math.min(DEVICE_NAME_FIELD_LENGTH - 1, DEVICE_NAME_BYTES.length));
        initialInfo.position(1 + DEVICE_NAME_FIELD_LENGTH);
        initialInfo.putInt(displayIds.length);
        for (DisplayInfo displayInfo : displayInfoHashMap.values()) {
            int displayId = displayInfo.getDisplayId();
            if (displayInfoMap.containsKey(displayId)) {
                initialInfo.put(displayInfoMap.get(displayId));
            }
            int count = 0;
            if (connectionsCount.containsKey(displayId)) {
                count = connectionsCount.get(displayId);
            }
            initialInfo.putInt(count);
            if (screenInfoBytesMap.containsKey(displayId)) {
                byte[] screenInfo = screenInfoBytesMap.get(displayId);
                initialInfo.putInt(screenInfo.length);
                initialInfo.put(screenInfo);
            } else {
                initialInfo.putInt(0);
            }
            if (videoSettingsBytesMap.containsKey(displayId)) {
                byte[] videoSettings = videoSettingsBytesMap.get(displayId);
                initialInfo.putInt(videoSettings.length);
                initialInfo.put(videoSettings);
            } else {
                initialInfo.putInt(0);
            }
        }
        initialInfo.putInt(encodersNames.size());
        for (byte[] encoderNameBytes : encodersNames) {
            initialInfo.putInt(encoderNameBytes.length);
            initialInfo.put(encoderNameBytes);
        }

        return initialInfo;
    }

    public void sendInitialInfoToAll() {
        List<WebSocket> webSockets = new ArrayList<>(this.getConnections());
        if (webSockets.isEmpty()) {
            return;
        }
        ByteBuffer initialInfo = getInitialInfo();
        for (WebSocket webSocket : webSockets) {
            SocketInfo socketInfo = webSocket.getAttachment();
            if (socketInfo == null) {
                continue;
            }
            sendInitialInfo(initialInfo, webSocket, socketInfo.getId());
        }
    }

    public static final class SocketInfo {
        private static final HashSet<Short> INSTANCES_BY_ID = new HashSet<>();
        private final short id;
        private WsScrcpySession session;

        SocketInfo(short id) {
            this.id = id;
            INSTANCES_BY_ID.add(id);
        }

        public static short getNextClientId() {
            short nextClientId = 0;
            while (INSTANCES_BY_ID.contains(++nextClientId)) {
                if (nextClientId == Short.MAX_VALUE) {
                    return -1;
                }
            }
            return nextClientId;
        }

        public short getId() {
            return id;
        }

        public WsScrcpySession getSession() {
            return this.session;
        }

        public void setSession(WsScrcpySession wsScrcpySession) {
            this.session = wsScrcpySession;
        }

        public void release() {
            INSTANCES_BY_ID.remove(id);
        }
    }
}