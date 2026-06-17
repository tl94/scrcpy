package com.genymobile.scrcpy.ws;

import com.genymobile.scrcpy.AsyncProcessor;
import com.genymobile.scrcpy.Options;
import com.genymobile.scrcpy.audio.AudioCapture;
import com.genymobile.scrcpy.audio.AudioDirectCapture;
import com.genymobile.scrcpy.audio.AudioEncoder;
import com.genymobile.scrcpy.audio.AudioPlaybackCapture;
import com.genymobile.scrcpy.audio.AudioSource;
import com.genymobile.scrcpy.control.Controller;
import com.genymobile.scrcpy.device.Device;
import com.genymobile.scrcpy.device.NewDisplay;
import com.genymobile.scrcpy.device.Size;
import com.genymobile.scrcpy.device.Streamer;
import com.genymobile.scrcpy.util.Ln;
import com.genymobile.scrcpy.video.CameraCapture;
import com.genymobile.scrcpy.video.NewDisplayCapture;
import com.genymobile.scrcpy.video.ScreenCapture;
import com.genymobile.scrcpy.video.SurfaceCapture;
import com.genymobile.scrcpy.video.SurfaceEncoder;
import com.genymobile.scrcpy.video.VideoSettings;
import com.genymobile.scrcpy.video.VideoSource;

import org.java_websocket.WebSocket;

import java.util.ArrayList;
import java.util.List;

public class WsScrcpySession {
    private final WebSocketConnection connection;
    private final SurfaceCapture surfaceCapture;
    private final SurfaceEncoder videoEncoder;
    private final AudioEncoder audioEncoder;
    private final Controller controller;
    private final List<AsyncProcessor> asyncProcessors = new ArrayList<>();

    private final int displayId;

    private SessionListener listener;

    public WsScrcpySession(Options options, WebSocketConnection connection) {
        this.connection = connection;

        // controller
        this.controller = new Controller(connection.getControlChannel(), null, options);

        // video
        if (options.getVideo()) {
            Streamer videoStreamer = new Streamer(connection.getVideoFd(), options.getVideoCodec(), options.getSendCodecMeta(), options.getSendFrameMeta());
            SurfaceCapture surfaceCapture = getSurfaceCapture(options);
            this.surfaceCapture = surfaceCapture;
            this.videoEncoder = new SurfaceEncoder(surfaceCapture, videoStreamer, options);
            this.controller.setSurfaceCapture(this.surfaceCapture);
        } else {
            this.surfaceCapture = null;
            this.videoEncoder = null;
        }

        // audio
        if (options.getAudio()) {
            Streamer audioStreamer = new Streamer(connection.getAudioFd(), options.getAudioCodec(), options.getSendCodecMeta(), options.getSendFrameMeta());
            AudioSource audioSource = options.getAudioSource();
            AudioCapture audioCapture;
            if (audioSource.isDirect()) {
                audioCapture = new AudioDirectCapture(audioSource);
            } else {
                audioCapture = new AudioPlaybackCapture(options.getAudioDup());
            }
            this.audioEncoder = new AudioEncoder(audioCapture, audioStreamer, options);
        } else {
            this.audioEncoder = null;
        }

        asyncProcessors.add(videoEncoder);
        asyncProcessors.add(audioEncoder);
        asyncProcessors.add(controller);

        this.displayId = options.getDisplayId();
    }

    private SurfaceCapture getSurfaceCapture(Options options) {
        SurfaceCapture surfaceCapture;
        if (options.getVideoSource() == VideoSource.DISPLAY) {
            NewDisplay newDisplay = options.getNewDisplay();
            if (newDisplay != null) {
                surfaceCapture = new NewDisplayCapture(controller, options);
            } else {
                assert options.getDisplayId() != Device.DISPLAY_ID_NONE;
                surfaceCapture = new ScreenCapture(controller, options);
            }
        } else {
            surfaceCapture = new CameraCapture(options);
        }
        return surfaceCapture;
    }

    public void join(WebSocket webSocket, VideoSettings videoSettings) {
        boolean isFirstClient = !connection.hasConnections();

        connection.addSocket(webSocket);

        if (isFirstClient) {
            this.start();
        } else {
            Ln.d("requesting new i-frame for new client");

            if (surfaceCapture != null) {
                videoEncoder.requestSyncFrame();
                surfaceCapture.requestInvalidate();
            }
        }

        if (listener != null) {
            listener.onSessionJoined(displayId);
        }

    }

    public void leave(WebSocket webSocket) {
        connection.removeSocket(webSocket);
        if (!connection.hasConnections()) {
            Ln.d("Last client has left");
            this.stop();
            if (this.listener != null) {
                this.listener.onSessionClosed(displayId);
            }
        }
    }

    public void start() {
        if (videoEncoder != null) {
            videoEncoder.start(fatalError -> {
                Ln.d("Video encoder terminated. Fatal: " + fatalError);
            });
        }
        if (audioEncoder != null) {
            audioEncoder.start(fatalError -> {
                Ln.d("Audio encoder terminated. Fatal: " + fatalError);
            });
        }
        controller.start(fatalError -> {
            Ln.d("Controller terminated. Fatal: " + fatalError);
        });
    }

    public void stop() {
        for (AsyncProcessor ap : this.asyncProcessors) {
            if (ap != null) {
                ap.stop();
            }
        }
    }

    private void notifyChange() {
        if (listener != null) {
            listener.onConnectionChanged();
        }
    }

    public WebSocketConnection getConnection() {
        return this.connection;
    }

    public void setListener(SessionListener listener) {
        this.listener = listener;
    }

    public Size getVideoSize() {
        return surfaceCapture.getSize();
    }
}
