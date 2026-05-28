package com.genymobile.scrcpy.ws;

public interface SessionListener {
    void onSessionJoined(int displayId);

    void onSessionClosed(int displayId);

    void onConnectionChanged();
}
