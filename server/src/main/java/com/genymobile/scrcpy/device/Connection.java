package com.genymobile.scrcpy.device;

import com.genymobile.scrcpy.control.ControlChannel;

import java.io.Closeable;
import java.io.FileDescriptor;
import java.io.IOException;

public abstract class Connection implements Closeable {
    public static final int DEVICE_NAME_FIELD_LENGTH = 64;

    public abstract void sendDeviceMeta(String deviceName) throws IOException;

    public abstract FileDescriptor getVideoFd();

    public abstract FileDescriptor getAudioFd();

    public abstract ControlChannel getControlChannel();

    public void shutdown() throws IOException {
    }
}
