package com.genymobile.scrcpy.util;

import android.os.Build;

import com.genymobile.scrcpy.AndroidVersions;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;

public final class AndroidApiCompatibilityUtils {
    public static byte[] readAllBytes(DataInputStream dis) throws IOException {
        if (Build.VERSION.SDK_INT >= AndroidVersions.API_33_ANDROID_13) {
            return dis.readAllBytes();
        }

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] tmp = new byte[4096];
        int read;

        while ((read = dis.read(tmp)) != -1) {
            buffer.write(tmp, 0, read);
        }

        return buffer.toByteArray();
    }
}
