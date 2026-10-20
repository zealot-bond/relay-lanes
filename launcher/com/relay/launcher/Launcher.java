package com.relay.launcher;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Unpacks the relay payload out of this jar and runs it on the bundled Node runtime.
 *
 * A Pterodactyl server only ever runs `java -jar server.jar`, so everything the relay
 * needs -- the JavaScript, a Node runtime, the baked configuration -- has to travel
 * inside the jar and be unpacked on first start.
 *
 * The operator start.properties is preserved across re-extraction: it lives in the app
 * directory and is meant to stay editable after an upload, and wiping it silently
 * discarded their overrides.
 */
public final class Launcher {

    private static final String APP_DIR = "app-payload";
    private static final String ENTRY = "src/main.js";
    private static final String STAMP = ".payload-stamp";

    public static void main(String[] args) throws Exception {
        File jar = locateJar();
        System.out.println("[launcher] jar=" + jar.getAbsolutePath());

        File work = new File(jar.getParentFile(), ".relay");
        if (!work.isDirectory() && !work.mkdirs()) {
            throw new IOException("cannot create work directory " + work);
        }
        System.out.println("[launcher] work=" + work.getAbsolutePath());

        File appDir = new File(work, APP_DIR);
        if (needsExtract(jar, work)) {
            System.out.println("[launcher] unpacking application payload...");
            byte[] saved = readIfPresent(new File(appDir, "start.properties"));
            deleteRecursively(appDir);
            extract(jar, work);
            if (saved != null) {
                File props = new File(appDir, "start.properties");
                Files.write(props.toPath(), saved);
                System.out.println("[launcher] preserved start.properties across re-extract");
            }
            writeStamp(jar, work);
        }

        File node = findNode(appDir);
        if (node != null) {
            System.out.println("[launcher] using bundled node runtime");
            System.out.println("[launcher] node=" + node.getAbsolutePath());
        } else {
            System.out.println("[launcher] no bundled node runtime; falling back to node from PATH");
        }

        List<String> cmd = new ArrayList<String>();
        cmd.add(node != null ? node.getAbsolutePath() : "node");
        cmd.add(new File(appDir, ENTRY).getAbsolutePath());
        for (String a : args) cmd.add(a);

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.directory(appDir);
        pb.environment().put("NODE_ENV", "production");
        pb.inheritIO();
        final Process child = pb.start();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() { child.destroy(); }
        }));
        System.exit(child.waitFor());
    }

    private static File locateJar() throws Exception {
        String[] candidates = {
            System.getProperty("java.class.path"),
            System.getProperty("sun.java.command"),
            "/home/container/server.jar",
        };
        for (String c : candidates) {
            if (c == null) continue;
            for (String part : c.split("[:\\s]+")) {
                if (part.endsWith(".jar") && new File(part).isFile()) return new File(part).getAbsoluteFile();
            }
        }
        File self = new File(Launcher.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (self.isFile()) return self;
        throw new IOException("cannot locate server.jar");
    }

    private static boolean needsExtract(File jar, File work) throws Exception {
        File stamp = new File(work, STAMP);
        if (!stamp.isFile()) return true;
        return jar.lastModified() > stamp.lastModified();
    }

    private static void writeStamp(File jar, File work) throws Exception {
        File stamp = new File(work, STAMP);
        if (!stamp.isFile() && !stamp.createNewFile()) return;
        stamp.setLastModified(jar.lastModified());
    }

    private static byte[] readIfPresent(File f) throws Exception {
        return f.isFile() ? Files.readAllBytes(f.toPath()) : null;
    }

    private static void extract(File jar, File work) throws Exception {
        ZipFile zip = new ZipFile(jar);
        try {
            Enumeration<? extends ZipEntry> entries = zip.entries();
            while (entries.hasMoreElements()) {
                ZipEntry e = entries.nextElement();
                String name = e.getName();
                if (name.endsWith("/")) continue;
                int cut = name.indexOf(APP_DIR + "/");
                if (cut < 0) continue;
                String rel = name.substring(cut + APP_DIR.length() + 1);
                if (rel.isEmpty()) continue;
                Path out = Paths.get(work.getAbsolutePath(), APP_DIR, rel);
                Files.createDirectories(out.getParent());
                InputStream in = zip.getInputStream(e);
                try {
                    OutputStream os = Files.newOutputStream(out);
                    try { copy(in, os); } finally { os.close(); }
                } finally { in.close(); }
                if (e.getTime() > 0) out.toFile().setLastModified(e.getTime());
                // The EXECUTABLE BIT must survive the extraction. Files.newOutputStream
                // creates them 0644, so the bundled Node runtime came out non-executable,
                // the launcher could not find it, and it fell back to node from PATH --
                // which the Pterodactyl container does not have, so the jar would have
                // failed to start on the panel. Caught by the CI boot test, which is the
                // only reason it was caught at all.
                if (rel.startsWith("node/bin/")) {
                    out.toFile().setReadable(true, false);
                    out.toFile().setExecutable(true, false);
                }
            }
        } finally { zip.close(); }
    }

    private static void copy(InputStream in, OutputStream out) throws IOException {
        byte[] buf = new byte[65536];
        int n;
        while ((n = in.read(buf)) > 0) out.write(buf, 0, n);
    }

    private static File findNode(File appDir) {
        File[] candidates = {
            new File(appDir, "node/bin/node"),
            new File(appDir, "node/bin/node.exe"),
        };
        for (File c : candidates) if (c.isFile() && c.canExecute()) return c;
        return null;
    }

    private static void deleteRecursively(File f) {
        if (f == null || !f.exists()) return;
        File[] kids = f.listFiles();
        if (kids != null) for (File k : kids) deleteRecursively(k);
        f.delete();
    }
}