package edu.coursera.distributed;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * A basic and very limited implementation of a file server that responds to GET
 * requests from HTTP clients.
 */
public final class FileServer {

    public FileServer() {}

    /**
     * Main entrypoint for the basic file server.
     *
     * @param socket Provided socket to accept connections on.
     * @param fs A proxy filesystem to serve files from. See the PCDPFilesystem
     *           class for more detailed documentation of its usage.
     * @param ncores The number of cores that are available to your
     *               multi-threaded file server. Using this argument is entirely
     *               optional. You are free to use this information to change
     *               how you create your threads, or ignore it.
     * @throws IOException If an I/O error is detected on the server. This
     *                     should be a fatal error, your file server
     *                     implementation is not expected to ever throw
     *                     IOExceptions during normal operation.
     */
    public void run(final ServerSocket socket,
                    final PCDPFilesystem fs,
                    final int ncores) throws IOException {
        while (true) {
            Socket s = socket.accept();

            Thread thread = new Thread(() -> {
                try {
                    InputStream stream = s.getInputStream();
                    InputStreamReader reader = new InputStreamReader(stream);
                    BufferedReader buffered = new BufferedReader(reader);

                    String line = buffered.readLine();
                    assert line != null;
                    assert line.startsWith("GET");
                    final String path = line.split(" ")[1];

                    // Read file
                    PCDPPath fullPath = new PCDPPath(path);
                    String contents = fs.readFile(fullPath);

                    // HTTP response
                    OutputStream out = s.getOutputStream();
                    PrintWriter printer = new PrintWriter(out);

                    if (contents != null) {
                        printer.write("HTTP/1.0 200 OK\r\n");
                        printer.write("\r\n");
                        printer.write("\r\n");
                        printer.write(contents + "\r\n");
                    } else {
                        printer.write("HTTP/1.0 404 Not Found\r\n");
                        printer.write("\r\n");
                        printer.write("\r\n");
                    }
                    printer.close();
                } catch (Exception ex) {
                    System.out.println("Exception: " + ex.toString());
                }
            });

            thread.start();
        }
    }


}
