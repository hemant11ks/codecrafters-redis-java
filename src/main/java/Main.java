import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class Main {
  public static void main(String[] args) {
    System.out.println("Logs from your program will appear here!");

    ServerSocket serverSocket = null;
    int port = 6379;

    try {
      serverSocket = new ServerSocket(port);
      serverSocket.setReuseAddress(true);

      // Infinitely loop to accept new client connections
      while (true) {
        // Wait for connection from client. This blocks until a client connects.
        Socket clientSocket = serverSocket.accept();
        System.out.println("Client connected");

        // Create a new thread to handle the client connection
        ClientHandler handler = new ClientHandler(clientSocket);
        new Thread(handler).start();
      }

    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      try {
        if (serverSocket != null) {
          serverSocket.close();
        }
      } catch (IOException e) {
        System.out.println("IOException closing server socket: " + e.getMessage());
      }
    }
  }
}

/**
 * Handles a single client connection in its own thread.
 */
class ClientHandler implements Runnable {
  private Socket clientSocket;

  public ClientHandler(Socket socket) {
    this.clientSocket = socket;
  }

  @Override
  public void run() {
    try {
      // Use BufferedReader to read lines (commands) from the client
      InputStream inputStream = clientSocket.getInputStream();
      BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
      OutputStream outputStream = clientSocket.getOutputStream();
      
      String line;

      // Loop to read commands until the client disconnects (readLine() returns null)
      while ((line = reader.readLine()) != null) {
        // 1. All commands are RESP Arrays. First line is the array header.
        // Example: *2 (for *2\r\n)
        if (!line.startsWith("*")) {
          System.out.println("Protocol error: Expected Array header, got: " + line);
          continue;
        }

        // Parse array length
        int numElements = Integer.parseInt(line.substring(1));
        ArrayList<String> commandParts = new ArrayList<>();

        // 2. Read each element (command and arguments) from the array
        for (int i = 0; i < numElements; i++) {
          // Read bulk string header (e.g., $4)
          String bulkHeader = reader.readLine();
          if (bulkHeader == null || !bulkHeader.startsWith("$")) {
            throw new IOException("Protocol error: Expected Bulk String header");
          }
          
          // Parse bulk string length
          int length = Integer.parseInt(bulkHeader.substring(1));

          // Read the actual data (e.g., "ECHO")
          char[] data = new char[length];
          reader.read(data, 0, length);
          
          // Consume the trailing \r\n
          reader.skip(2); 
          
          commandParts.add(new String(data));
        }

        // 3. Process the parsed command
        if (commandParts.isEmpty()) {
          continue;
        }

        // Use toUpperCase() for case-insensitive command matching
        String command = commandParts.get(0).toUpperCase();

        switch (command) {
          case "PING":
            // Respond with a Simple String
            outputStream.write("+PONG\r\n".getBytes());
            break;

          case "ECHO":
            if (commandParts.size() < 2) {
              // Handle error: not enough arguments
              outputStream.write("-ERR wrong number of arguments for 'echo' command\r\n".getBytes());
            } else {
              // Get the argument to echo
              String echoArg = commandParts.get(1);
              // Respond with a Bulk String: $length\r\nargument\r\n
              String response = "$" + echoArg.length() + "\r\n" + echoArg + "\r\n";
              outputStream.write(response.getBytes());
            }
            break;
          
          default:
            // Handle unknown command
            outputStream.write(("-ERR unknown command '" + commandParts.get(0) + "'\r\n").getBytes());
        }
      }
    } catch (IOException e) {
      // This often happens when the client disconnects normally
      System.out.println("Client disconnected or IOException: " + e.getMessage());
    } catch (NumberFormatException e) {
      System.out.println("Protocol error, bad number format: " + e.getMessage());
    } finally {
      // Always close the client socket when the thread is done
      try {
        if (clientSocket != null) {
          clientSocket.close();
        }
      } catch (IOException e) {
        System.out.println("IOException closing client socket: " + e.getMessage());
      }
    }
  }
}