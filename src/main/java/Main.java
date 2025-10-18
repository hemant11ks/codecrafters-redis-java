import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Map; // Import Map
import java.util.concurrent.ConcurrentHashMap; // Import ConcurrentHashMap

public class Main {
  
  // Create a thread-safe, shared key-value store
  private static final Map<String, String> dataStore = new ConcurrentHashMap<>();

  public static void main(String[] args) {
    System.out.println("Logs from your program will appear here!");

    ServerSocket serverSocket = null;
    int port = 6379;

    try {
      serverSocket = new ServerSocket(port);
      serverSocket.setReuseAddress(true);

      while (true) {
        Socket clientSocket = serverSocket.accept();
        System.out.println("Client connected");

        // Pass the shared dataStore to each new ClientHandler
        ClientHandler handler = new ClientHandler(clientSocket, dataStore);
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
  // Field to hold the reference to the shared data store
  private Map<String, String> dataStore;

  // Update constructor to accept the data store
  public ClientHandler(Socket socket, Map<String, String> dataStore) {
    this.clientSocket = socket;
    this.dataStore = dataStore;
  }

  @Override
  public void run() {
    try {
      InputStream inputStream = clientSocket.getInputStream();
      BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
      OutputStream outputStream = clientSocket.getOutputStream();
      
      String line;

      while ((line = reader.readLine()) != null) {
        // --- RESP Parsing (same as before) ---
        if (!line.startsWith("*")) {
          System.out.println("Protocol error: Expected Array header, got: " + line);
          continue;
        }

        int numElements = Integer.parseInt(line.substring(1));
        ArrayList<String> commandParts = new ArrayList<>();

        for (int i = 0; i < numElements; i++) {
          String bulkHeader = reader.readLine();
          if (bulkHeader == null || !bulkHeader.startsWith("$")) {
            throw new IOException("Protocol error: Expected Bulk String header");
          }
          int length = Integer.parseInt(bulkHeader.substring(1));
          char[] data = new char[length];
          reader.read(data, 0, length);
          reader.skip(2); // Consume \r\n
          commandParts.add(new String(data));
        }
        // --- End of RESP Parsing ---

        if (commandParts.isEmpty()) {
          continue;
        }

        String command = commandParts.get(0).toUpperCase();

        // --- Command Handling ---
        switch (command) {
          case "PING":
            outputStream.write("+PONG\r\n".getBytes());
            break;

          case "ECHO":
            if (commandParts.size() < 2) {
              outputStream.write("-ERR wrong number of arguments for 'echo' command\r\n".getBytes());
            } else {
              String echoArg = commandParts.get(1);
              String response = "$" + echoArg.length() + "\r\n" + echoArg + "\r\n";
              outputStream.write(response.getBytes());
            }
            break;
          
          case "SET":
            // SET key value
            if (commandParts.size() != 3) {
              outputStream.write("-ERR wrong number of arguments for 'set' command\r\n".getBytes());
            } else {
              String key = commandParts.get(1);
              String value = commandParts.get(2);
              dataStore.put(key, value);
              // Respond with Simple String "OK"
              outputStream.write("+OK\r\n".getBytes());
            }
            break;
            
          case "GET":
            // GET key
            if (commandParts.size() != 2) {
              outputStream.write("-ERR wrong number of arguments for 'get' command\r\n".getBytes());
            } else {
              String key = commandParts.get(1);
              String value = dataStore.get(key);
              
              if (value == null) {
                // Key not found: Respond with Null Bulk String
                outputStream.write("$-1\r\n".getBytes());
              } else {
                // Key found: Respond with Bulk String
                String response = "$" + value.length() + "\r\n" + value + "\r\n";
                outputStream.write(response.getBytes());
              }
            }
            break;

          default:
            outputStream.write(("-ERR unknown command '" + commandParts.get(0) + "'\r\n").getBytes());
        }
      }
    } catch (IOException e) {
      System.out.println("Client disconnected or IOException: " + e.getMessage());
    } catch (NumberFormatException e) {
      System.out.println("Protocol error, bad number format: " + e.getMessage());
    } finally {
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