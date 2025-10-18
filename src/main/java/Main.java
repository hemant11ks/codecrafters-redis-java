import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Main server class.
 */
public class Main {
  
  // Update dataStore to hold ValueEntry objects
  private static final Map<String, ValueEntry> dataStore = new ConcurrentHashMap<>();

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
  // Update dataStore field type
  private Map<String, ValueEntry> dataStore;

  // Update constructor to accept the new map type
  public ClientHandler(Socket socket, Map<String, ValueEntry> dataStore) {
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
            // Must have at least SET key value (3 parts)
            if (commandParts.size() < 3) {
              outputStream.write("-ERR wrong number of arguments for 'set' command\r\n".getBytes());
              break;
            }
            
            String key = commandParts.get(1);
            String value = commandParts.get(2);
            long expiryTime = -1; // Default: no expiry

            // Check for optional arguments (PX)
            // SET key value PX milliseconds (5 parts)
            if (commandParts.size() == 5) {
              if (commandParts.get(3).equalsIgnoreCase("PX")) {
                try {
                  long duration = Long.parseLong(commandParts.get(4));
                  expiryTime = System.currentTimeMillis() + duration;
                } catch (NumberFormatException e) {
                  outputStream.write("-ERR value is not an integer or out of range\r\n".getBytes());
                  break; // Don't proceed to set
                }
              } else {
                // Unknown option
                outputStream.write("-ERR syntax error\r\n".getBytes());
                break;
              }
            } else if (commandParts.size() != 3) {
              // Wrong number of args if not 3 or 5
              outputStream.write("-ERR wrong number of arguments for 'set' command\r\n".getBytes());
              break;
            }

            // Create the ValueEntry and put it in the map
            ValueEntry entry = new ValueEntry(value, expiryTime);
            dataStore.put(key, entry);
            
            outputStream.write("+OK\r\n".getBytes());
            break;
            
          case "GET":
            if (commandParts.size() != 2) {
              outputStream.write("-ERR wrong number of arguments for 'get' command\r\n".getBytes());
            } else {
              String getKey = commandParts.get(1);
              ValueEntry getValue = dataStore.get(getKey);
              
              if (getValue == null) {
                // Key not found
                outputStream.write("$-1\r\n".getBytes());
              } else if (getValue.isExpired()) {
                // Key found, but it's expired
                dataStore.remove(getKey); // Lazy eviction
                outputStream.write("$-1\r\n".getBytes());
              } else {
                // Key found and not expired
                String response = "$" + getValue.value.length() + "\r\n" + getValue.value + "\r\n";
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

/**
 * A helper class to store the value and its expiry time.
 */
class ValueEntry {
  String value;
  long expiryTime; // Absolute time in milliseconds when this expires

  /**
   * Creates a new ValueEntry.
   * @param value The string value to store.
   * @param expiryTime The absolute system time (in ms) when this entry should expire.
   * -1 indicates no expiry.
   */
  public ValueEntry(String value, long expiryTime) {
    this.value = value;
    this.expiryTime = expiryTime;
  }

  /**
   * Checks if this entry is expired.
   * @return true if the entry has an expiry time and the current time is past it,
   * false otherwise.
   */
  public boolean isExpired() {
    if (expiryTime == -1) {
      return false; // No expiry set
    }
    return System.currentTimeMillis() > expiryTime;
  }
}