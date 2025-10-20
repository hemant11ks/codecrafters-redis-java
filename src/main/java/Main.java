import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.LinkedList; // Import LinkedList
import java.util.List; // Import List
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Main server class.
 */
public class Main {
  
  // Update dataStore to hold any type of RedisData
  private static final Map<String, RedisData> dataStore = new ConcurrentHashMap<>();

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
  private Map<String, RedisData> dataStore;

  // Update constructor to accept the new map type
  public ClientHandler(Socket socket, Map<String, RedisData> dataStore) {
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
            if (commandParts.size() < 3) {
              outputStream.write("-ERR wrong number of arguments for 'set' command\r\n".getBytes());
              break;
            }
            
            String key = commandParts.get(1);
            String value = commandParts.get(2);
            long expiryTime = -1; 

            if (commandParts.size() == 5) {
              if (commandParts.get(3).equalsIgnoreCase("PX")) {
                try {
                  long duration = Long.parseLong(commandParts.get(4));
                  expiryTime = System.currentTimeMillis() + duration;
                } catch (NumberFormatException e) {
                  outputStream.write("-ERR value is not an integer or out of range\r\n".getBytes());
                  break; 
                }
              } else {
                outputStream.write("-ERR syntax error\r\n".getBytes());
                break;
              }
            } else if (commandParts.size() != 3) {
              outputStream.write("-ERR wrong number of arguments for 'set' command\r\n".getBytes());
              break;
            }

            // Create a RedisString object
            RedisString stringEntry = new RedisString(value, expiryTime);
            dataStore.put(key, stringEntry);
            
            outputStream.write("+OK\r\n".getBytes());
            break;
            
          case "GET":
            if (commandParts.size() != 2) {
              outputStream.write("-ERR wrong number of arguments for 'get' command\r\n".getBytes());
            } else {
              String getKey = commandParts.get(1);
              RedisData getValue = dataStore.get(getKey);
              
              if (getValue == null) {
                // Key not found
                outputStream.write("$-1\r\n".getBytes());
              } else if (getValue.isExpired()) {
                // Key found, but it's expired
                dataStore.remove(getKey); // Lazy eviction
                outputStream.write("$-1\r\n".getBytes());
              } else if (getValue instanceof RedisString) {
                // Key found, not expired, and is a String
                RedisString foundString = (RedisString) getValue;
                String response = "$" + foundString.value.length() + "\r\n" + foundString.value + "\r\n";
                outputStream.write(response.getBytes());
              } else {
                // Key found, but it's not a String (e.g., it's a List)
                outputStream.write("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".getBytes());
              }
            }
            break;

          case "RPUSH":
            // RPUSH key element [element ...]
            if (commandParts.size() < 3) {
              outputStream.write("-ERR wrong number of arguments for 'rpush' command\r\n".getBytes());
              break;
            }

            String listKey = commandParts.get(1);
            RedisData existingEntry = dataStore.get(listKey);
            RedisList list;

            if (existingEntry == null) {
              // Case 1: Key doesn't exist. Create a new list.
              list = new RedisList();
              dataStore.put(listKey, list);
            } else if (existingEntry.isExpired()) {
              // Case 2: Key exists but is expired. Evict and create new.
              dataStore.remove(listKey);
              list = new RedisList();
              dataStore.put(listKey, list);
            } else if (existingEntry instanceof RedisList) {
              // Case 3: Key exists and is a list.
              list = (RedisList) existingEntry;
            } else {
              // Case 4: Key exists but is NOT a list.
              outputStream.write("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".getBytes());
              break;
            }
            
            // Add all provided elements to the list
            int newSize = 0;
            for (int i = 2; i < commandParts.size(); i++) {
              newSize = list.rpush(commandParts.get(i));
            }
            
            // Respond with the *final* size of the list as an Integer
            outputStream.write((":" + newSize + "\r\n").getBytes());
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
 * Base class for all data types stored in Redis.
 * Handles expiry.
 */
abstract class RedisData {
  long expiryTime; // Absolute time in milliseconds when this expires

  public RedisData(long expiryTime) {
    this.expiryTime = expiryTime;
  }
  
  public RedisData() {
    this.expiryTime = -1; // Default: no expiry
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

/**
 * Represents a String value in Redis.
 */
class RedisString extends RedisData {
  String value;

  public RedisString(String value, long expiryTime) {
    super(expiryTime);
    this.value = value;
  }

  public RedisString(String value) {
    super(-1); // No expiry
    this.value = value;
  }
}

/**
 * Represents a List value in Redis.
 */
class RedisList extends RedisData {
  // Use LinkedList for efficient push/pop from both ends
  List<String> list = new LinkedList<>();

  public RedisList() {
    super(-1); // No expiry
  }
  
  /**
   * Appends an element to the end of the list (RPUSH).
   * @param element The string element to add.
   * @return The new size of the list.
   */
  public int rpush(String element) {
    list.add(element);
    return list.size();
  }
}