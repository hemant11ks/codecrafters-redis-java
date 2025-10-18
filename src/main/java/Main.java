import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

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
        // Pass the clientSocket to the new ClientHandler object
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
      // This loop handles multiple commands from the *same* client
      while (true) {
        byte[] input = new byte[1024];
        
        // read() will block until data is received
        // or return -1 if the client disconnects
        int bytesRead = clientSocket.getInputStream().read(input);
        
        if (bytesRead == -1) {
          System.out.println("Client disconnected.");
          break; // Exit the loop for this client
        }

        // Per the instructions, just send "PONG" for any command
        clientSocket.getOutputStream().write("+PONG\r\n".getBytes());
      }
    } catch (IOException e) {
      // Handle exceptions that occur during client communication
      // e.g., "Connection reset by peer" if client force-quits
      System.out.println("IOException in client handler: " + e.getMessage());
    } finally {
      // This is crucial: ensure each client's socket is closed
      // when its handling loop finishes (due to disconnection or error).
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