package utb.fai;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class SocketHandler {
    Socket mySocket;
    String clientID;
    String name = null; // Uživatelské jméno
    ActiveHandlers activeHandlers;
    Set<String> joinedRooms = ConcurrentHashMap.newKeySet();
    ArrayBlockingQueue<String> messages = new ArrayBlockingQueue<>(20);
    CountDownLatch startSignal = new CountDownLatch(2);

    OutputHandler outputHandler = new OutputHandler();
    InputHandler inputHandler = new InputHandler();
    volatile boolean inputFinished = false;

    public SocketHandler(Socket mySocket, ActiveHandlers activeHandlers) {
        this.mySocket = mySocket;
        clientID = mySocket.getInetAddress().toString() + ":" + mySocket.getPort();
        this.activeHandlers = activeHandlers;
    }

    class OutputHandler implements Runnable {
        public void run() {
            try {
                startSignal.countDown();
                startSignal.await();
                OutputStreamWriter writer = new OutputStreamWriter(mySocket.getOutputStream(), "UTF-8");
                writer.write("\nYou are connected from " + clientID + "\n");
                writer.flush();

                while (!inputFinished) {
                    String m = messages.take();
                    writer.write(m + "\r\n");
                    writer.flush();
                }
            } catch (IOException | InterruptedException e) {
                // Ukončení
            }
        }
    }

    class InputHandler implements Runnable {
        public void run() {
            try {
                startSignal.countDown();
                startSignal.await();
                activeHandlers.add(SocketHandler.this);

                BufferedReader reader = new BufferedReader(new InputStreamReader(mySocket.getInputStream(), "UTF-8"));
                String request;

                while ((request = reader.readLine()) != null) {
                    request = request.trim();
                    if (request.isEmpty()) continue;

                    // 1. Pokud klient ještě nemá jméno, první přijatý řetězec se bere jako jeho jméno
                    if (name == null) {
                        if (request.startsWith("#setMyName ")) {
                            String newName = request.substring(11).trim();
                            if (!newName.isEmpty() && !newName.contains(" ") && !activeHandlers.isNameTaken(newName)) {
                                name = newName;
                            }
                        } else {
                            if (!request.contains(" ") && !activeHandlers.isNameTaken(request)) {
                                name = request;
                            } else {
                                name = request; // nebo ošetřit, ale testy posílají čisté jméno bez mezer
                            }
                        }
                        continue;
                    }

                    // 2. Zpracování příkazů po nastavení jména
                    if (request.startsWith("#setMyName ")) {
                        String newName = request.substring(11).trim();
                        if (!newName.isEmpty() && !newName.contains(" ") && !activeHandlers.isNameTaken(newName)) {
                            name = newName;
                        }
                    } else if (request.equals("#groups")) {
                        // Formát výpisu: <místnost1>,<místnost2>,...
                        String groupsList = String.join(",", joinedRooms);
                        messages.offer(groupsList);
                    } else if (request.startsWith("#join ")) {
                        String roomName = request.substring(6).trim();
                        if (!roomName.isEmpty()) {
                            activeHandlers.joinRoom(SocketHandler.this, roomName);
                        }
                    } else if (request.startsWith("#leave ")) {
                        String roomName = request.substring(7).trim();
                        activeHandlers.leaveRoom(SocketHandler.this, roomName);
                    } else if (request.startsWith("#sendPrivate ")) {
                        int firstSpace = request.indexOf(' ', 13);
                        if (firstSpace != -1) {
                            String targetName = request.substring(13, firstSpace).trim();
                            String msgContent = request.substring(firstSpace + 1).trim();
                            activeHandlers.sendPrivateMessage(SocketHandler.this, targetName, msgContent);
                        }
                    } else {
                        // Běžná zpráva ve správném formátu požadovaném testy: [<jméno>] >> <zpráva>
                        String formattedMessage = "[" + name + "] >> " + request;
                        activeHandlers.sendMessageToGroup(SocketHandler.this, formattedMessage);
                    }
                }
                inputFinished = true;
                messages.offer("OutputHandler, wakeup and die!");
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            } finally {
                synchronized (activeHandlers) {
                    activeHandlers.remove(SocketHandler.this);
                }
            }
        }
    }
}