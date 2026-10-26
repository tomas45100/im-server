package utb.fai;

import java.util.*;
import java.util.concurrent.*;

public class ActiveHandlers {
    private ConcurrentHashMap<String, SocketHandler> activeHandlersMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Set<SocketHandler>> rooms = new ConcurrentHashMap<>();

    synchronized void add(SocketHandler handler) {
        activeHandlersMap.put(handler.clientID, handler);
        joinRoom(handler, "public"); // Automaticky do public
    }

    synchronized void remove(SocketHandler handler) {
        activeHandlersMap.remove(handler.clientID);
        for (String roomName : new HashSet<>(rooms.keySet())) {
            leaveRoom(handler, roomName);
        }
    }

    synchronized void joinRoom(SocketHandler handler, String roomName) {
        rooms.computeIfAbsent(roomName, k -> ConcurrentHashMap.newKeySet()).add(handler);
        handler.joinedRooms.add(roomName);
    }

    synchronized void leaveRoom(SocketHandler handler, String roomName) {
        Set<SocketHandler> room = rooms.get(roomName);
        if (room != null) {
            room.remove(handler);
            if (room.isEmpty()) {
                rooms.remove(roomName);
            }
        }
        handler.joinedRooms.remove(roomName);
    }

    // Odeslání zprávy všem uživatelům ve stejných místnostech
    synchronized void sendMessageToGroup(SocketHandler sender, String message) {
        Set<SocketHandler> recipients = ConcurrentHashMap.newKeySet();
        for (String roomName : sender.joinedRooms) {
            Set<SocketHandler> roomMembers = rooms.get(roomName);
            if (roomMembers != null) {
                recipients.addAll(roomMembers);
            }
        }

        for (SocketHandler handler : recipients) {
            if (handler != sender) {
                if (!handler.messages.offer(message)) {
                    System.err.printf("Client %s message queue is full!\n", handler.name);
                }
            }
        }
    }

    // Soukromá zpráva
    synchronized boolean sendPrivateMessage(SocketHandler sender, String targetName, String message) {
        SocketHandler target = null;
        for (SocketHandler h : activeHandlersMap.values()) {
            if (targetName.equals(h.name)) {
                target = h;
                break;
            }
        }

        if (target != null) {
            String formattedMsg = "[" + sender.name + "] >> " + message;
            return target.messages.offer(formattedMsg);
        }
        return false;
    }

    synchronized boolean isNameTaken(String name) {
        for (SocketHandler h : activeHandlersMap.values()) {
            if (name.equals(h.name)) {
                return true;
            }
        }
        return false;
    }
}