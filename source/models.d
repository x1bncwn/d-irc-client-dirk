// source/models.d
module models;

import std.stdio;
import std.conv;
import std.string;
import std.array;
import std.algorithm;

immutable string defaultChannel = "#pike-test";
immutable string defaultServer = "irc.servercentral.net";
immutable ushort defaultPort = 9999;
immutable string defaultNick = "x2bncwn";

/// Types of messages sent from the IRC thread to the GTK thread
enum IrcToGtkType {
    chatMessage,
    channelUpdate,
    systemMessage,
    channelTopic
}

/// Structured chat message with separate raw nick and prefix
struct ChatMessage {
    string server;
    string channel;
    string timestamp;
    string rawNick;
    string prefix;
    string messageType;
    string body;
}

/// Channel join/part/failed update
struct ChannelUpdate {
    string server;
    string channel;
    string action;
}

/// Channel topic update
struct ChannelTopic {
    string server;
    string channel;
    string topic;
}

/// System message types
enum SystemMsgType {
    generic,
    motd,
    whois,
    error,
    info,
    warning
}

/// System message struct for generic or typed messages
struct SystemMessage {
    SystemMsgType msgType = SystemMsgType.generic;
    string text;

    // Constructor for generic system messages
    this(string t) {
        this.text = t;
    }

    // Constructor for typed system messages
    this(SystemMsgType type, string t) {
        this.text = t;
        this.msgType = type;
    }
}

/// Union of all messages sent to GTK
struct IrcToGtkMessage {
    IrcToGtkType type;

    union {
        ChatMessage chat;
        ChannelUpdate channelUpdate;
        ChannelTopic topicData;
        SystemMessage systemMsg;
    }

    // Factory methods
    static IrcToGtkMessage fromChat(ChatMessage c) {
        IrcToGtkMessage m;
        m.type = IrcToGtkType.chatMessage;
        m.chat = c;
        return m;
    }

    static IrcToGtkMessage fromUpdate(ChannelUpdate u) {
        IrcToGtkMessage m;
        m.type = IrcToGtkType.channelUpdate;
        m.channelUpdate = u;
        return m;
    }

    static IrcToGtkMessage fromSystem(string text) {
	IrcToGtkMessage m;
        m.type = IrcToGtkType.systemMessage;
        m.systemMsg = SystemMessage(text);
	return m;
    }

    static IrcToGtkMessage fromSystem(string text, SystemMsgType msgType) {
        IrcToGtkMessage m;
        m.type = IrcToGtkType.systemMessage;
        m.systemMsg = SystemMessage(msgType, text);
        return m;
    }

    static IrcToGtkMessage fromTopic(ChannelTopic t) {
        IrcToGtkMessage m;
        m.type = IrcToGtkType.channelTopic;
        m.topicData = t;
        return m;
    }
}

/// Messages from GTK to IRC thread
struct IrcFromGtkMessage {
    enum Type {
        Message,
        UpdateChannels,
        channelTopic
    }

    Type type;
    string channel;
    string text;
    string action;
}
