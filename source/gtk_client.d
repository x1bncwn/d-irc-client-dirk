// source/gtk_client.d
module gtk_client;

import gtk.application;
import gtk.application_window;
import gtk.scrolled_window;
import gtk.text_buffer;
import gtk.text_view;
import gtk.text_iter;
import gtk.text_tag;
import gtk.text_tag_table;
import gtk.entry;
import gtk.tree_view;
import gtk.tree_iter;
import gtk.tree_selection;
import gtk.button;
import gtk.paned;
import gtk.box;
import gtk.label;
import gtk.tree_view_column;
import gtk.cell_renderer_text;
import gtk.separator;
import gtk.header_bar;
import gtk.popover_menu;
import gtk.window;
import gtk.types;
import gtk.tree_model;
import gtk.menu_button;
import gtk.tree_store;
import gtk.settings;
import gtk.c.types;
import gtk.c.functions;

import gio.types;
import gio.menu;
import gio.simple_action;
import gio.menu_model;

import glib.variant;
import glib.types;
import glib.global;
import glib.source;
import glib.iochannel;

import gobject.value;
import gobject.types : GTypeEnum;

import std.concurrency;
import std.string;
import std.conv;
import std.algorithm;
import std.array;
import std.datetime;
import std.range;                                                            import std.math;
import core.atomic;
import core.time;
import core.thread;
import core.stdc.errno;
import core.sys.posix.unistd;                                                import core.sys.posix.fcntl;

import models;
import logging;
import irc_client;

struct ServerConnection
{
    Tid threadId;
    bool connected;
    string serverName;
}

class GTKClient
{
    Application app;
    ApplicationWindow window;
    TextView textView;
    TextBuffer textBuffer;
    Entry inputEntry;
    TreeView channelList;
    TreeStore channelStore;
    Box sidebar;
    Box mainBox;
    Paned hpaned;
    Box chatAreaBox;
    Box inputBox;

    string currentDisplay;
    string currentServer;
    ServerConnection[string] connections;
    Tid[string] serverThreads;

    TextBuffer[string] displayBuffers;
    string[] displayHistory;

    private bool colorizeNicks = true;
    private string timestampFormat = "[HH:mm]";
    private bool isDarkTheme = true;
    private bool autoSwitchToNewChannels = true;

    private int[2] pipeFds;
    private uint pipeSourceId = 0;

    private TextTag[string][string] nicknameTags;
    private TextTag[string][string] modeSymbolTags;
    private TextTag[string] timestampTags;
    private TextTag[string] systemMessageTags;

    // Track channel topics
    // server -> channel -> topic
    private string[string][string] channelTopics;

    this()
    {
        logToTerminal("Initializing GTK application", "INFO", "main");
        app = new Application("org.example.dIRC", ApplicationFlags.FlagsNone);

        // Create pipe for io callback
        logToTerminal("Creating pipe...", "DEBUG", "main");
        if (pipe(pipeFds) == -1)
        {
            logToTerminal("Failed to create pipe, errno: " ~ to!string(errno), "ERROR", "main");
            throw new Exception("Failed to create pipe: " ~ to!string(errno));
        }
        logToTerminal("Pipe created: read fd=" ~ to!string(pipeFds[0]) ~ ", write fd=" ~ to!string(pipeFds[1]), "DEBUG", "main");

        // Set non-blocking
        foreach (i, fd; pipeFds)
        {
            auto flags = fcntl(fd, F_GETFL, 0);
            if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1)
            {
                logToTerminal("Failed to set non-blocking on fd " ~ to!string(fd) ~ ", errno: " ~ to!string(errno), "WARNING", "main");
            }
        }

        currentDisplay = "System";
        currentServer = "";

        displayBuffers["System"] = new TextBuffer(null);
        initializeTextTags("System");

        app.connectActivate(delegate(Application app) { setupGui(); });
    }

    ~this()
    {
        // Clean up pipe
        if (pipeFds[0] != 0)
            core.sys.posix.unistd.close(pipeFds[0]);
        if (pipeFds[1] != 0)
            core.sys.posix.unistd.close(pipeFds[1]);
    }

    private void initializeTextTags(string bufferName)
    {
        if (!(bufferName in displayBuffers))
            return;

        auto buffer = displayBuffers[bufferName];
        auto tagTable = buffer.getTagTable();

        if (!(bufferName in timestampTags))
        {
            auto tag = new TextTag("timestamp-" ~ bufferName);
            tag.foreground = isDarkTheme ? "#FFFFFF" : "#000000";
            tagTable.add(tag);
            timestampTags[bufferName] = tag;
        }
        else
        {
            timestampTags[bufferName].foreground = isDarkTheme ? "#FFFFFF" : "#000000";
        }

        if (!(bufferName in systemMessageTags))
        {
            auto tag = new TextTag("system-" ~ bufferName);
            tag.foreground = isDarkTheme ? "#AAAAAA" : "#555555";
            tagTable.add(tag);
            systemMessageTags[bufferName] = tag;
        }
        else
        {
            systemMessageTags[bufferName].foreground = isDarkTheme ? "#AAAAAA" : "#555555";
        }

        if (!(bufferName in nicknameTags))
        {
            nicknameTags[bufferName] = null;
        }
        if (!(bufferName in modeSymbolTags))
        {
            modeSymbolTags[bufferName] = null;
        }
    }

    private TextTag getNicknameTag(string bufferName, string nickname, string color)
    {
        if (!(bufferName in nicknameTags))
        {
            return null;
        }

        string tagName = "nick-" ~ nickname;
        if (tagName in nicknameTags[bufferName])
        {
            return nicknameTags[bufferName][tagName];
        }

        if (!(bufferName in displayBuffers))
            return null;

        auto buffer = displayBuffers[bufferName];
        auto tagTable = buffer.getTagTable();
        auto tag = new TextTag(tagName);
        tag.foreground = color;
        tag.weight = 700;
        tagTable.add(tag);

        nicknameTags[bufferName][tagName] = tag;
        return tag;
    }

    private TextTag getModeSymbolTag(string bufferName, char modeSymbol)
    {
        if (!(bufferName in modeSymbolTags))
            return null;

        string tagName = "mode-" ~ modeSymbol;
        if (tagName in modeSymbolTags[bufferName])
        {
            return modeSymbolTags[bufferName][tagName];
        }

        if (!(bufferName in displayBuffers))
            return null;

        auto buffer = displayBuffers[bufferName];
        auto tagTable = buffer.getTagTable();
        auto tag = new TextTag(tagName);
        tag.foreground = getModeSymbolColor(modeSymbol);
        tag.weight = 700;
        tagTable.add(tag);

        modeSymbolTags[bufferName][tagName] = tag;
        return tag;
    }

    private void insertWithTags(TextBuffer buffer, TextIter iter, string text, TextTag[] tags...)
    {
        if (tags.length == 0)
        {
            buffer.insert(iter, text);
        }
        else
        {
            auto cBuffer = cast(GtkTextBuffer*) buffer._cPtr;
            auto cIter = cast(GtkTextIter*) iter._cPtr;
            auto cText = text.toStringz();
            auto cLength = cast(int) text.length;

            if (tags.length == 1)
            {
                auto tag1 = cast(GtkTextTag*) tags[0]._cPtr;
                gtk_text_buffer_insert_with_tags(cBuffer, cIter, cText, cLength, tag1, null);
            }
            else if (tags.length == 2)
            {
                auto tag1 = cast(GtkTextTag*) tags[0]._cPtr;
                auto tag2 = cast(GtkTextTag*) tags[1]._cPtr;
                gtk_text_buffer_insert_with_tags(cBuffer, cIter, cText, cLength, tag1, tag2, null);
            }
            else
            {
                auto tag1 = cast(GtkTextTag*) tags[0]._cPtr;
                gtk_text_buffer_insert_with_tags(cBuffer, cIter, cText, cLength, tag1, null);
            }
        }
    }

    void setupGui()
    {
        logToTerminal("Setting up GUI", "INFO", "main");

        auto settings = gtk.settings.Settings.getDefault();
        if (settings)
        {
            settings.gtkApplicationPreferDarkTheme = true;
            settings.gtkThemeName = "Default";
            isDarkTheme = true;
        }

        window = new ApplicationWindow(app);
        window.setTitle("D IRC Client - GTK 4");
        window.setDefaultSize(1024, 768);
        mainBox = new Box(Orientation.Vertical, 0);
        window.setChild(mainBox);

        auto headerBar = new HeaderBar();
        window.setTitlebar(headerBar);

        auto menuButton = new MenuButton();
        menuButton.setIconName("open-menu-symbolic");
        headerBar.packStart(menuButton);

        auto menu = new Menu();
        menu.append("Connect", "app.connect");
        menu.append("Disconnect", "app.disconnect");
        menu.append("Quit", "app.quit");
        menu.append("Light Theme", "app.light-theme");
        menu.append("Dark Theme", "app.dark-theme");

        auto popoverMenu = PopoverMenu.newFromModel(menu);
        menuButton.setPopover(popoverMenu);

        hpaned = new Paned(Orientation.Horizontal);
        hpaned.setShrinkStartChild(false);
        hpaned.setShrinkEndChild(false);
        hpaned.setResizeStartChild(true);
        hpaned.setResizeEndChild(true);
        hpaned.setPosition(cast(int)(1024 * 0.2));
        mainBox.append(hpaned);

        setupSidebar();
        setupChatArea();
        setupInputArea();

        chatAreaBox.setVexpand(true);
        textView.setVexpand(true);
        inputBox.setMarginTop(10);

        setupActions();
        setupSignals();

        window.present();

        // Show welcome message
        appendWelcomeMessage();
    }

    private void appendWelcomeMessage()
    {
        appendSystemMessage("Welcome to D IRC Client!");
        appendSystemMessage("Type /connect <server> to connect to an IRC server");
        appendSystemMessage("Type /join #channel to join a channel");
        appendSystemMessage("Type /whois <nickname> for user information");
        appendSystemMessage("Type /help for more commands");
    }

    void setupSidebar()
    {
        sidebar = new Box(Orientation.Vertical, 5);
        sidebar.setMarginStart(5);
        sidebar.setMarginEnd(5);
        sidebar.setMarginTop(5);
        sidebar.setMarginBottom(5);
        sidebar.setSizeRequest(200, -1);

        auto sidebarLabel = new Label("Servers & Channels");
        sidebarLabel.setHalign(Align.Start);
        sidebar.append(sidebarLabel);

        auto separator = new Separator(Orientation.Horizontal);
        sidebar.append(separator);

        channelStore = TreeStore.new_([GTypeEnum.String, GTypeEnum.String]);
        channelList = new TreeView();
        channelList.setModel(channelStore);

        auto renderer = new CellRendererText();
        auto column = new TreeViewColumn();
        column.setTitle("Name");
        column.packStart(renderer, true);
        column.addAttribute(renderer, "text", 0);
        channelList.appendColumn(column);

        channelList.setVexpand(true);
        channelList.setHexpand(true);
        channelList.setHeadersVisible(false);

        auto scrolledSidebar = new ScrolledWindow();
        scrolledSidebar.setPolicy(PolicyType.Automatic, PolicyType.Automatic);
        scrolledSidebar.setChild(channelList);
        scrolledSidebar.setVexpand(true);
        scrolledSidebar.setHexpand(true);
        sidebar.append(scrolledSidebar);

        hpaned.setStartChild(sidebar);
    }

    void setupChatArea()
    {
        chatAreaBox = new Box(Orientation.Vertical, 5);
        chatAreaBox.setMarginStart(5);
        chatAreaBox.setMarginEnd(5);
        chatAreaBox.setMarginTop(5);
        chatAreaBox.setMarginBottom(5);
        chatAreaBox.setVexpand(true);
        chatAreaBox.setHexpand(true);

        auto chatBox = new Box(Orientation.Vertical, 0);
        chatBox.setVexpand(true);
        chatBox.setHexpand(true);

        textView = new TextView();
        textBuffer = displayBuffers["System"];
        textView.setBuffer(textBuffer);
        textView.setEditable(false);
        textView.setWrapMode(WrapMode.Word);
        textView.setVexpand(true);
        textView.setHexpand(true);
        textView.setAcceptsTab(false);

        auto scrolledChat = new ScrolledWindow();
        scrolledChat.setPolicy(PolicyType.Automatic, PolicyType.Automatic);
        scrolledChat.setChild(textView);
        scrolledChat.setVexpand(true);
        scrolledChat.setHexpand(true);

        chatBox.append(scrolledChat);
        chatAreaBox.append(chatBox);

        hpaned.setEndChild(chatAreaBox);
    }

    void setupInputArea()
    {
        inputBox = new Box(Orientation.Horizontal, 5);
        inputBox.setMarginStart(5);
        inputBox.setMarginEnd(5);
        inputBox.setMarginBottom(5);
        inputBox.setMarginTop(5);

        inputEntry = new Entry();
        inputEntry.setHexpand(true);
        inputEntry.setPlaceholderText("Type message or command...");

        auto sendButton = new Button();
        sendButton.setLabel("Send");
        sendButton.setMarginStart(5);

        inputBox.append(inputEntry);
        inputBox.append(sendButton);

        mainBox.append(inputBox);

        sendButton.connectClicked(delegate(Button button) { sendMessage(); });
    }

    void setupActions()
    {
        auto connectAction = new SimpleAction("connect", null);
        connectAction.connectActivate(delegate(glib.variant.Variant parameter) {
            logToTerminal("Connect action triggered", "INFO", "main");
            startConnection(defaultServer);
        });
        app.addAction(connectAction);

        auto disconnectAction = new SimpleAction("disconnect", null);
        disconnectAction.connectActivate(delegate(glib.variant.Variant parameter) {
            logToTerminal("Disconnect action triggered", "INFO", "main");
            disconnectFromServer();
        });
        app.addAction(disconnectAction);

        auto quitAction = new SimpleAction("quit", null);
        quitAction.connectActivate(delegate(glib.variant.Variant parameter) {
            logToTerminal("Quit action triggered", "INFO", "main");
            disconnectAllServers();
            app.quit();
        });
        app.addAction(quitAction);

        auto lightThemeAction = new SimpleAction("light-theme", null);
        lightThemeAction.connectActivate(delegate(glib.variant.Variant parameter) {
            logToTerminal("Switching to light theme", "INFO", "main");
            setApplicationTheme(false);
        });
        app.addAction(lightThemeAction);

        auto darkThemeAction = new SimpleAction("dark-theme", null);
        darkThemeAction.connectActivate(delegate(glib.variant.Variant parameter) {
            logToTerminal("Switching to dark theme", "INFO", "main");
            setApplicationTheme(true);
        });
        app.addAction(darkThemeAction);
    }

    void setApplicationTheme(bool darkMode)
    {
        auto settings = gtk.settings.Settings.getDefault();
        if (settings)
        {
            if (darkMode)
            {
                settings.gtkThemeName = "Default-dark";
                settings.gtkApplicationPreferDarkTheme = true;
                isDarkTheme = true;
            }
            else
            {
                settings.gtkThemeName = "Default";
                settings.gtkApplicationPreferDarkTheme = false;
                isDarkTheme = false;
            }
        }

        foreach (bufferName, buffer; displayBuffers)
        {
            updateTagsForTheme(bufferName, darkMode);
        }

        window.queueDraw();
        appendSystemMessage("Switched to " ~ (darkMode ? "dark" : "light") ~ " theme");
    }

    private void updateTagsForTheme(string bufferName, bool darkMode)
    {
        if (bufferName in timestampTags)
        {
            timestampTags[bufferName].foreground = darkMode ? "#FFFFFF" : "#000000";
        }

        if (bufferName in systemMessageTags)
        {
            systemMessageTags[bufferName].foreground = darkMode ? "#AAAAAA" : "#555555";
        }
    }

    void setupSignals()
    {
        inputEntry.connectActivate(delegate(Entry entry) { sendMessage(); });

        auto selection = channelList.getSelection();
        selection.connectChanged(delegate{
            TreeModel model;
            TreeIter iter;
            if (selection.getSelected(model, iter))
            {
                Value val = new Value("");
                channelStore.getValue(iter, 0, val);
                string display = val.getString();

                Value typeVal = new Value("");
                channelStore.getValue(iter, 1, typeVal);
                string itemType = typeVal.getString();

                currentDisplay = display;
                logToTerminal("Selected: " ~ display ~ " (type: " ~ itemType ~ ")", "INFO", "main");

                if (itemType == "server")
                {
                    currentServer = display;
                }
                else if (itemType == "channel")
                {
                    TreeIter parentIter;
                    if (channelStore.iterParent(parentIter, iter))
                    {
                        Value parentVal = new Value("");
                        channelStore.getValue(parentIter, 0, parentVal);
                        currentServer = parentVal.getString();
                    }
                }

                if (currentDisplay in displayBuffers)
                {
                    textBuffer = displayBuffers[currentDisplay];
                    textView.setBuffer(textBuffer);
                    scrollToEnd();
                }
            }
        });

        // Setup pipe watch - Use IOChannel and ioAddWatch
        logToTerminal("Setting up pipe watch on fd " ~ to!string(pipeFds[0]), "DEBUG", "main");

        auto channel = IOChannel.unixNew(pipeFds[0]);
        if (channel is null)
        {
            logToTerminal("IOChannel.unixNew returned null", "ERROR", "main");
            throw new Exception("Failed to create IOChannel for pipe");
        }
        logToTerminal("Created IOChannel successfully", "DEBUG", "main");

        pipeSourceId = ioAddWatch(channel, 0, IOCondition.In | IOCondition.Hup | IOCondition.Err, delegate bool(IOChannel channel, IOCondition condition) {
            logToTerminal("Pipe callback fired, condition: " ~ to!string(condition), "DEBUG", "main");

            if (condition & IOCondition.In)
            {
                // Drain pipe
                char[1] buffer;
                ssize_t bytesRead = 0;
                do
                {
                    bytesRead = core.sys.posix.unistd.read(pipeFds[0], buffer.ptr, 1);
                    if (bytesRead > 0)
                    {
                        logToTerminal("Read " ~ to!string(bytesRead) ~ " bytes from pipe", "DEBUG", "main");
                    }
                    else if (bytesRead == -1 && errno != EAGAIN && errno != EWOULDBLOCK)
                    {
                        logToTerminal("Read error, errno: " ~ to!string(errno), "ERROR", "main");
                    }
                }
                while (bytesRead > 0);

                // Process messages
                processPendingMessages();
            }

            if (condition & (IOCondition.Hup | IOCondition.Err))
            {
                logToTerminal("Pipe HUP or ERR condition", "ERROR", "main");
                return false; // Remove watch
            }

            return true; // Keep the watch active
        });

        logToTerminal("ioAddWatch returned source ID: " ~ to!string(pipeSourceId), "DEBUG", "main");

        window.connectCloseRequest(delegate(Window window) {
            logToTerminal("Close request received", "INFO", "main");
            disconnectAllServers();

            // Remove pipe source
            if (pipeSourceId > 0)
            {
                Source.remove(pipeSourceId);
            }

            // Close IRC threads
            foreach (server, tid; serverThreads)
            {
                send(tid, IrcFromGtkMessage(IrcFromGtkMessage.Type.UpdateChannels, "", "", "quit"));
            }

            Thread.sleep(100.msecs);
            app.quit();
            return true;
        });
    }

    private string getNickColor(string nickname)
    {
        if (!colorizeNicks)
            return isDarkTheme ? "#CCCCCC" : "#666666";

        string normalized = nickname.strip().toLower();
        if (normalized.length == 0)
            normalized = "user";

        uint hash1 = 0;
        uint hash2 = 0x811c9dc5u;

        for (int i = 0; i < normalized.length; i++)
        {
            char c = normalized[i];
            uint pos = i + 1;
            hash1 = ((hash1 << 5) + hash1) + c * pos;
            hash2 ^= c * (pos * 31);
            hash2 *= 0x01000193u;
        }

        uint combined = hash1 ^ hash2;
        combined ^= combined >> 16;
        combined *= 0x85ebca6bu;
        combined ^= combined >> 13;
        combined *= 0xc2b2ae35u;
        combined ^= combined >> 16;

        float hue = cast(float)(combined % 360);
        hue = hue * 0.618033988749895f;
        hue = fmod(hue, 360.0f);

        if (isDarkTheme)
        {
            float saturation = 0.85f;
            float lightness = 0.65f;
            uint varHash = (combined >> 8) & 0xFF;
            saturation += 0.1f * (cast(float) varHash / 255.0f);
            lightness += 0.1f * (cast(float)((combined >> 16) & 0xFF) / 255.0f);
            return hslToHex(hue, saturation, lightness);
        }
        else
        {
            float saturation = 0.9f;
            float lightness = 0.45f;
            uint varHash = (combined >> 8) & 0xFF;
            saturation += 0.05f * (cast(float) varHash / 255.0f);
            lightness += 0.1f * (cast(float)((combined >> 16) & 0xFF) / 255.0f);
            return hslToHex(hue, saturation, lightness);
        }
    }

    private string hslToHex(float h, float s, float l)
    {
	h = fmod(h, 360.0f);
	if (h < 0) h += 360.0f;
	s = s < 0.0f ? 0.0f : (s > 1.0f ? 1.0f : s);
	l = l < 0.0f ? 0.0f : (l > 1.0f ? 1.0f : l);
	float c = (1.0f - abs(2.0f * l - 1.0f)) * s;
	float x = c * (1.0f - abs(fmod(h / 60.0f, 2.0f) - 1.0f));
	float m = l - c / 2.0f;
	float r, g, b;

        if (h < 60)
        {
            r = c;
            g = x;
            b = 0;
        }
        else if (h < 120)
	{
	    r = x;
	    g = c;
	    b = 0;
	}
	else if (h < 180)
	{
	    r = 0;
            g = c;
            b = x;
	}
        else if (h < 240)
        {
            r = 0;
            g = x;
            b = c;
        }
        else if (h < 300)
        {
            r = x;
            g = 0;
            b = c;
        }
        else
        {
            r = c;
            g = 0;
            b = x;
        }
        r += m;
        g += m;
        b += m;
        r = r < 0.0f ? 0.0f : (r > 1.0f ? 1.0f : r);
        g = g < 0.0f ? 0.0f : (g > 1.0f ? 1.0f : g);
        b = b < 0.0f ? 0.0f : (b > 1.0f ? 1.0f : b);

        int ri = cast(int)(r * 255);
        int gi = cast(int)(g * 255);
        int bi = cast(int)(b * 255);
        return "#" ~ format("%02X%02X%02X", ri, gi, bi);
    }

    private string getModeSymbolColor(char modeSymbol)
    {
        switch (modeSymbol)
        {
        case '@':
            return isDarkTheme ? "#FF4444" : "#D32F2F";
        case '%':
            return isDarkTheme ? "#FF9800" : "#F57C00";
        case '+':
            return isDarkTheme ? "#4CAF50" : "#388E3C";
        case '&':
            return isDarkTheme ? "#2196F3" : "#1976D2";
        case '~':
            return isDarkTheme ? "#9C27B0" : "#7B1FA2";
        default:
            return isDarkTheme ? "#CCCCCC" : "#666666";
        }
    }

    private int getUtf8CharCount(string str)
    {
        return cast(int)(str.walkLength());
    }

    private bool findServerInTree(string server, ref TreeIter iter)
    {
        bool found = false;
        TreeIter childIter;
        if (channelStore.getIterFirst(childIter))
        {
            do
            {
                Value val = new Value("");
                channelStore.getValue(childIter, 0, val);
                string serverName = val.getString();

                Value typeVal = new Value("");
                channelStore.getValue(childIter, 1, typeVal);
                string itemType = typeVal.getString();
                if (itemType == "server" && serverName == server)
                {
                    iter = childIter;
                    found = true;
                    break;
                }
            }
            while (channelStore.iterNext(childIter));
        }
        return found;
    }

    private bool findChannelUnderServer(TreeIter serverIter, string channel, ref TreeIter iter)
    {
        bool found = false;
        TreeIter childIter;
        if (channelStore.iterChildren(childIter, serverIter))
        {
            do
            {
                Value val = new Value("");
                channelStore.getValue(childIter, 0, val);
                string channelName = val.getString();
                if (channelName == channel)
                {
                    iter = childIter;
                    found = true;
                    break;
                }
            }
            while (channelStore.iterNext(childIter));
        }
        return found;
    }

    private void addServerToTree(string server)
    {
        TreeIter serverIter;
        if (!findServerInTree(server, serverIter))
        {
            TreeIter newServerIter;
            channelStore.append(newServerIter, null);
            channelStore.setValue(newServerIter, 0, new Value(server));
            channelStore.setValue(newServerIter, 1, new Value("server"));
        }
    }

    private void addChannelToTree(string server, string channel)
    {
        TreeIter serverIter;
        if (findServerInTree(server, serverIter))
        {
            TreeIter channelIter;
            if (!findChannelUnderServer(serverIter, channel, channelIter))
            {
                TreeIter newChannelIter;
                channelStore.append(newChannelIter, serverIter);
                channelStore.setValue(newChannelIter, 0, new Value(channel));
                channelStore.setValue(newChannelIter, 1, new Value("channel"));
                channelList.expandRow(channelStore.getPath(serverIter), false);
            }
        }
    }

    private void removeChannelFromTree(string server, string channel)
    {
        TreeIter serverIter;
        if (findServerInTree(server, serverIter))
        {
            TreeIter channelIter;
            if (findChannelUnderServer(serverIter, channel, channelIter))
            {
                channelStore.remove(channelIter);
            }
        }
    }

    private void removeServerFromTree(string server)
    {
        TreeIter serverIter;
        if (findServerInTree(server, serverIter))
        {
            channelStore.remove(serverIter);
        }
    }

    private void startConnection(string server)
    {
        if (server in connections && connections[server].connected)
        {
            appendSystemMessage("Already connected to " ~ server ~ ".");
            return;
        }
        logToTerminal("Passing pipe write fd " ~ to!string(pipeFds[1]) ~ " to IRC thread", "DEBUG", "main");
        auto tid = spawn(&runIrcServer, server.strip(), thisTid, pipeFds[1]);
        serverThreads[server] = tid;
        connections[server] = ServerConnection(tid, true, server);
        if (!(server in displayBuffers))
        {
            displayBuffers[server] = new TextBuffer(null);
            initializeTextTags(server);
        }
        addServerToTree(server);

        currentDisplay = server;
        currentServer = server;
        textBuffer = displayBuffers[server];
        textView.setBuffer(textBuffer);
        scrollToEnd();

        appendSystemMessage("Connecting to " ~ server ~ "...");

        if (!displayHistory.canFind(server))
        {
            displayHistory ~= server;
        }
    }

    private void disconnectFromServer()
    {
        if (currentServer.length == 0 || !(currentServer in serverThreads))
        {
            return;
        }
        auto tid = serverThreads[currentServer];
        send(tid, IrcFromGtkMessage(IrcFromGtkMessage.Type.UpdateChannels, "", "", "quit"));
        Thread.sleep(100.msecs);
        removeServerFromTree(currentServer);
        connections.remove(currentServer);
        serverThreads.remove(currentServer);
        appendSystemMessage("Disconnected from " ~ currentServer ~ ".");
        currentServer = "";
        currentDisplay = "System";
        textBuffer = displayBuffers["System"];
        textView.setBuffer(textBuffer);
        scrollToEnd();
    }

    private void disconnectAllServers()
    {
        foreach (server, tid; serverThreads)
        {
            send(tid, IrcFromGtkMessage(IrcFromGtkMessage.Type.UpdateChannels, "", "", "quit"));
        }

        Thread.sleep(150.msecs);
        channelStore.clear();
        connections = null;
        serverThreads = null;

        appendSystemMessage("Disconnected from all servers.");
        currentDisplay = "System";
        currentServer = "";
        textBuffer = displayBuffers["System"];
        textView.setBuffer(textBuffer);
        scrollToEnd();
    }


    private void processPendingMessages()
    {
    	// Outer loop: keep processing as long as flag was set
    	do {
            // Inner loop: process all available messages
            bool gotMessage = true;
	    while (gotMessage)
            {
            	gotMessage = receiveTimeout(Duration.zero, (IrcToGtkMessage msg) {
		    logToTerminal("Processing message of type: " ~ to!string(msg.type), "DEBUG", "main");

                    final switch (msg.type)
                    {
                    	case IrcToGtkType.chatMessage:
                            auto data = msg.chat;
                            string display;
                            if (data.channel.length > 0)
                            {
                            	display = data.channel;
                            }
                            else
                            {
                            	display = data.server;
                            }
                            appendChatMessage(display, data.timestamp, data.prefix ~ data.rawNick, data.messageType, data.body);
                            break;
			case IrcToGtkType.channelUpdate:
                           auto u = msg.channelUpdate;
                           updateChannelList(u.server, u.channel, u.action);
			   break;
                    	case IrcToGtkType.systemMessage:
			    auto sysMsg = msg.systemMsg;
                            if (currentServer.length > 0)
                            {
                            	appendChatMessage(currentServer, formatTimestampNow(), "", "system", sysMsg.text);
                            }
                            else
                            {
                            	appendChatMessage("System", formatTimestampNow(), "", "system", sysMsg.text);
                            }
                            break;
                    	case IrcToGtkType.channelTopic:
                            auto topicData = msg.topicData;
                            handleChannelTopic(topicData.server, topicData.channel, topicData.topic);
                            break;
                    }
                    return true;
		});
	    }
	} while (atomicExchange(&pipeSignalPending, false));
    }

    private void handleChannelTopic(string server, string channel, string topic)
    {
        if (!(server in channelTopics))
        {
            channelTopics[server] = null;
        }
        channelTopics[server][channel] = topic;

        if (channel in displayBuffers)
        {
            appendChatMessage(channel, formatTimestampNow(), "", "system", "Topic: " ~ topic);
        }
        else
        {
            appendChatMessage(server, formatTimestampNow(), "", "system", "Topic for " ~ channel ~ ": " ~ topic);
        }
    }

    private void updateChannelList(string server, string channel, string action)
    {
        logToTerminal("Updating channel list: " ~ server ~ " -> " ~ channel ~ " " ~ action, "INFO", "main");

        if (action == "join")
        {
            addChannelToTree(server, channel);

            if (!(channel in displayBuffers))
            {
                displayBuffers[channel] = new TextBuffer(null);
                initializeTextTags(channel);
            }

            if (autoSwitchToNewChannels)
            {
                currentDisplay = channel;
                if (currentDisplay in displayBuffers)
                {
                    textBuffer = displayBuffers[currentDisplay];
                    textView.setBuffer(textBuffer);
                    scrollToEnd();
                }
            }

            if (!displayHistory.canFind(channel))
            {
                displayHistory ~= channel;
            }

            if (server in channelTopics && channel in channelTopics[server])
            {
                string topic = channelTopics[server][channel];
                appendChatMessage(channel, formatTimestampNow(), "", "system", "Topic: " ~ topic);
            }
        }
        else if (action == "part")
        {
            removeChannelFromTree(server, channel);

            size_t idx = -1;
            for (size_t i = 0; i < displayHistory.length; i++)
            {
                if (displayHistory[i] == channel)
                {
                    idx = i;
                    break;
                }
            }
            if (idx != -1)
            {
                displayHistory = displayHistory[0 .. idx] ~ displayHistory[idx + 1 .. $];
            }

            if (currentDisplay == channel)
            {
                if (displayHistory.length > 0)
                {
                    currentDisplay = displayHistory[$ - 1];
                }
                else
                {
                    currentDisplay = "System";
                }

                if (currentDisplay in displayBuffers)
                {
                    textBuffer = displayBuffers[currentDisplay];
                    textView.setBuffer(textBuffer);
                    scrollToEnd();
                }
            }

            if (server in channelTopics)
            {
                channelTopics[server].remove(channel);
            }
        }
    }

    void sendMessage()
    {
        auto text = inputEntry.getText();
        inputEntry.setText("");
        if (text.length == 0)
            return;

        logToTerminal("User input: " ~ text, "INFO", "main");

        if (currentServer.length == 0 || !(currentServer in serverThreads))
        {
            appendSystemMessage("Not connected to any server.");
            return;
        }

        if (text.length > 1 && text[0] == '/')
        {
            handleCommand(text);
            return;
        }

        if (currentDisplay == currentServer)
        {
            auto spacePos = text.indexOf(" ");
            if (spacePos != -1)
            {
                auto recipient = text[0 .. spacePos].strip();
                auto message = text[spacePos .. $].strip();
                send(serverThreads[currentServer], IrcFromGtkMessage(IrcFromGtkMessage.Type.Message, recipient, message, ""));
            }
            else
            {
                appendSystemMessage("Usage: nick message (for private messages)");
            }
        }
        else if (currentDisplay.startsWith("#"))
        {
            send(serverThreads[currentServer], IrcFromGtkMessage(IrcFromGtkMessage.Type.Message, currentDisplay, text, ""));
        }
        else
        {
            appendSystemMessage("Cannot send message to this tab.");
        }
    }

    private void handleCommand(string text)
    {
        if (text.startsWith("/connect "))
        {
            auto server = text["/connect ".length .. $].strip();
            startConnection(server);
        }
        else if (text.startsWith("/join "))
        {
            auto channel = text["/join ".length .. $].strip();
            if (!channel.startsWith("#"))
            {
                channel = "#" ~ channel;
            }
            appendSystemMessage("Joining " ~ channel);
            send(serverThreads[currentServer], IrcFromGtkMessage(IrcFromGtkMessage.Type.UpdateChannels, channel, "", "join"));
            updateChannelList(currentServer, channel, "join");
        }
        else if (text.startsWith("/part "))
        {
            auto channel = text["/part ".length .. $].strip();
            appendSystemMessage("Leaving " ~ channel);
            send(serverThreads[currentServer], IrcFromGtkMessage(IrcFromGtkMessage.Type.UpdateChannels, channel, "", "part"));
            updateChannelList(currentServer, channel, "part");
        }
        else if (text.startsWith("/whois "))
        {
            auto target = text["/whois ".length .. $].strip();
            if (currentServer.length > 0 && currentServer in serverThreads)
            {
                send(serverThreads[currentServer], IrcFromGtkMessage(IrcFromGtkMessage.Type.UpdateChannels, target, "", "whois"));
                appendChatMessage(currentDisplay, formatTimestampNow(), "", "system", "WHOIS request sent for " ~ target);
            }
            else
            {
                appendSystemMessage("Not connected to a server.");
            }
        }
        else if (text.startsWith("/disconnect"))
        {
            disconnectFromServer();
        }
        else if (text.startsWith("/quit"))
        {
            disconnectAllServers();
            appendSystemMessage("Goodbye!");
            app.quit();
        }
        else if (text.startsWith("/msg ") || text.startsWith("/query "))
        {
            auto rest = text["/msg ".length .. $].strip();
            auto spacePos = rest.indexOf(" ");
            if (spacePos != -1)
            {
                auto recipient = rest[0 .. spacePos].strip();
                auto message = rest[spacePos .. $].strip();
                send(serverThreads[currentServer], IrcFromGtkMessage(IrcFromGtkMessage.Type.Message, recipient, message, ""));
            }
            else
            {
                appendSystemMessage("Usage: /msg nick message");
            }
        }
        else if (text.startsWith("/me "))
        {
            if (currentDisplay.startsWith("#"))
            {
                auto action = text["/me ".length .. $];
                string actionMsg = "\x01ACTION " ~ action ~ "\x01";
                send(serverThreads[currentServer], IrcFromGtkMessage(IrcFromGtkMessage.Type.Message, currentDisplay, actionMsg, ""));
            }
            else
            {
                appendSystemMessage("/me can only be used in channels");
            }
        }
        else if (text.startsWith("/nick "))
        {
            auto newNick = text["/nick ".length .. $].strip();
            send(serverThreads[currentServer], IrcFromGtkMessage(IrcFromGtkMessage.Type.Message, "", "NICK " ~ newNick, ""));
            appendChatMessage(currentServer, formatTimestampNow(), defaultNick, "system", "Changing nickname to: " ~ newNick);
        }
        else if (text.startsWith("/help"))
        {
            appendSystemMessage("Available commands:");
            appendSystemMessage("  /connect <server> - Connect to an IRC server");
            appendSystemMessage("  /join <#channel> - Join a channel");
            appendSystemMessage("  /part [channel] - Leave current or specified channel");
            appendSystemMessage("  /whois <nickname> - Get user information");
            appendSystemMessage("  /msg <nick> <message> - Send private message");
            appendSystemMessage("  /me <action> - Send action to channel");
            appendSystemMessage("  /nick <newnick> - Change nickname");
            appendSystemMessage("  /disconnect - Disconnect from current server");
            appendSystemMessage("  /quit - Quit the application");
            appendSystemMessage("  /help - Show this help");
        }
        else
        {
            string rawCommand = text[1 .. $];
            send(serverThreads[currentServer], IrcFromGtkMessage(IrcFromGtkMessage.Type.Message, "", rawCommand, ""));

            if (currentDisplay.startsWith("#"))
            {
                appendChatMessage(currentDisplay, formatTimestampNow(), "", "system", ">>> " ~ rawCommand);
            }
            else
            {
                appendChatMessage(currentServer, formatTimestampNow(), "",
                        "system", ">>> " ~ rawCommand);
            }
        }
    }

    private string formatTimestampNow()
    {
        auto now = Clock.currTime();
        return "[" ~ format("%02d:%02d", now.hour, now.minute) ~ "]";
    }

    void appendSystemMessage(string message)
    {
        appendChatMessage("System", formatTimestampNow(), "", "system", message);
    }

    private void scrollToEnd()
    {
        if (textView && textBuffer)
        {
            TextIter scrollIter;
            textBuffer.getEndIter(scrollIter);
            textView.scrollToIter(scrollIter, 0.0, true, 0.0, 1.0);
        }
    }

    void appendChatMessage(string display, string timestamp, string nickname,
            string type, string message)
    {
        logToTerminal("Appending message to display " ~ display ~ ": " ~ nickname ~ " (" ~ type ~ "): " ~ message, "INFO", "main");

        if (display.length == 0)
        {
            logToTerminal("Warning: Empty display name for message", "ERROR", "main");
            return;
        }

        if (!(display in displayBuffers))
        {
            displayBuffers[display] = new TextBuffer(null);
            initializeTextTags(display);
        }

        TextBuffer targetBuffer = displayBuffers[display];
        TextIter insertIter;
        targetBuffer.getEndIter(insertIter);

        if (display in timestampTags)
        {
            insertWithTags(targetBuffer, insertIter, timestamp ~ " ", timestampTags[display]);
        }
        else
        {
            targetBuffer.insert(insertIter, timestamp ~ " ");
        }

        targetBuffer.getEndIter(insertIter);

        char modeSymbol = '\0';
        string baseNickname = nickname;

        if (nickname.length > 0 && (nickname[0] == '@' || nickname[0] == '+' || nickname[0] == '%' || nickname[0] == '&' || nickname[0] == '~'))
        {
            modeSymbol = nickname[0];
            baseNickname = nickname[1 .. $];
        }

        switch (type)
        {
        case "message":
            if (nickname.length > 0)
            {
                if (modeSymbol != '\0')
                {
                    if (auto modeTag = getModeSymbolTag(display, modeSymbol))
                    {
                        insertWithTags(targetBuffer, insertIter, [modeSymbol].idup, modeTag);
                    }
                    else
                    {
                        targetBuffer.insert(insertIter, [modeSymbol].idup);
                    }
                }

                if (colorizeNicks)
                {
                    string nickColor = getNickColor(baseNickname);
                    if (auto nickTag = getNicknameTag(display, baseNickname, nickColor))
                    {
                        insertWithTags(targetBuffer, insertIter, baseNickname, nickTag);
                    }
                    else
                    {
                        targetBuffer.insert(insertIter, baseNickname);
                    }
                }
                else
                {
                    targetBuffer.insert(insertIter, baseNickname);
                }

                targetBuffer.getEndIter(insertIter);
                targetBuffer.insert(insertIter, ": " ~ message ~ "\n");
            }
            else
            {
                targetBuffer.insert(insertIter, message ~ "\n");
            }
            break;

        case "action":
            targetBuffer.insert(insertIter, "* ");
            targetBuffer.getEndIter(insertIter);

            if (nickname.length > 0)
            {
                if (modeSymbol != '\0')
                {
                    if (auto modeTag = getModeSymbolTag(display, modeSymbol))
                    {
                        insertWithTags(targetBuffer, insertIter, [modeSymbol].idup, modeTag);
                    }
                    else
                    {
                        targetBuffer.insert(insertIter, [modeSymbol].idup);
                    }
                }

                if (colorizeNicks)
                {
                    string nickColor = getNickColor(baseNickname);
                    if (auto nickTag = getNicknameTag(display, baseNickname, nickColor))
                    {
                        insertWithTags(targetBuffer, insertIter, baseNickname, nickTag);
                    }
                    else
                    {
                        targetBuffer.insert(insertIter, baseNickname);
                    }
                }
                else
                {
                    targetBuffer.insert(insertIter, baseNickname);
                }

                targetBuffer.getEndIter(insertIter);
                targetBuffer.insert(insertIter, " " ~ message ~ "\n");
            }
            else
            {
                targetBuffer.insert(insertIter, " " ~ message ~ "\n");
            }
            break;
        case "notice":
            targetBuffer.insert(insertIter, "-");
            targetBuffer.getEndIter(insertIter);

            if (nickname.length > 0)
            {
                if (modeSymbol != '\0')
                {
                    if (auto modeTag = getModeSymbolTag(display, modeSymbol))
                    {
                        insertWithTags(targetBuffer, insertIter, [modeSymbol].idup, modeTag);
                    }
                    else
                    {
                        targetBuffer.insert(insertIter, [modeSymbol].idup);
                    }
                }

                if (colorizeNicks)
                {
                    string nickColor = getNickColor(baseNickname);
                    if (auto nickTag = getNicknameTag(display, baseNickname, nickColor))
                    {
                        insertWithTags(targetBuffer, insertIter, baseNickname, nickTag);
                    }
                    else
                    {
                        targetBuffer.insert(insertIter, baseNickname);
                    }
                }
                else
                {
                    targetBuffer.insert(insertIter, baseNickname);
                }
                targetBuffer.getEndIter(insertIter);
                targetBuffer.insert(insertIter, "- " ~ message ~ "\n");
            }
            else
            {
                targetBuffer.insert(insertIter, "- " ~ message ~ "\n");
            }
            break;

        case "join":
        case "part":
        case "quit":
        case "kick":
        case "nick":
            if (nickname.length > 0)
            {
                if (modeSymbol != '\0')
                {
                    if (auto modeTag = getModeSymbolTag(display, modeSymbol))
                    {
                        insertWithTags(targetBuffer, insertIter, [modeSymbol].idup, modeTag);
                    }
                    else
                    {
                        targetBuffer.insert(insertIter, [modeSymbol].idup);
                    }
                }

                if (colorizeNicks)
                {
                    string nickColor = getNickColor(baseNickname);
                    if (auto nickTag = getNicknameTag(display, baseNickname, nickColor))
                    {
                        insertWithTags(targetBuffer, insertIter, baseNickname, nickTag);
                    }
                    else
                    {
                        targetBuffer.insert(insertIter, baseNickname);
                    }
                }
                else
                {
                    targetBuffer.insert(insertIter, baseNickname);
                }

                targetBuffer.getEndIter(insertIter);
                targetBuffer.insert(insertIter, " " ~ message ~ "\n");
            }
            else
            {
                targetBuffer.insert(insertIter, message ~ "\n");
            }
            break;

        case "system":
            if (display in systemMessageTags)
            {
                targetBuffer.getEndIter(insertIter);
                insertWithTags(targetBuffer, insertIter, message ~ "\n", systemMessageTags[display]);
            }
            else
            {
                targetBuffer.getEndIter(insertIter);
                targetBuffer.insert(insertIter, message ~ "\n");
            }
            break;

        default:
            if (nickname.length > 0)
            {
                if (colorizeNicks)
                {
                    string nickColor = getNickColor(baseNickname);
                    if (auto nickTag = getNicknameTag(display, baseNickname, nickColor))
                    {
                        insertWithTags(targetBuffer, insertIter, baseNickname, nickTag);
                    }
                    else
                    {
                        targetBuffer.insert(insertIter, baseNickname);
                    }
                }
                else
                {
                    targetBuffer.insert(insertIter, baseNickname);
                }
                targetBuffer.getEndIter(insertIter);
                targetBuffer.insert(insertIter, ": " ~ message ~ "\n");
            }
            else
            {
                targetBuffer.getEndIter(insertIter);
                targetBuffer.insert(insertIter, message ~ "\n");
            }
            break;
        }

        if (display == currentDisplay)
        {
            textBuffer = targetBuffer;
            textView.setBuffer(textBuffer);
            scrollToEnd();
        }
    }

    void run()
    {
        app.run([]);
    }
}
