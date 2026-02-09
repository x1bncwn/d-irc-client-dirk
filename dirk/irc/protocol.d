module irc.protocol;

import irc.exception;

import std.algorithm;
import std.array;
import std.exception;
import std.string;
import std.typetuple : staticIndexOf, TypeTuple;

//debug=Dirk;
debug (Dirk) static import std.stdio;
debug (Dirk) import std.conv;

@safe:

enum IRC_MAX_LEN = 512;

/*
 * 63 is the maximum hostname length defined by the protocol.  10 is a common
 * username limit on many networks.  1 is for the `@'.
 * Shamelessly stolen from IRSSI, irc/core/irc-servers.c
 */
enum MAX_USERHOST_LEN = 63 + 10 + 1;
private enum AdditionalMsgLens
{
    PRIVMSG = MAX_USERHOST_LEN,
    NOTICE = MAX_USERHOST_LEN
}

/*
 * Returns the additional length requirements of a method.
 *
 * Params:
 *      method = The method to query.
 * Returns:
 *      The additional length requirements.
 */
template additionalMsgLen(string method)
{
    static if(staticIndexOf!(method, __traits(allMembers, AdditionalMsgLens)) == -1)
        enum additionalMsgLen = 0;
    else
        enum additionalMsgLen = __traits(getMember, AdditionalMsgLens, method);
}

unittest
{
    static assert(additionalMsgLen!"PRIVMSG" == MAX_USERHOST_LEN);
    static assert(additionalMsgLen!"NOTICE" == MAX_USERHOST_LEN);
    static assert(additionalMsgLen!"JOIN" == 0);
}

enum IRC_MAX_COMMAND_PARAMETERS = 15; // RFC2812

/**
 * Structure representing a parsed IRC message.
 */
struct IrcLine
{
    /// Note: null when the message has no _prefix.
    const(char)[] prefix; // Optional
    ///
    const(char)[] command;
    ///
    const(char)[][] arguments() @property pure nothrow @nogc return
    {
        return argumentBuffer[0 .. numArguments];
    }

    private const(char)[][IRC_MAX_COMMAND_PARAMETERS] argumentBuffer;
    private size_t numArguments;
}

/// List of the four valid channel prefixes;
/// &, #, + and !.
alias channelPrefixes = TypeTuple!('&', '#', '+', '!');

// Helper function to skip whitespace
private void skipSpaces(ref const(char)[] s) pure @nogc
{
    while (s.length > 0 && s[0] == ' ')
        s = s[1 .. $];
}

// Helper function to get next token (until space or end)
private const(char)[] getToken(ref const(char)[] s) pure @nogc
{
    size_t i = 0;
    while (i < s.length && s[i] != ' ')
        i++;

    auto token = s[0 .. i];
    s = s[i .. $];
    return token;
}

// [:prefix] <command> <parameters ...> [:long parameter]
// TODO: do something about the allocation of the argument array
bool parse(const(char)[] raw, out IrcLine line) pure @nogc
{
    debug (Dirk) std.stdio.writeln("parse called with: \"" ~ raw ~ "\"");

    auto working = raw;

    if (working.length > 0 && working[0] == ':')
    {
        debug (Dirk) std.stdio.writeln("  Has prefix");
        working = working[1 .. $];
        auto prefixEnd = working.indexOf(' ');
        if (prefixEnd == -1)
        {
            debug (Dirk) std.stdio.writeln("  No space after prefix, parse failed");
            return false;
        }

        line.prefix = working[0 .. prefixEnd];
        debug (Dirk) std.stdio.writeln("  Prefix: \"" ~ line.prefix ~ "\"");
        working = working[prefixEnd .. $];
    }
    else
    {
        debug (Dirk) std.stdio.writeln("  No prefix");
        line.prefix = null;
    }

    skipSpaces(working);

    // Get command
    line.command = getToken(working);
    if (line.command.length == 0)
    {
        debug (Dirk) std.stdio.writeln("  Empty command, parse failed");
        return false;
    }

    debug (Dirk) std.stdio.writeln("  Command: \"" ~ line.command ~ "\"");

    skipSpaces(working);

    line.numArguments = 0;

    // Parse arguments
    debug (Dirk) std.stdio.writeln("  Parsing arguments, remaining: \"" ~ working ~ "\"");
    while (working.length > 0)
    {
        if (working[0] == ':')
        {
            // Last argument (can contain spaces)
            working = working[1 .. $];
            assert(line.numArguments < line.argumentBuffer.length);
            line.argumentBuffer[line.numArguments++] = working;
            debug (Dirk) std.stdio.writeln("  Final argument [" ~ (line.numArguments-1).to!string ~ "]: \"" ~ working ~ "\"");
            working = null;
            break;
        }
        else
        {
            // Regular argument
            auto arg = getToken(working);
            assert(line.numArguments < line.argumentBuffer.length);
            line.argumentBuffer[line.numArguments++] = arg;
            debug (Dirk) std.stdio.writeln("  Argument [" ~ (line.numArguments-1).to!string ~ "]: \"" ~ arg ~ "\"");

            skipSpaces(working);
        }
    }

    debug (Dirk) std.stdio.writeln("  Parse successful, total arguments: " ~ line.numArguments.to!string);
    return true;
}

version(unittest)
{
    import std.stdio;
}

unittest
{
    struct InputOutput
    {
        string input;

        struct Output
        {
            string prefix, command;
            string[] arguments;
        }
        Output output;

        bool valid = true;
    }

    static InputOutput[] testData = [
        {
            input: "PING 123456",
            output: {command: "PING", arguments: ["123456"]}
        },
        {
            input: ":foo!bar@baz PRIVMSG #channel hi!",
            output: {prefix: "foo!bar@baz", command: "PRIVMSG", arguments: ["#channel", "hi!"]}
        },
        {
            input: ":foo!bar@baz PRIVMSG #channel :hello, world!",
            output: {prefix: "foo!bar@baz", command: "PRIVMSG", arguments: ["#channel", "hello, world!"]}
        },
        {
            input: ":foo!bar@baz 005 testnick CHANLIMIT=#:120 :are supported by this server",
            output: {prefix: "foo!bar@baz", command: "005", arguments: ["testnick", "CHANLIMIT=#:120", "are supported by this server"]}
        },
        {
            input: ":nick!~ident@00:00:00:00::00 PRIVMSG #some.channel :some message",
            output: {prefix: "nick!~ident@00:00:00:00::00", command: "PRIVMSG", arguments: ["#some.channel", "some message"]}
        },
        {
            input: ":foo!bar@baz JOIN :#channel",
            output: {prefix: "foo!bar@baz", command: "JOIN", arguments: ["#channel"]}
        }
    ];

    foreach(i, test; testData)
    {
        IrcLine line;
        bool succ = parse(test.input, line);

        scope(failure)
        {
            writefln("irc.protocol.parse unittest failed, test #%s", i + 1);
            writefln(`prefix: "%s"`, line.prefix);
            writefln(`command: "%s"`, line.command);
            writefln(`arguments: "%s"`, line.arguments);
        }

        if(test.valid)
        {
            assert(line.prefix == test.output.prefix);
            assert(line.command == test.output.command);
            assert(line.arguments == test.output.arguments);
        }
        else
            assert(!succ);
    }
}

/**
 * Structure representing an IRC user.
 */
struct IrcUser
{
    ///
    const(char)[] nickName;
    ///
    const(char)[] userName;
    ///
    const(char)[] hostName;

    deprecated alias nick = nickName;

    // TODO: Change to use sink once formattedWrite supports them
    version(none) string toString() const
    {
        return format("%s!%s@%s", nickName, userName, hostName);
    }

    void toString(scope void delegate(const(char)[]) @safe sink) const
    {
        if(nickName)
            sink(nickName);

        if(userName)
        {
            sink("!");
            sink(userName);
        }

        if(hostName)
        {
            sink("@");
            sink(hostName);
        }
    }

    unittest
    {
        auto user = IrcUser("nick", "user", "host");
        assert(format("%s", user) == "nick!user@host");
        user.hostName = null;
        assert(format("%s", user) == "nick!user");

        user.userName = null;
        assert(format("%s", user) == "nick");

        user.hostName = "host";
        assert(format("%s", user) == "nick@host");
    }

    static:
    /**
     * Create an IRC user from a message prefix.
     */
    IrcUser fromPrefix(const(char)[] prefix)
    {
        debug (Dirk) std.stdio.writeln("fromPrefix called with: \"" ~ prefix ~ "\"");

        IrcUser user;

        if (prefix.length > 0)
        {
            // Parse nick!user@host format
            auto exclamation = prefix.indexOf('!');
            if (exclamation != -1)
            {
                user.nickName = prefix[0 .. exclamation];
                debug (Dirk) std.stdio.writeln("  Nick: \"" ~ user.nickName ~ "\"");

                auto at = prefix.indexOf('@', exclamation + 1);
                if (at != -1)
                {
                    user.userName = prefix[exclamation + 1 .. at];
                    user.hostName = prefix[at + 1 .. $];
                    debug (Dirk) std.stdio.writeln("  User: \"" ~ user.userName ~ "\", Host: \"" ~ user.hostName ~ "\"");
                }
                else
                {
                    user.userName = prefix[exclamation + 1 .. $];
                    debug (Dirk) std.stdio.writeln("  User: \"" ~ user.userName ~ "\", No host");
                }
            }
            else
            {
                // No '!' - could be just nick or server name
                user.nickName = prefix;
                debug (Dirk) std.stdio.writeln("  Only nick: \"" ~ user.nickName ~ "\"");
            }
        }
        else
        {
            debug (Dirk) std.stdio.writeln("  Empty prefix");
        }

        return user;
    }

    /**
     * Create users from userhost reply.
     */
    size_t parseUserhostReply(ref IrcUser[5] users, in char[] reply)
    {
        debug (Dirk) std.stdio.writeln("parseUserhostReply called with: \"" ~ reply ~ "\"");

        auto tokens = reply.split();
        size_t i = 0;

        for (; i < 5 && i < tokens.length; i++)
        {
            auto token = tokens[i];
            debug (Dirk) std.stdio.writeln("  Processing token: \"" ~ token ~ "\"");

            auto equalsPos = token.indexOf('=');
            if (equalsPos == -1)
            {
                debug (Dirk) std.stdio.writeln("  No '=' found, stopping");
                break;
            }

            users[i].nickName = token[0 .. equalsPos];
            debug (Dirk) std.stdio.writeln("    Nick: \"" ~ users[i].nickName ~ "\"");

            auto rest = token[equalsPos + 1 .. $];
            auto atPos = rest.indexOf('@');

            if (atPos != -1)
            {
                users[i].userName = rest[0 .. atPos];
                users[i].hostName = rest[atPos + 1 .. $];
                debug (Dirk) std.stdio.writeln("    User: \"" ~ users[i].userName ~ "\", Host: \"" ~ users[i].hostName ~ "\"");
            }
            else
            {
                users[i].userName = rest;
                debug (Dirk) std.stdio.writeln("    User: \"" ~ users[i].userName ~ "\", No host");
            }
        }

        debug (Dirk) std.stdio.writeln("  Processed " ~ i.to!string ~ " users");
        return i;
    }
}

unittest
{
    IrcUser user;

    user = IrcUser.fromPrefix("foo!bar@baz");
    assert(user.nickName == "foo");
    assert(user.userName == "bar");
    assert(user.hostName == "baz");

    // TODO: figure out which field to fill with prefixes like "irc.server.net"
}

