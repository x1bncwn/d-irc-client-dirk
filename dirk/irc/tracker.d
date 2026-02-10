// TODO: more accurate/atomic tracking starting procedure
module irc.tracker;

import irc.client;
import irc.util : ExceptionConstructor;

import std.algorithm : canFind, countUntil, remove, map;
import std.exception : enforce;
import std.traits : Unqual;
import std.typetuple : TypeTuple;

///
class IrcTrackingException : Exception
{
	mixin ExceptionConstructor!();
}

// TODO: Add example
/**
 * Create a new channel and user tracking object for the given
 * $(DPREF _client, IrcClient). Tracking for the new object
 * is initially disabled; use $(MREF IrcTracker.start) to commence tracking.
 *
 * Params:
 *   Payload = type of extra storage per $(MREF TrackedUser) object
 * See_Also:
 *    $(MREF IrcTracker), $(MREF TrackedUser.payload)
 */
CustomIrcTracker!Payload track(Payload = void)(IrcClient client)
{
	return new typeof(return)(client);
}

/**
 * Keeps track of all channels and channel members
 * visible to the associated $(DPREF client, IrcClient) connection.
 *
 * Params:
 *   Payload = type of extra storage per $(MREF TrackedUser) object
 * See_Also:
 *   $(MREF CustomTrackedUser.payload)
 */
class CustomIrcTracker(Payload = void)
{
	private:
	IrcClient _client;
	CustomTrackedChannel!Payload[string] _channels;
	CustomTrackedUser!Payload*[string] _users;
	CustomTrackedUser!Payload* thisUser;

	enum State { disabled, starting, enabled }
	auto _isTracking = State.disabled;

	debug(IrcTracker) import std.stdio;

	final:
	debug(IrcTracker) void checkIntegrity()
	{
		import std.algorithm;

		if(!isTracking)
		{
			assert(channels.empty);
			assert(_channels is null);
			assert(_users is null);
			return;
		}

		foreach(channel; channels)
		{
			assert(channel.name.length != 0);
			assert(channel.users.length != 0);
			foreach(member; channel.users)
			{
				auto user = findUser(member.nickName);
				assert(user);
				assert(user == member);
			}
		}

		assert(thisUser == _users[thisUser.nickName]);

		foreach(user; users)
			if(user != thisUser)
				assert(channels.map!(chan => chan.users).joiner().canFind(user), "unable to find " ~ user.nickName ~ " in any channels");
	}

	void onConnect()
	{
		thisUser.nickName = _client.nickName;
		thisUser.userName = _client.userName;
		thisUser.realName = _client.realName;

		debug(IrcTracker) writeln("tracker connected; thisUser = ", *thisUser);
	}

	void onSuccessfulJoin(in char[] channelName)
	{
		debug(IrcTracker)
		{
			writeln("onmejoin: ", channelName);
			checkIntegrity();
		}

		auto channel = CustomTrackedChannel!Payload(channelName.idup);
		channel._users = [_client.nickName: thisUser];
		_channels[channel.name] = channel;

		debug(IrcTracker)
		{
			write("checking... ");
			checkIntegrity();
			writeln("done.");
		}
	}

	void onNameList(in char[] channelName, in char[][] nickNames)
	{
		debug(IrcTracker)
		{
			writefln("names %s: %(%s%|, %)", channelName, nickNames);
			checkIntegrity();
		}

		auto channel = _channels[channelName];

		// Get prefix modes from client's ISUPPORT data
		auto prefixedModes = _client.getPrefixedChannelModes();

		foreach(nickName; nickNames)
		{
			// Strip prefix from NAMES using client's ISUPPORT data
			string nick = nickName.idup;
			char prefix = '\0';
			char mode = '\0';

			if (nickName.length > 0) {
				char first = nickName[0];
				// Check if this character is a prefix according to ISUPPORT
				foreach(pair; prefixedModes) {
					if (pair[0] == first) {  // pair[0] is prefix, pair[1] is mode
						prefix = first;
						mode = pair[1];
						nick = nickName[1..$].idup;
						break;
					}
				}
			}

			if(auto pUser = nick in _users)
			{
				auto user = *pUser;
				user.channels ~= channel.name;
				channel._users[cast(immutable)nick] = user;
				// Apply prefix from NAMES if present
				if (prefix != '\0') {
					user.addPrefixWithMode(channelName.idup, prefix, mode);
				}
			}
			else
			{
				auto immNick = nick.idup;
				auto user = new CustomTrackedUser!Payload(immNick);
				user.channels = [channel.name];
				// Apply prefix from NAMES if present
				if (prefix != '\0') {
					user.addPrefixWithMode(channelName.idup, prefix, mode);
				}
				channel._users[immNick] = user;
				_users[immNick] = user;
			}
		}

		debug(IrcTracker)
		{
			import std.algorithm : map;
			writeln(channel._users.values.map!(user => *user));
			write("checking... ");
			checkIntegrity();
			writeln("done.");
		}
	}

	void onJoin(IrcUser user, in char[] channelName)
	{
		debug(IrcTracker)
		{
			writefln("%s joined %s", user.nickName, channelName);
			checkIntegrity();
		}

		auto channel = _channels[channelName];

		if(auto pUser = user.nickName in _users)
		{
			auto storedUser = *pUser;
			if(!storedUser.userName)
				storedUser.userName = user.userName.idup;
			if(!storedUser.hostName)
				storedUser.hostName = user.hostName.idup;

			storedUser.channels ~= channel.name;
			channel._users[user.nickName] = storedUser;
		}
		else
		{
			auto immNick = user.nickName.idup;

			auto newUser = new CustomTrackedUser!Payload(immNick);
			newUser.userName = user.userName.idup;
			newUser.hostName = user.hostName.idup;
			newUser.channels = [channel.name];

			_users[immNick] = newUser;
			channel._users[immNick] = newUser;
		}

		debug(IrcTracker)
		{
			write("checking... ");
			checkIntegrity();
			writeln("done.");
		}
	}

	// Utility function
	void onMeLeave(in char[] channelName)
	{
		import std.algorithm : countUntil, remove, SwapStrategy;

		debug(IrcTracker)
		{
			writeln("onmeleave: ", channelName);
			checkIntegrity();
		}

		auto channel = _channels[channelName];

		foreach(ref user; channel._users)
		{
			auto channelIndex = user.channels.countUntil(channelName);
			assert(channelIndex != -1);
			user.channels = user.channels.remove!(SwapStrategy.unstable)(channelIndex);
			// Remove prefixes for this channel
			user.removeAllPrefixes(channelName.idup);
			if(user.channels.length == 0 && user.nickName != _client.nickName)
				_users.remove(cast(immutable)user.nickName);
		}

		_channels.remove(channel.name);

		debug(IrcTracker)
		{
			write("checking... ");
			checkIntegrity();
			writeln("done.");
		}
	}

	// Utility function
	void onLeave(in char[] channelName, in char[] nick)
	{
		import std.algorithm : countUntil, remove, SwapStrategy;

		debug(IrcTracker)
		{
			writefln("%s left %s", nick, channelName);
			checkIntegrity();
		}

		_channels[channelName]._users.remove(cast(immutable)nick);

		auto pUser = nick in _users;
		auto user = *pUser;
		auto channelIndex = user.channels.countUntil(channelName);
		assert(channelIndex != -1);
		user.channels = user.channels.remove!(SwapStrategy.unstable)(channelIndex);
		// Remove prefixes for this channel
		user.removeAllPrefixes(channelName.idup);
		if(user.channels.length == 0)
			_users.remove(cast(immutable)nick);

		debug(IrcTracker)
		{
			write("checking... ");
			checkIntegrity();
			writeln("done.");
		}
	}

	void onPart(IrcUser user, in char[] channelName)
	{
		if(user.nickName == _client.nickName)
			onMeLeave(channelName);
		else
			onLeave(channelName, user.nickName);
	}

	void onKick(IrcUser kicker, in char[] channelName, in char[] nick, in char[] comment)
	{
		debug(IrcTracker) writefln(`%s kicked %s: %s`, kicker.nickName, nick, comment);
		if(nick == _client.nickName)
			onMeLeave(channelName);
		else
			onLeave(channelName, nick);
	}

	void onQuit(IrcUser user, in char[] comment)
	{
		debug(IrcTracker)
		{
			writefln("%s quit", user.nickName);
			checkIntegrity();
		}

		foreach(channelName; _users[user.nickName].channels)
		{
			debug(IrcTracker) writefln("%s left %s by quitting", user.nickName, channelName);
			_channels[channelName]._users.remove(cast(immutable)user.nickName);
		}

		_users.remove(cast(immutable)user.nickName);

		debug(IrcTracker)
		{
			write("checking... ");
			checkIntegrity();
			writeln("done.");
		}
	}

	void onNickChange(IrcUser user, in char[] newNick)
	{
		debug(IrcTracker)
		{
			writefln("%s changed nick to %s", user.nickName, newNick);
			checkIntegrity();
		}

		auto userInSet = _users[user.nickName];
		_users.remove(cast(immutable)user.nickName);

		auto immNewNick = newNick.idup;
		userInSet.nickName = immNewNick;
		_users[immNewNick] = userInSet;

		debug(IrcTracker)
		{
			write("checking... ");
			checkIntegrity();
			writeln("done.");
		}
	}

	// Mode tracking: Mode change handler
	private void onMode(string channel, string modeStr, string[] params)
	{
		if (channel.length == 0 || channel[0] != '#') return;

		// Get prefix mapping from client
		auto prefixedModes = _client.getPrefixedChannelModes();
		// Create reverse mapping: mode -> prefix
		char[char] modeToPrefix;
		foreach(pair; prefixedModes) {
			modeToPrefix[pair[1]] = pair[0];  // pair[1] is mode, pair[0] is prefix
		}

		size_t paramIdx = 0;
		bool adding = true;

		foreach (char c; modeStr) {
			if (c == '+') { adding = true; continue; }
			if (c == '-') { adding = false; continue; }

			// Check if this is a prefix mode using client's ISUPPORT data
			if (c in modeToPrefix) {
				char prefix = modeToPrefix[c];
				// Get target user
				string target;
				if (paramIdx < params.length) {
					target = params[paramIdx];
					paramIdx++;
				} else {
					// No more parameters, use last one
					if (params.length > 0) {
						target = params[params.length - 1];
					} else {
						continue;
					}
				}

				if (auto chan = channel in _channels) {
					if (auto userPtr = target in chan._users) {
						auto user = *userPtr;
						if (adding) {
							user.addPrefixWithMode(channel, prefix, c);
						} else {
							user.removePrefix(channel, prefix);
						}
					}
				}
			} else {
				// Non-prefix mode - advance parameter index
				if (paramIdx < params.length) {
					paramIdx++;
				}
			}
		}
	}

	// Mode Tracking: Wrapper for IrcClient's onModeChange event
	void onModeChange(in char[] channel, in char[] modeStr, const(char)[][] params)
	{
		// Convert params to string[]
		string[] stringParams = new string[params.length];
		foreach(i, param; params) {
			stringParams[i] = param.idup;
		}
		onMode(channel.idup, modeStr.idup, stringParams);
	}

	alias eventHandlers = TypeTuple!(onConnect, onSuccessfulJoin, onNameList, onJoin, onPart, onKick, onQuit, onNickChange, onModeChange);

	// Start tracking functions
	void onMyChannelsReply(in char[] nick, in char[][] channels)
	{
		if(nick != _client.nickName)
			return;

		_client.onWhoisChannelsReply.unsubscribeHandler(&onMyChannelsReply);
		_client.onWhoisEnd.unsubscribeHandler(&onWhoisEnd);

		if(_isTracking != State.starting)
			return;

		startNow();

		foreach(channel; channels)
			onSuccessfulJoin(channel);

		_client.queryNames(channels);
	}

	void onWhoisEnd(in char[] nick)
	{
		if(nick != _client.nickName)
			return;

		// Weren't in any channels.
		_client.onWhoisChannelsReply.unsubscribeHandler(&onMyChannelsReply);
		_client.onWhoisEnd.unsubscribeHandler(&onWhoisEnd);

		if(_isTracking != State.starting)
			return;

		startNow();
	}

	private void startNow()
	{
		assert(_isTracking != State.enabled);

		foreach(handler; eventHandlers)
			mixin("_client." ~ __traits(identifier, handler)) ~= &handler;

		auto thisNick = _client.nickName;
		thisUser = new CustomTrackedUser!Payload(thisNick);
		thisUser.userName = _client.userName;
		thisUser.realName = _client.realName;
		_users[thisNick] = thisUser;

		_isTracking = State.enabled;
	}

	public:
	this(IrcClient client)
	{
		this._client = client;
	}

	~this()
	{
		stop();
	}

	/**
	 * Initiate or restart tracking, or do nothing if the tracker is already tracking.
	 *
	 * If the associated client is unconnected, tracking starts immediately.
	 * If it is connected, information about the client's current channels will be queried,
	 * and tracking starts as soon as the information has been received.
	 */
	void start()
	{
		if(_isTracking != State.disabled)
			return;

		if(_client.connected)
		{
			_client.onWhoisChannelsReply ~= &onMyChannelsReply;
			_client.onWhoisEnd ~= &onWhoisEnd;
			_client.queryWhois(_client.nickName);
			_isTracking = State.starting;
		}
		else
			startNow();
	}

	/**
	 * Stop tracking, or do nothing if the tracker is not currently tracking.
	 */
	void stop()
	{
		final switch(_isTracking)
		{
			case State.enabled:
				_users = null;
				thisUser = null;
				_channels = null;
				foreach(handler; eventHandlers)
					mixin("_client." ~ __traits(identifier, handler)).unsubscribeHandler(&handler);
				break;
			case State.starting:
				_client.onWhoisChannelsReply.unsubscribeHandler(&onMyChannelsReply);
				_client.onWhoisEnd.unsubscribeHandler(&onWhoisEnd);
				break;
			case State.disabled:
				return;
		}

		_isTracking = State.disabled;
	}

	/// Boolean whether or not the tracker is currently tracking.
	bool isTracking() const @property @safe pure nothrow
	{
		return _isTracking == State.enabled;
	}

	/// $(DPREF _client, IrcClient) that this tracker is tracking for.
	inout(IrcClient) client() inout @property @safe pure nothrow
	{
		return _client;
	}

	/**
	 * $(D InputRange) (with $(D length)) of all _channels the associated client is currently
	 * a member of.
	 * Throws:
	 *    $(MREF IrcTrackingException) if the tracker is disabled or not yet ready
	 */
	auto channels() @property
	{
		import std.range : takeExactly;
		enforce(_isTracking, "not currently tracking");
		return _channels.values.takeExactly(_channels.length);
	}

	unittest
	{
		import std.range;
		static assert(isInputRange!(typeof(CustomIrcTracker.init.channels)));
		static assert(is(ElementType!(typeof(CustomIrcTracker.init.channels)) : CustomTrackedChannel!Payload));
		static assert(hasLength!(typeof(CustomIrcTracker.init.channels)));
	}

	/**
	 * $(D InputRange) (with $(D length)) of all _users currently seen by the associated client.
	 *
	 * The range includes the user for the associated client. Users that are not a member of any
	 * of the channels the associated client is a member of, but have sent a private message to
	 * the associated client, are $(I not) included.
	 * Throws:
	 *    $(MREF IrcTrackingException) if the tracker is disabled or not yet ready
	 */
	auto users() @property
	{
		import std.algorithm : map;
		import std.range : takeExactly;
		enforce(_isTracking, "not currently tracking");
		return _users.values.takeExactly(_users.length);
	}

	unittest
	{
		import std.range;
		static assert(isInputRange!(typeof(CustomIrcTracker.init.users)));
		static assert(is(ElementType!(typeof(CustomIrcTracker.init.users)) == CustomTrackedUser!Payload*));
		static assert(hasLength!(typeof(CustomIrcTracker.init.users)));
	}

	/**
	 * Lookup a channel on this tracker by name.
	 *
	 * The channel name must include the channel name prefix. Returns $(D null)
	 * if the associated client is not currently a member of the given channel.
	 * Params:
	 *    channelName = name of channel to lookup
	 * Throws:
	 *    $(MREF IrcTrackingException) if the tracker is disabled or not yet ready
	 * See_Also:
	 *    $(MREF TrackedChannel)
	 */
	CustomTrackedChannel!Payload* findChannel(in char[] channelName)
	{
		enforce(_isTracking, "not currently tracking");
		return channelName in _channels;
	}

	/**
	 * Lookup a user on this tracker by nick name.
	 *
	 * Users are searched among the members of all channels the associated
	 * client is currently a member of. The set includes the user for the
	 * associated client.
	 * Params:
	 *    nickName = nick name of user to lookup
	 * Throws:
	 *    $(MREF IrcTrackingException) if the tracker is disabled or not yet ready
	 * See_Also:
	 *    $(MREF TrackedUser)
	 */
	CustomTrackedUser!Payload* findUser(in char[] nickName)
	{
		enforce(_isTracking, "not currently tracking");
		if(auto user = nickName in _users)
			return *user;
		else
			return null;
	}
}

/// Ditto
alias IrcTracker = CustomIrcTracker!void;

/**
 * Represents an IRC channel and its member users for use by $(MREF IrcTracker).
 *
 * The list of members includes the user associated with the tracking object.
 * If the $(D IrcTracker) used to access an instance of this type
 * was since stopped, the channel presents the list of members as it were
 * at the time of the tracker being stopped.
 *
 * Params:
 *   Payload = type of extra storage per $(MREF TrackedUser) object
 * See_Also:
 *   $(MREF CustomTrackedUser.payload)
 */
struct CustomTrackedChannel(Payload = void)
{
	private:
	string _name;
	CustomTrackedUser!Payload*[string] _users;

	this(string name, CustomTrackedUser!Payload*[string] users = null)
	{
		_name = name;
		_users = users;
	}

	public:
	@disable this();

	/// Name of the channel, including the channel prefix.
	string name() @property
	{
		return _name;
	}

	/// $(D InputRange) of all member _users of this channel,
	/// where each user is given as a $(D (MREF TrackedUser)*).
	auto users() @property
	{
		import std.range : takeExactly;
		return _users.values.takeExactly(_users.length);
	}

	/**
	 * Lookup a member of this channel by nick name.
	 * $(D null) is returned if the given nick is not a member
	 * of this channel.
	 * Params:
	 *   nick = nick name of member to lookup
	 */
	CustomTrackedUser!Payload* opBinary(string op : "in")(in char[] nick)
	{
		enforce(cast(bool)this, "the TrackedChannel is invalid");
		if(auto pUser = nick in _users)
			return *pUser;
		else
			return null;
	}

	static if(!is(Payload == void))
	{
		TrackedChannel erasePayload() @property
		{
			return TrackedChannel(_name, cast(TrackedUser*[string])_users);
		}

		alias erasePayload this;
	}
}

/// Ditto
alias TrackedChannel = CustomTrackedChannel!void;

/**
 * Represents an IRC user for use by $(MREF IrcTracker).
 */
struct TrackedUser
{
	private:
	this(string nickName)
	{
		this.nickName = nickName;
	}

	// Mode Tracking: Mode tracking per channel
	// Now stores both prefix and mode char
	private struct PrefixInfo
	{
		char prefix;
		char mode;
	}
	private PrefixInfo[][string] _channelPrefixes; // channel -> array of prefix info

	public:
	@disable this();

	/**
	 * Nick name, user name and host name of the _user.
	 *
	 * $(D TrackedUser) is a super-type of $(DPREF protocol, IrcUser).
	 *
	 * Only the nick name is guaranteed to be non-null.
	 * See_Also:
	 *   $(DPREF protocol, IrcUser)
	 */
	string nickName;
	string userName;
	string hostName;

	/**
	 * Real name of the user. Is $(D null) unless a whois-query
	 * has been successfully issued for the user.
	 *
	 * See_Also:
	 *   $(DPREF client, IrcClient.queryWhois)
	 */
	string realName;

	/**
	 * Channels in which both the current user and the tracked
	 * user share membership.
	 *
	 * See_Also:
	 * $(DPREF client, IrcClient.queryWhois) to query channels
	 * a user is in, regardless of shared membership with the current user.
	 */
	string[] channels;

	// Mode Tracking: Mode tracking methods

	/// Get highest priority prefix for display in a channel
	char getHighestPrefix(string channel) const
	{
		if (auto prefixesPtr = channel in _channelPrefixes) {
			char highest = '\0';
			int highestPrio = 0;
			foreach (info; *prefixesPtr) {
				// Simple priority: @ > % > + (based on typical IRC)
				int prio = 0;
				switch(info.prefix) {
					case '@': prio = 3; break;
					case '%': prio = 2; break;
					case '+': prio = 1; break;
					case '&': prio = 4; break;
					case '~': prio = 5; break;
					default: prio = 0; break;
				}
				if (prio > highestPrio) {
					highest = info.prefix;
					highestPrio = prio;
				}
			}
			return highest;
		}
		return '\0';
	}

	/// Get mode char for a specific prefix in channel
	char getModeForPrefix(string channel, char prefix) const
	{
		if (auto prefixesPtr = channel in _channelPrefixes) {
			foreach (info; *prefixesPtr) {
				if (info.prefix == prefix) {
					return info.mode;
				}
			}
		}
		return '\0';
	}

	/// Check if user has a specific prefix in channel
	bool hasPrefix(string channel, char prefix) const
	{
		if (auto prefixesPtr = channel in _channelPrefixes) {
			foreach (info; *prefixesPtr) {
				if (info.prefix == prefix) {
					return true;
				}
			}
		}
		return false;
	}

	/// Check if user has a specific mode in channel
	bool hasMode(string channel, char mode) const
	{
		if (auto prefixesPtr = channel in _channelPrefixes) {
			foreach (info; *prefixesPtr) {
				if (info.mode == mode) {
					return true;
				}
			}
		}
		return false;
	}

	/// Add a prefix with mode to user in channel
	void addPrefixWithMode(string channel, char prefix, char mode)
	{
		if (!(channel in _channelPrefixes)) {
			_channelPrefixes[channel] = [];
		}
		if (!hasPrefix(channel, prefix)) {
			_channelPrefixes[channel] ~= PrefixInfo(prefix, mode);
		} else {
			// Update mode if prefix already exists
			foreach(ref info; _channelPrefixes[channel]) {
				if (info.prefix == prefix) {
					info.mode = mode;
					break;
				}
			}
		}
	}

	/// Remove a prefix from user in channel
	void removePrefix(string channel, char prefix)
	{
		if (auto prefixesPtr = channel in _channelPrefixes) {
			PrefixInfo[] newPrefixes;
			foreach (info; *prefixesPtr) {
				if (info.prefix != prefix) {
					newPrefixes ~= info;
				}
			}
			_channelPrefixes[channel] = newPrefixes;
			if (newPrefixes.length == 0) {
				_channelPrefixes.remove(channel);
			}
		}
	}

	/// Remove all prefixes for a channel (when user leaves)
	void removeAllPrefixes(string channel)
	{
		_channelPrefixes.remove(channel);
	}

	/// Get all prefixes user has in channel
	char[] getAllPrefixes(string channel) const
	{
		if (auto prefixesPtr = channel in _channelPrefixes) {
			char[] result;
			foreach (info; *prefixesPtr) {
				result ~= info.prefix;
			}
			return result;
		}
		return [];
	}

	/// Get all prefix info (prefix + mode) user has in channel
	PrefixInfo[] getAllPrefixInfo(string channel) const
	{
		if (auto prefixesPtr = channel in _channelPrefixes) {
			return (*prefixesPtr).dup;
		}
		return [];
	}

	void toString(scope void delegate(const(char)[]) @safe sink) const
	{
		import std.format;
		sink(nickName);
		if (userName) {
			sink("!");
			sink(userName);
		}
		if (hostName) {
			sink("@");
			sink(hostName);
		}
		if (realName) {
			sink("$");
			sink(realName);
		}
		formattedWrite(sink, "(%(%s%|,%))", channels);
	}

	unittest
	{
		import std.string : format;
		auto user = TrackedUser("nick");
		user.userName = "user";
		user.hostName = "host";
		user.realName = "Foo Bar";
		user.channels = ["#a", "#b"];
		assert(format("%s", user) == `nick!user@host$Foo Bar("#a","#b")`);
	}
}

/**
 * Represents an IRC user for use by $(MREF CustomIrcTracker).
 * Params:
 *   Payload = type of extra data per user.
 */
align(1) struct CustomTrackedUser(Payload)
{
	/// $(D CustomTrackedUser) is a super-type of $(MREF TrackedUser).
	TrackedUser user;

	/// Ditto
	alias user this;

	/**
	 * Extra data attached to this user for per-application data.
     */
	Payload payload;

	///
	this(string nickName)
	{
		user = TrackedUser(nickName);
	}
}

///
alias CustomTrackedUser(Payload : void) = TrackedUser;
