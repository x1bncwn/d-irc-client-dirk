// source/logging.d
module logging;

import std.datetime;
import std.stdio;

void logToTerminal(string message, string level, string thread) {
    auto timestamp = Clock.currTime().toUnixTime();
    writeln("[", timestamp, "] [Thread ", thread, "] [", level, "] ", message);
}
