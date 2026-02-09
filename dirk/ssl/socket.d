module ssl.socket;

import std.exception;
import std.socket;

// Import OpenSSL bindings
import deimos.openssl.opensslv;
import deimos.openssl.err;
import deimos.openssl.ssl;

private __gshared SSL_CTX* sslContext;
private __gshared bool sslContextInitialized = false;

void initSslContext()
{
    if(!sslContextInitialized) {
        // Initialize OpenSSL
        int initResult = OPENSSL_init_ssl(OPENSSL_INIT_LOAD_SSL_STRINGS, null);
        if (initResult != 1) {
            throw new Exception("Failed to initialize OpenSSL");
        }
        
        auto method = TLS_client_method();
        enforce(method !is null, "TLS_client_method returned null");
        
        sslContext = SSL_CTX_new(method);
        enforce(sslContext !is null, "SSL_CTX_new failed");
        
        // Disable certificate verification for IRC
        // SSL_CTX_set_verify(sslContext, SSL_VERIFY_NONE, null);
        
        sslContextInitialized = true;
    }
}

/**
 * Represents a secure TCP socket using TLS.
 */
class SslSocket : Socket
{
    private:
    SSL* ssl;
    
    public:
    /**
     * Create a new unconnected and blocking SSL socket.
     */
    this(AddressFamily af)
    {
        initSslContext();
        
        super(af, SocketType.STREAM, ProtocolType.TCP);
        
        enforce(sslContext !is null, "SSL context is null");
        
        ssl = SSL_new(sslContext);
        enforce(ssl !is null, "SSL_new failed");
        
        SSL_set_fd(ssl, super.handle);
        SSL_set_verify(ssl, SSL_VERIFY_NONE, null);
    }
    
    ~this()
    {
        if (ssl !is null)
        {
            SSL_shutdown(ssl);
            SSL_free(ssl);
            ssl = null;
        }
    }
    
    private int sslEnforce(const SSL* ssl, int result, string file = __FILE__, size_t line = __LINE__)
    {
	import core.stdc.config : c_ulong;
        import core.stdc.string : strlen;

        if(result <= 0)
        {
            auto error = SSL_get_error(ssl, result);
            
            // SSL_ERROR_WANT_READ = 2, SSL_ERROR_WANT_WRITE = 3
            if(error != 2 && error != 3)
            {
                c_ulong errCode = ERR_get_error();
                char[256] buf;
                string msg = "Unknown SSL error";
                
                if(errCode != 0) {
                    ERR_error_string(errCode, buf.ptr);
                    msg = buf[0 .. strlen(buf.ptr)].idup;
                }
                
                throw new Exception("SSL error: " ~ msg, file, line);
            }
        }
        
        return result;
    }
    
    override:
    void connect(Address to) @trusted
    {
        super.connect(to);
        sslEnforce(ssl, SSL_connect(ssl));
    }
    
    ptrdiff_t receive(scope void[] buf, SocketFlags flags) @trusted
    {
        enforce(ssl !is null, "SSL object is null");
        auto result = sslEnforce(ssl, SSL_read(ssl, buf.ptr, cast(int)buf.length));
        return cast(ptrdiff_t)result;
    }
    
    ptrdiff_t receive(scope void[] buf) @trusted
    {
        return receive(buf, SocketFlags.NONE);
    }
    
    ptrdiff_t send(scope const(void)[] buf, SocketFlags flags) @trusted
    {
        enforce(ssl !is null, "SSL object is null");
        auto result = sslEnforce(ssl, SSL_write(ssl, buf.ptr, cast(int)buf.length));
        return cast(ptrdiff_t)result;
    }
    
    ptrdiff_t send(scope const(void)[] buf) @trusted
    {
        return send(buf, SocketFlags.NONE);
    }
}
