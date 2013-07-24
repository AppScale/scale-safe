package com.google.appengine.tools.development;

import com.google.appengine.api.NamespaceManager;
import com.google.appengine.api.users.dev.LoginCookieUtils;
import java.util.concurrent.ConcurrentMap;
import javax.servlet.http.HttpServletRequest;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class LocalHttpRequestEnvironment extends LocalEnvironment
{
    static final String                               DEFAULT_NAMESPACE_HEADER = "X-AppEngine-Default-Namespace";
    static final String                               CURRENT_NAMESPACE_HEADER = "X-AppEngine-Current-Namespace";
    private static final String                       CURRENT_NAMESPACE_KEY    = NamespaceManager.class.getName() + ".currentNamespace";

    private static final String                       APPS_NAMESPACE_KEY       = NamespaceManager.class.getName() + ".appsNamespace";
    private static final String                       USER_ID_KEY              = "com.google.appengine.api.users.UserService.user_id_key";
    private static final String                       USER_ORGANIZATION_KEY    = "com.google.appengine.api.users.UserService.user_organization";
    private static final String                       X_APPENGINE_QUEUE_NAME   = "X-AppEngine-QueueName";

    /*
     * AppScale -- repladed CookieData with AppScaleCookieData (see
     * LocalCookieUtils)
     */
    private final LoginCookieUtils.AppScaleCookieData loginCookieData;
    private static final String                       COOKIE_NAME              = "dev_appserver_login";
    private boolean                                   FORCE_ADMIN              = false;
    private String                                    DEVEL_FAKE_IS_ADMIN_RAW_HEADER = "X-AppEngine-Fake-Is-Admin";
    private String                                    DEVEL_PAYLOAD_RAW_HEADER = "HTTP_X_APPENGINE_DEVELOPMENT_PAYLOAD";

     public LocalHttpRequestEnvironment(String appId, String serverName, String majorVersionId, int instance, HttpServletRequest request, Long deadlineMillis, ServersFilterHelper serversFilterHelper)
     {
        super(appId, majorVersionId, deadlineMillis);
        this.loginCookieData = LoginCookieUtils.getCookieData(request);
        this.FORCE_ADMIN = checkForceAdmin(request); 
        String requestNamespace = request.getHeader("X-AppEngine-Default-Namespace");
        if (requestNamespace != null)
        {
            this.attributes.put(APPS_NAMESPACE_KEY, requestNamespace);
        }
        String currentNamespace = request.getHeader("X-AppEngine-Current-Namespace");
        if (currentNamespace != null)
        {
            this.attributes.put(CURRENT_NAMESPACE_KEY, currentNamespace);
        }
        if (this.loginCookieData != null)
        {
            this.attributes.put("com.google.appengine.api.users.UserService.user_id_key", this.loginCookieData.getUserId());
            this.attributes.put("com.google.appengine.api.users.UserService.user_organization", "");
        }
        if (request.getHeader("X-AppEngine-QueueName") != null)
        {
            this.attributes.put("com.google.appengine.request.offline", Boolean.TRUE);
        }
        this.attributes.put("com.google.appengine.http_servlet_request", request);
        this.attributes.put("com.google.appengine.tools.development.servers_filter_helper", serversFilterHelper);
    }

    public boolean isLoggedIn()
    {
        if(this.FORCE_ADMIN)
        {
            return true;
        }
        if(this.loginCookieData == null)
        {   
            return false;
        }
        return this.loginCookieData.isValid();
    }

    public String getEmail()
    {
        if(this.FORCE_ADMIN)
        { 
            return "admin@admin.com";
        }
        if (this.loginCookieData == null)
        {
            return null;
        }
        return this.loginCookieData.getEmail();
    }

    public boolean isAdmin()
    {
        if(this.FORCE_ADMIN)
        {
            return true;
        }
        if (this.loginCookieData == null)
        {
            return false;
        }
        return this.loginCookieData.isAdmin();
    }

    private boolean checkForceAdmin(HttpServletRequest request)
    {
        String secretHashHeader = request.getHeader(DEVEL_PAYLOAD_RAW_HEADER);
        String secretHash = getSecretHash();
        if(secretHashHeader != null)
        {
            if(secretHashHeader.equals(secretHash))
            {
                return true;
            }    
        }
        else
        {
            secretHashHeader = request.getHeader(DEVEL_FAKE_IS_ADMIN_RAW_HEADER);
            if(secretHashHeader != null)
            {
                if(secretHashHeader.equals(secretHash))
                {
                    return true;
                }
            }
        }
        return false;
    }

    private String getSecretHash()
    {
        String secret = getAppName() + "/" + getSecret();
        String secretHash = toSHA1(secret.getBytes());
        return secretHash;
    }
  
    private String toSHA1(byte[] convertme)
    {
        MessageDigest md = null;
        try
        {
            md = MessageDigest.getInstance("SHA-1");
        }
        catch(NoSuchAlgorithmException e) 
        {
            e.printStackTrace();
        } 
        return byteArrayToHexString(md.digest(convertme));
    }

    private String byteArrayToHexString(byte[] b)
    {
        String result = "";
        for (int i=0; i < b.length; i++)
        {
            result += Integer.toString( ( b[i] & 0xff ) + 0x100, 16).substring( 1 );
        }
        return result;
    }
   
    private String getAppName()
    {
        String appName = System.getProperty("APPLICATION_ID");
        return appName;
    }

    private String getSecret()
    {
        String secret = System.getProperty("COOKIE_SECRET");
        return secret;
    }  
}
