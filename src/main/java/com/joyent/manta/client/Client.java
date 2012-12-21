package com.joyent.manta.client;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.javanet.NetHttpTransport;

public class Client {
        private static final Log LOG = LogFactory.getLog(Client.class);
        
        private static final HttpRequestFactory HTTP_REQUEST_FACTORY = new NetHttpTransport().createRequestFactory();
        
        private final GenericUrl url_;
        private final String login_;
        private final String keyPath_;
        private Client(String url, String login, String keyPath) {
                url_ = new GenericUrl(url);
                login_ = login;
                keyPath_ = keyPath;
        }

        public static Client newInstance(String url, String login, String keyPath) {
                LOG.debug("entering newInstance with url " + url + " login " + login + " keyPath " + keyPath);
                Client c = new Client(url, login, keyPath);
                return c;
        }
        
        public InputStream get(String path, HttpHeaders headers) throws IOException {
                HttpRequest req = HTTP_REQUEST_FACTORY.buildGetRequest(this.url_);
                return null;
        }
}
