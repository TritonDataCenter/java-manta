package com.joyent.manta.client.exception;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Set;

/**
 * Tests the deserialization of errors on the Manta API that return JSON.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaClientHttpResponseExceptionTest {

    @Test
    public void simulate404() throws IOException {
        final String json = "{\"code\":\"ResourceNotFound\",\"message\":"
                + "\"/elijah.zupancic/stor/19dd6047-e0d4-41c1-9a3c-013e180fa07e "
                + "was not found\"}";

        MockLowLevelHttpRequest lowLevelHttpRequest = new MockLowLevelHttpRequest();
        MockLowLevelHttpResponse lowLevelHttpResponse = new MockLowLevelHttpResponse();

        lowLevelHttpResponse.setContent(json);
        lowLevelHttpResponse.setStatusCode(404);
        lowLevelHttpResponse.setReasonPhrase("404 Not Found");

        HttpResponse response = fakeResponse(lowLevelHttpRequest, lowLevelHttpResponse,
                "GET");

        HttpResponseException httpResponseException =
                new HttpResponseException(response);
        MantaClientHttpResponseException exception =
                new MantaClientHttpResponseException(httpResponseException);

        System.out.println(exception.toString());
    }

    private static HttpResponse fakeResponse(MockLowLevelHttpRequest lowLevelHttpRequest,
                                             MockLowLevelHttpResponse lowLevelHttpResponse,
                                             String method) {

        MockHttpTransport.Builder builder = new MockHttpTransport.Builder();
        Set<String> methods = new HashSet<>();
        methods.add(method);

        lowLevelHttpRequest.setResponse(lowLevelHttpResponse);

        MockHttpTransport transport =
                builder.setLowLevelHttpRequest(lowLevelHttpRequest)
                       .setSupportedMethods(methods)
                       .build();


        Class<HttpRequest> requestClazz = HttpRequest.class;
        Class<HttpResponse> responseClazz = HttpResponse.class;

        try {
            // Build request
            Constructor<HttpRequest> requestConstructor =
                    requestClazz.getDeclaredConstructor(HttpTransport.class,
                            String.class);
            requestConstructor.setAccessible(true);
            HttpRequest request = requestConstructor.newInstance(transport, method);

            // Build response
            Constructor<HttpResponse> responseConstructor =
                    responseClazz.getDeclaredConstructor(HttpRequest.class,
                            LowLevelHttpResponse.class);
            responseConstructor.setAccessible(true);

            return responseConstructor.newInstance(request, lowLevelHttpResponse);

        } catch (NoSuchMethodException | IllegalAccessException |
                 InstantiationException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
