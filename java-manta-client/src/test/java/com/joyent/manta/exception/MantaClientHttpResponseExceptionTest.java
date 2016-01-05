package com.joyent.manta.exception;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static com.joyent.manta.exception.MantaErrorCode.NO_CODE_ERROR;
import static com.joyent.manta.exception.MantaErrorCode.RESOURCE_NOT_FOUND_ERROR;

/**
 * Tests the deserialization of errors on the Manta API that return JSON.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaClientHttpResponseExceptionTest {

    @Test
    public void simulate404WithContent() throws IOException {
        final String json = "{\"code\":\"ResourceNotFound\",\"message\":"
                + "\"/bob/stor/19dd6047-e0d4-41c1-9a3c-013e180fa07e "
                + "was not found\"}";

        MockLowLevelHttpRequest lowLevelHttpRequest = new MockLowLevelHttpRequest();

        MockLowLevelHttpResponse lowLevelHttpResponse = new MockLowLevelHttpResponse();

        final String reasonPhrase = "Not Found";
        final int httpErrorCode = 404;
        final String method = "GET";

        lowLevelHttpResponse.setContent(json);
        lowLevelHttpResponse.setStatusCode(httpErrorCode);
        lowLevelHttpResponse.setReasonPhrase(reasonPhrase);
        lowLevelHttpResponse.addHeader("x-request-id", new UUID(0L, 0L).toString());

        HttpResponse response = fakeResponse(lowLevelHttpRequest, lowLevelHttpResponse,
                method);

        HttpResponseException httpResponseException =
                new HttpResponseException(response);
        MantaClientHttpResponseException exception =
                new MantaClientHttpResponseException(httpResponseException);

        Assert.assertEquals(exception.getContent(), json);
        Assert.assertEquals(exception.getServerCode(), RESOURCE_NOT_FOUND_ERROR);
        Assert.assertEquals(exception.getServerMessage(),
                "/bob/stor/19dd6047-e0d4-41c1-9a3c-013e180fa07e was not found");
        Assert.assertEquals(exception.getStatusMessage(), reasonPhrase);
        Assert.assertEquals(exception.getStatusCode(), httpErrorCode);
        Assert.assertEquals(exception.getMessage(),
                "404 Not Found (request: 00000000-0000-0000-0000-000000000000) - [ResourceNotFound] "
                + "/bob/stor/19dd6047-e0d4-41c1-9a3c-013e180fa07e was not found");
    }

    @Test
    public void simulate404WithNoContent() throws IOException {
        MockLowLevelHttpRequest lowLevelHttpRequest = new MockLowLevelHttpRequest();
        MockLowLevelHttpResponse lowLevelHttpResponse = new MockLowLevelHttpResponse();

        final String reasonPhrase = "Not Found";
        final int httpErrorCode = 404;
        final String method = "GET";

        lowLevelHttpResponse.setStatusCode(httpErrorCode);
        lowLevelHttpResponse.setReasonPhrase(reasonPhrase);

        HttpResponse response = fakeResponse(lowLevelHttpRequest, lowLevelHttpResponse,
                method);

        HttpResponseException httpResponseException =
                new HttpResponseException(response);
        MantaClientHttpResponseException exception =
                new MantaClientHttpResponseException(httpResponseException);

        Assert.assertNull(exception.getContent());
        Assert.assertEquals(exception.getServerCode(), NO_CODE_ERROR);
        Assert.assertNull(exception.getServerMessage());
        Assert.assertEquals(exception.getStatusMessage(), reasonPhrase);
        Assert.assertEquals(exception.getStatusCode(), httpErrorCode);
        Assert.assertEquals(exception.getMessage(), "404 Not Found");
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
