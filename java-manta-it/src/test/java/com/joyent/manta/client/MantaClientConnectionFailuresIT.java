//package com.joyent.manta.client;

//@Test // Incompatible with Shade
//public class MantaClientConnectionFailuresIT {
//    private ConfigContext config;
//    private int retryAttempts;
//
//    @BeforeClass()
//    @Parameters({"manta.url", "manta.user", "manta.key_path", "manta.key_id", "manta.timeout", "manta.retries"})
//    public void beforeClass(@Optional String mantaUrl,
//                            @Optional String mantaUser,
//                            @Optional String mantaKeyPath,
//                            @Optional String mantaKeyId,
//                            @Optional Integer mantaTimeout,
//                            @Optional InputStream retries)
//            throws IOException {
//
//        // Let TestNG configuration take precedence over environment variables
//        this.config = new IntegrationTestConfigContext(
//                mantaUrl, mantaUser, mantaKeyPath, mantaKeyId, mantaTimeout);
//
//        // We start at -1 one because the first try doesn't count
//        this.retryAttempts = -1;
//    }
//
//    private class NoHttpResponseDefaultHttpResponseParser extends DefaultHttpResponseParser {
//        public NoHttpResponseDefaultHttpResponseParser(SessionInputBuffer buffer, LineParser lineParser, HttpResponseFactory responseFactory, MessageConstraints constraints) {
//            super(buffer, lineParser, responseFactory, constraints);
//        }
//
//        @Override
//        protected HttpResponse parseHead(SessionInputBuffer sessionBuffer) throws IOException, HttpException, ParseException {
//            retryAttempts++;
//            throw new NoHttpResponseException("simulating server failure to respond");
//        }
//    }
//
//    private class NoHttpResponseDefaultHttpResponseParserFactory extends DefaultHttpResponseParserFactory {
//        private final LineParser lineParser;
//        private final HttpResponseFactory responseFactory;
//
//        public NoHttpResponseDefaultHttpResponseParserFactory(final LineParser lineParser,
//                                                              final HttpResponseFactory responseFactory) {
//            super(lineParser, responseFactory);
//            this.lineParser = lineParser != null ? lineParser : BasicLineParser.INSTANCE;
//            this.responseFactory = responseFactory != null ? responseFactory
//                    : DefaultHttpResponseFactory.INSTANCE;
//        }
//
//        public NoHttpResponseDefaultHttpResponseParserFactory() {
//            this(null, null);
//        }
//
//        @Override
//        public HttpMessageParser<HttpResponse> create(final SessionInputBuffer buffer,
//                                                      final MessageConstraints constraints) {
//            return new NoHttpResponseDefaultHttpResponseParser(buffer, lineParser, responseFactory, constraints);
//        }
//    }
//
//    private class NoHttpResponseMantaConnectionFactory extends MantaConnectionFactory {
//        public NoHttpResponseMantaConnectionFactory(ConfigContext config, KeyPair keyPair) {
//            super(config, keyPair);
//        }
//
//        @Override
//        protected HttpConnectionFactory<HttpRoute, ManagedHttpClientConnection> buildHttpConnectionFactory() {
//            return new ManagedHttpClientConnectionFactory(
//                    new DefaultHttpRequestWriterFactory(),
//                    new NoHttpResponseDefaultHttpResponseParserFactory());
//        }
//    }
//
//    public void canRetryOnNoHttpResponseException() throws IOException {
//        final KeyPairFactory keyPairFactory = new KeyPairFactory(config);
//        final KeyPair keyPair = keyPairFactory.createKeyPair();
//        MantaClient mantaClient = new MantaClient(config, keyPair,
//                new NoHttpResponseMantaConnectionFactory(config, keyPair));
//        String testPathPrefix = String.format("%s/stor/%s/",
//                config.getMantaHomeDirectory(), UUID.randomUUID());
//
//        boolean thrown = false;
//        try {
//            mantaClient.head(testPathPrefix);
//        } catch (NoHttpResponseException e) {
//            thrown = true;
//        } finally {
//            mantaClient.closeWithWarning();
//        }
//
//        Assert.assertTrue(thrown, "NoHttpResponseException was never thrown");
//        Assert.assertEquals(retryAttempts, config.getRetries().intValue());
//    }
//}
