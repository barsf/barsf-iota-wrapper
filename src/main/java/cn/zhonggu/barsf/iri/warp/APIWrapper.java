package cn.zhonggu.barsf.iri.warp;

import cn.zhonggu.barsf.iri.modelWrapper.AddressWrapper;
import cn.zhonggu.barsf.iri.service.dto.GetAddressResponse;
import cn.zhonggu.barsf.iri.service.dto.TestResponse;
import cn.zhonggu.barsf.iri.storage.innoDB.mybatis.DbHelper;
import cn.zhonggu.barsf.iri.storage.innoDB.mybatis.modelMapper.AddressMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.iota.iri.*;
import com.iota.iri.conf.Configuration;
import com.iota.iri.controllers.AddressViewModel;
import com.iota.iri.controllers.BundleViewModel;
import com.iota.iri.controllers.TagViewModel;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.hash.Curl;
import com.iota.iri.hash.PearlDiver;
import com.iota.iri.model.Hash;
import com.iota.iri.network.Neighbor;
import com.iota.iri.service.API;
import com.iota.iri.service.ValidationException;
import com.iota.iri.service.dto.*;
import com.iota.iri.utils.Converter;
import com.iota.iri.utils.MapIdentityManager;
import io.undertow.Undertow;
import io.undertow.security.api.AuthenticationMechanism;
import io.undertow.security.api.AuthenticationMode;
import io.undertow.security.handlers.AuthenticationCallHandler;
import io.undertow.security.handlers.AuthenticationConstraintHandler;
import io.undertow.security.handlers.AuthenticationMechanismsHandler;
import io.undertow.security.handlers.SecurityInitialHandler;
import io.undertow.security.idm.IdentityManager;
import io.undertow.security.impl.BasicAuthenticationMechanism;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.channels.StreamSinkChannel;
import org.xnio.streams.ChannelInputStream;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.undertow.Handlers.path;

@SuppressWarnings("unchecked")
public class APIWrapper {

    private static final Logger log = LoggerFactory.getLogger(APIWrapper.class);
    private final IXI ixi;

    private Undertow server;

    private final Gson gson = new GsonBuilder().create();

    private final AtomicInteger counter = new AtomicInteger(0);

    private Pattern trytesPattern = Pattern.compile("[9A-Z]*");

    private final static int HASH_SIZE = 81;

    private final int maxRequestList;
    private final int maxBodyLength;
    private final boolean testNet;

    private final static String overMaxErrorMessage = "Could not complete request";
    private final static String invalidParams = "Invalid parameters";

    private ConcurrentHashMap<Hash, Boolean> previousEpochsSpentAddresses;

    private final static char ZERO_LENGTH_ALLOWED = 'Y';
    private final static char ZERO_LENGTH_NOT_ALLOWED = 'N';
    private Iota instance;
    private Object iotaApi;
    private Method iotaMethodProcess;
    private List<String> iotaApiMethod = Arrays.asList( "storeMessage", "addNeighbors", "attachToTangle", "broadcastTransactions", "findTransactions", "getBalances", "getInclusionStates", "getNeighbors",  "getNodeInfo", "getTips", "getTransactionsToApprove", "getTrytes", "interruptAttachingToTangle", "removeNeighbors", "storeTransactions", "getMissingTransactions", "checkConsistency", "wereAddressesSpentFrom");

    public APIWrapper(Iota instance, IXI ixi) {
        this.instance = instance;
        this.ixi = ixi;
        maxRequestList = instance.configuration.integer(Configuration.DefaultConfSettings.MAX_REQUESTS_LIST);
        maxBodyLength = instance.configuration.integer(Configuration.DefaultConfSettings.MAX_BODY_LENGTH);
        testNet = instance.configuration.booling(Configuration.DefaultConfSettings.TESTNET);
        previousEpochsSpentAddresses = new ConcurrentHashMap<>();
    }

    public void init() throws Exception {
        Class clazz = Class.forName("com.iota.iri.service.API");
        Constructor cons = clazz.getConstructor(Iota.class, IXI.class);
        iotaApi = cons.newInstance(instance,ixi);
        iotaMethodProcess = clazz.getDeclaredMethod("process", String.class, InetSocketAddress.class);
        iotaMethodProcess.setAccessible(true);

        readPreviousEpochsSpentAddresses(testNet);
        final int apiPort = instance.configuration.integer(Configuration.DefaultConfSettings.PORT);
        final String apiHost = instance.configuration.string(Configuration.DefaultConfSettings.API_HOST);

        log.debug("Binding JSON-REST API Undertow server on {}:{}", apiHost, apiPort);

        server = Undertow.builder().addHttpListener(apiPort, apiHost)
                .setHandler(path().addPrefixPath("/", addSecurity(new HttpHandler() {
                    @Override
                    public void handleRequest(final HttpServerExchange exchange) throws Exception {
                        HttpString requestMethod = exchange.getRequestMethod();
                        if (Methods.OPTIONS.equals(requestMethod)) {
                            String allowedMethods = "GET,HEAD,POST,PUT,DELETE,TRACE,OPTIONS,CONNECT,PATCH";
                            //return list of allowed methods in response headers
                            exchange.setStatusCode(StatusCodes.OK);
                            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, MimeMappings.DEFAULT_MIME_MAPPINGS.get("txt"));
                            exchange.getResponseHeaders().put(Headers.CONTENT_LENGTH, 0);
                            exchange.getResponseHeaders().put(Headers.ALLOW, allowedMethods);
                            exchange.getResponseHeaders().put(new HttpString("Access-Control-Allow-Origin"), "*");
                            exchange.getResponseHeaders().put(new HttpString("Access-Control-Allow-Headers"), "Origin, X-Requested-With, Content-Type, Accept, X-IOTA-API-Version");
                            exchange.getResponseSender().close();
                            return;
                        }

                        if (exchange.isInIoThread()) {
                            exchange.dispatch(this);
                            return;
                        }

                        processRequest(exchange);
                    }
                }))).build();
        server.start();
    }

    private void readPreviousEpochsSpentAddresses(boolean isTestnet) throws IOException {
        if (isTestnet) {
            return;
        }

        if (!SignedFiles.isFileSignatureValid(Configuration.PREVIOUS_EPOCHS_SPENT_ADDRESSES_TXT,
                Configuration.PREVIOUS_EPOCH_SPENT_ADDRESSES_SIG,
                Snapshot.SNAPSHOT_PUBKEY, Snapshot.SNAPSHOT_PUBKEY_DEPTH, Snapshot.SPENT_ADDRESSES_INDEX)) {
            throw new RuntimeException("Failed to load previousEpochsSpentAddresses - signature failed.");
        }

        InputStream in = Snapshot.class.getResourceAsStream(Configuration.PREVIOUS_EPOCHS_SPENT_ADDRESSES_TXT);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                previousEpochsSpentAddresses.put(new Hash(line), true);
            }
        } catch (IOException e) {
            log.error("Failed to load previousEpochsSpentAddresses.");
        }
    }

    private void processRequest(final HttpServerExchange exchange) throws Exception {
        final ChannelInputStream cis = new ChannelInputStream(exchange.getRequestChannel());
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");

        final long beginningTime = System.currentTimeMillis();
        final String body = IOUtils.toString(cis, StandardCharsets.UTF_8);
        final AbstractResponse response;

        if (!exchange.getRequestHeaders().contains("X-IOTA-API-Version")) {
            response = ErrorResponse.create("Invalid API Version");
        } else if (body.length() > maxBodyLength) {
            response = ErrorResponse.create("Request too long");
        } else {
            Object reVal = null;
            boolean hasIotaApi = false;
            for (String methodStr:iotaApiMethod) {
                if(StringUtils.contains(body, methodStr)){
                    hasIotaApi = true;
                    break;
                }
            }
            if(hasIotaApi){
                log.info("The requester into Iota Api");
                reVal = iotaMethodProcess.invoke(iotaApi, body, exchange.getSourceAddress());
                response = (AbstractResponse) reVal;
            }else{
                log.info("The requester into Barsf Api");
                response = process(body, exchange.getSourceAddress());
            }
        }
        sendResponse(exchange, response, beginningTime);

    }


    private AbstractResponse process(final String requestString, InetSocketAddress sourceAddress) throws UnsupportedEncodingException {

        try {

            final Map<String, Object> request = gson.fromJson(requestString, Map.class);
            if (request == null) {
                return ExceptionResponse.create("Invalid request payload: '" + requestString + "'");
            }

            final String command = (String) request.get("command");
            if (command == null) {
                return ErrorResponse.create("COMMAND parameter has not been specified in the request.");
            }

            if (instance.configuration.string(Configuration.DefaultConfSettings.REMOTE_LIMIT_API).contains(command) &&
                    !sourceAddress.getAddress().isLoopbackAddress()) {
                return AccessLimitedResponse.create("COMMAND " + command + " is not available on this node");
            }

            log.debug("# {} -> Requesting command '{}'", counter.incrementAndGet(), command);

            switch (command) {
                case "test": {
                    List<String> guys = getParameterAsList(request, "guys", 0);
                    return TestResponse.create(guys);
                }

                case "getAddress": {
                    final List<String> addresses = getParameterAsList(request, "addresses", HASH_SIZE);
                    HashMap<String, AddressWrapper> resultMap = new HashMap<>();

                    // todo 可以在subProvider中直接提供方法
                    try (final SqlSession session = DbHelper.getSingletonSessionFactory().openSession(true)) {
                        AddressMapper mapper = session.getMapper(AddressMapper.class);
                        for (String address : addresses) {
                            AddressWrapper anAddress = mapper.selectByPrimaryKey(address);
                            if(anAddress!=null) {
                                resultMap.put(anAddress.getHash(), anAddress);
                            }
                        }
                    }

                    return GetAddressResponse.create(resultMap);
                }
                default: {
                    AbstractResponse response = ixi.processCommand(command, request);
                    return response == null ?
                            ErrorResponse.create("Command [" + command + "] is unknown") :
                            response;
                }
            }

        } catch (final ValidationException e) {
            log.info("API Validation failed: " + e.getLocalizedMessage());
            return ErrorResponse.create(e.getLocalizedMessage());
        } catch (final Exception e) {
            log.error("API Exception: ", e);
            return ExceptionResponse.create(e.getLocalizedMessage());
        }
    }


    private void validateTrytes(String paramName, int size, String result) throws ValidationException {
        if (!validTrytes(result, size, ZERO_LENGTH_NOT_ALLOWED)) {
            throw new ValidationException("Invalid " + paramName + " input");
        }
    }

    private void validateParamExists(Map<String, Object> request, String paramName) throws ValidationException {
        if (!request.containsKey(paramName)) {
            throw new ValidationException(invalidParams);
        }
    }

    private List<String> getParameterAsList(Map<String, Object> request, String paramName, int size) throws ValidationException {
        validateParamExists(request, paramName);
        final List<String> paramList = (List<String>) request.get(paramName);
        if (paramList.size() > maxRequestList) {
            throw new ValidationException(overMaxErrorMessage);
        }

        if (size > 0) {
            //validate
            for (final String param : paramList) {
                validateTrytes(paramName, size, param);
            }
        }

        return paramList;

    }


    private void sendResponse(final HttpServerExchange exchange, final AbstractResponse res, final long beginningTime)
            throws IOException {
        res.setDuration((int) (System.currentTimeMillis() - beginningTime));
        final String response = gson.toJson(res);

        if (res instanceof ErrorResponse) {
            exchange.setStatusCode(400); // bad request
        } else if (res instanceof AccessLimitedResponse) {
            exchange.setStatusCode(401); // api method not allowed
        } else if (res instanceof ExceptionResponse) {
            exchange.setStatusCode(500); // internal error
        }

        setupResponseHeaders(exchange);

        ByteBuffer responseBuf = ByteBuffer.wrap(response.getBytes(StandardCharsets.UTF_8));
        exchange.setResponseContentLength(responseBuf.array().length);
        StreamSinkChannel sinkChannel = exchange.getResponseChannel();
        sinkChannel.getWriteSetter().set(channel -> {
            if (responseBuf.remaining() > 0) {
                try {
                    sinkChannel.write(responseBuf);
                    if (responseBuf.remaining() == 0) {
                        exchange.endExchange();
                    }
                } catch (IOException e) {
                    log.error("Lost connection to client - cannot send response");
                    exchange.endExchange();
                    sinkChannel.getWriteSetter().set(null);
                }
            } else {
                exchange.endExchange();
            }
        });
        sinkChannel.resumeWrites();
    }

    private boolean validTrytes(String trytes, int length, char zeroAllowed) {
        if (trytes.length() == 0 && zeroAllowed == ZERO_LENGTH_ALLOWED) {
            return true;
        }
        if (trytes.length() != length) {
            return false;
        }
        Matcher matcher = trytesPattern.matcher(trytes);
        return matcher.matches();
    }

    private static void setupResponseHeaders(final HttpServerExchange exchange) {
        final HeaderMap headerMap = exchange.getResponseHeaders();
        headerMap.add(new HttpString("Access-Control-Allow-Origin"), "*");
        headerMap.add(new HttpString("Keep-Alive"), "timeout=500, max=100");
    }

    private HttpHandler addSecurity(final HttpHandler toWrap) {
        String credentials = instance.configuration.string(Configuration.DefaultConfSettings.REMOTE_AUTH);
        if (credentials == null || credentials.isEmpty()) {
            return toWrap;
        }

        final Map<String, char[]> users = new HashMap<>(2);
        users.put(credentials.split(":")[0], credentials.split(":")[1].toCharArray());

        IdentityManager identityManager = new MapIdentityManager(users);
        HttpHandler handler = toWrap;
        handler = new AuthenticationCallHandler(handler);
        handler = new AuthenticationConstraintHandler(handler);
        final List<AuthenticationMechanism> mechanisms = Collections.singletonList(new BasicAuthenticationMechanism("Iota Realm"));
        handler = new AuthenticationMechanismsHandler(handler, mechanisms);
        handler = new SecurityInitialHandler(AuthenticationMode.PRO_ACTIVE, identityManager, handler);
        return handler;
    }

    public void shutDown() {
        if (server != null) {
            server.stop();
        }
    }


}

