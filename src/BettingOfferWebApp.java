import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class BettingOfferWebApp {

    //定义session的存储
    private static final ConcurrentHashMap<String, String> sessionMap = new ConcurrentHashMap<>();
    //定义session过期时间的存储
    private static final ConcurrentHashMap<String, Long> sessionExpireMap = new ConcurrentHashMap<>();
    //定义sessionKey对应userId的存储
    private static final ConcurrentHashMap<String, String> sessionReverseMap = new ConcurrentHashMap<>();
    //定义betOffer的存储
    private static final ConcurrentHashMap<String, HashMap<String, Integer>> betOfferMap = new ConcurrentHashMap<>();
    //session过期时间，默认10分钟
    private static final int SESSION_TIMEOUT = 600_000;
    //清理线程执行周期，默认60分钟
    private static final int CLEAN_TIMER = 3600_000;

    public static void main(String[] args) throws IOException {
        // 创建服务器并绑定端口
        HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
        // 路由配置
        server.createContext("/session/", new SessionHandler());
        server.createContext("/stake/", new StakeHandler());
        server.createContext("/highstakes/", new HighStakesHandler());

        // 启动服务器
        server.setExecutor(null);
        server.start();

        System.out.println("Server started on port 8080");
    }

    // session处理器
    static class SessionHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equals(exchange.getRequestMethod())) {
                jsonResponse(exchange, "请求方法有误", false);
                return;
            }
            //从请求路径中获取userId
            String userId = getParameter(exchange);
            if (Optional.ofNullable(userId).isEmpty()) {
                jsonResponse(exchange, "userId不可为空", false);
                return;
            }
            //获取SessionKey
            String sessionKey = getSessionKey(userId);
            jsonResponse(exchange, sessionKey, true);
        }

        /**
         * 获取SessionKey
         *
         * @param userId userId
         * @return SessionKey
         */
        private String getSessionKey(String userId) {
            String sessionKey;
            if (sessionMap.containsKey(userId) && sessionExpireMap.get(userId) > System.currentTimeMillis()) {
                //存在key且key在过期时间之内
                sessionKey = sessionMap.get(userId);
            } else {
                sessionKey = UUID.randomUUID().toString();
                //这里只做新增和覆盖，过时的session数据在定时任务中清理
                sessionMap.put(userId, sessionKey);
                sessionReverseMap.put(sessionKey, userId);
                sessionExpireMap.put(userId, System.currentTimeMillis() + SESSION_TIMEOUT);
            }
            return sessionKey;
        }
    }

    // Stake处理器
    static class StakeHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equals(exchange.getRequestMethod())) {
                jsonResponse(exchange, "请求方法有误", false);
                return;
            }
            // 获取offerId
            String offerId = getParameter(exchange);
            if (Optional.ofNullable(offerId).isEmpty()) {
                jsonResponse(exchange, "offerId不可为空", false);
                return;
            }
            // 获取sessionKey
            Map<String, String> paramsMap = parseQueryParams(exchange.getRequestURI());
            if (paramsMap.isEmpty() || paramsMap.get("sessionkey").isEmpty()) {
                jsonResponse(exchange, "sessionkey不可为空", false);
                return;
            }
            // 获取stake
            String requestBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            int stake;
            try {
                stake = Integer.parseInt(requestBody);
            } catch (NumberFormatException e) {
                jsonResponse(exchange, "入参有误", false);
                return;
            }

            String userId = sessionReverseMap.get(paramsMap.get("sessionkey"));
            if (Optional.ofNullable(userId).isEmpty()) {
                jsonResponse(exchange, "无效sessionKey", false);
                return;
            }
            //处理betOfferMap
            handleBetOfferMap(userId, stake, offerId);
            jsonResponse(exchange, requestBody, true);
        }

        /**
         * 处理betOfferMap
         *
         * @param userId  userId
         * @param stake   stake
         * @param offerId offerId
         */
        private void handleBetOfferMap(String userId, int stake, String offerId) {
            String userIdAndOfferId = userId + "_" + offerId;
            if (betOfferMap.containsKey(offerId)) {
                //只存放该用户对该offer的最高报价
                if (betOfferMap.get(offerId).containsKey(userIdAndOfferId)) {
                    if (betOfferMap.get(offerId).get(userIdAndOfferId) < stake) {
                        betOfferMap.get(offerId).put(userIdAndOfferId, stake);
                    }
                } else {
                    HashMap<String, Integer> map = betOfferMap.get(offerId);
                    map.put(userIdAndOfferId, stake);
                    betOfferMap.put(offerId, map);
                }
            } else {
                HashMap<String, Integer> map = new HashMap<>();
                map.put(userIdAndOfferId, stake);
                betOfferMap.put(offerId, map);
            }
        }
    }

    // HighStakes处理器
    static class HighStakesHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equals(exchange.getRequestMethod())) {
                jsonResponse(exchange, "userId不可为空", false);
                return;
            }

            String offerId = getParameter(exchange);
            if (Optional.ofNullable(offerId).isEmpty()) {
                jsonResponse(exchange, "offerId不可为空", false);
                return;
            }

            String response = getStakes(offerId);
            jsonResponse(exchange, response, true);
        }

        /**
         * 获取Stakes
         *
         * @param offerId offerId
         * @return String
         */
        private String getStakes(String offerId) {
            if (!betOfferMap.containsKey(offerId)) {
                return null;
            }
            return betOfferMap.get(offerId).entrySet().stream()
                    .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                    .limit(20)
                    .map(entry -> {
                        String userId = Arrays.stream(entry.getKey().split("_")).findFirst().orElse(null);
                        return userId + "=" + entry.getValue();
                    })
                    .collect(Collectors.joining(","));
        }
    }

    /**
     * 解析查询参数
     *
     * @param uri uri
     * @return Map
     */
    public static Map<String, String> parseQueryParams(URI uri) {
        Map<String, String> params = new HashMap<>();
        String query = uri.getRawQuery(); // 获取原始未解码的查询字符串
        if (query == null || query.isEmpty()) {
            return params;
        }
        // 分割参数对
        String[] pairs = query.split("&");
        for (String pair : pairs) {
            int idx = pair.indexOf("=");
            String key = URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8);
            String value = URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8);
            params.put(key, value);
        }
        return params;
    }

    /**
     * 获取url路径参数
     *
     * @param exchange exchange
     * @return String
     */
    private static String getParameter(HttpExchange exchange) {
        String path = exchange.getRequestURI().getPath();
        String[] pathSegments = path.split("/");
        return (pathSegments.length >= 3) ? pathSegments[2] : null;
    }

    /**
     * json格式返回
     *
     * @param exchange exchange
     * @param response response
     * @param status   status
     */
    private static void jsonResponse(HttpExchange exchange, String response, Boolean status) throws IOException {
        String jsonResponse = "{\"status\":" + status + ", \"data\":\"" + response + "\"}";
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, jsonResponse.getBytes().length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(jsonResponse.getBytes());
        }
    }

    // 启动定时清理线程，默认1小时执行一次
    static {
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                System.out.println("定时清理执行开始->" + System.currentTimeMillis());
                List<String> expireKeylist = sessionExpireMap.entrySet().stream()
                        .filter(stringLongEntry -> System.currentTimeMillis() > stringLongEntry.getValue())
                        .map(Map.Entry::getKey)
                        .toList();
                sessionExpireMap.entrySet().removeIf(entry -> expireKeylist.contains(entry.getKey()));
                sessionMap.entrySet().removeIf(entry -> expireKeylist.contains(entry.getKey()));
                sessionReverseMap.entrySet().removeIf(entry -> expireKeylist.contains(entry.getValue()));
                System.out.println("定时清理执行结束->" + System.currentTimeMillis());
            }
        }, CLEAN_TIMER, CLEAN_TIMER);
    }

}
