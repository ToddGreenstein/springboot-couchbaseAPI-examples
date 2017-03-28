package com.example;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.deps.io.netty.channel.ConnectTimeoutException;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.couchbase.client.java.*;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.error.TemporaryFailureException;
import com.couchbase.client.java.query.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.*;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import javax.servlet.*;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import rx.Observable;
import static com.couchbase.client.java.util.retry.RetryBuilder.anyOf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SpringBootApplication
@RestController
@RequestMapping("/")
public class DemoApplication implements Filter {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    @Override
    public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain)
            throws IOException, ServletException {
        HttpServletResponse response = (HttpServletResponse) res;
        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
        chain.doFilter(req, res);
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void destroy() {
    }

    @Value("${hostname}")
    private String hostname;

    @Value("${bucket}")
    private String bucket;

    @Value("${password}")
    private String password;

    private static final Logger LOGGER = LoggerFactory.getLogger(DemoApplication.class);

    public @Bean
    Cluster cluster() {
        return CouchbaseCluster.create(hostname);
    }

    public @Bean
    Bucket bucket() {
        return cluster().openBucket(bucket, password);
    }

    @RequestMapping(value = "/findPIISSN", method = RequestMethod.GET)
    public Object findPIISSN(@RequestParam("verbose") Optional<String> verbose) {
        String query;
        if (verbose.isPresent()) {
            query = "SELECT * FROM `" + bucket().name() + "` WHERE ANY v IN "
                    + "TOKENS(`" + bucket().name() + "`, {\"specials\":true}) SATISFIES "
                    + "REGEXP_LIKE(TOSTRING(v),'(\\\\d{3}-\\\\d{2}-\\\\d{4})|(\\\\b\\\\d{9}\\\\b)') END";
        } else {
            query = "SELECT meta().id FROM `" + bucket().name() + "` WHERE ANY v IN "
                    + "TOKENS(`" + bucket().name() + "`, {\"specials\":true}) SATISFIES "
                    + "REGEXP_LIKE(TOSTRING(v),'(\\\\d{3}-\\\\d{2}-\\\\d{4})|(\\\\b\\\\d{9}\\\\b)') END";
        }
        return bucket().async().query(N1qlQuery.simple(query))
                .flatMap(AsyncN1qlQueryResult::rows)
                .map(result -> result.value().toMap())
                .toList()
                .timeout(10, TimeUnit.SECONDS)
                .toBlocking()
                .single();

    }

    @RequestMapping(value = "/findRange", method = RequestMethod.GET)
    public Object findRange(@RequestParam("lower") String lower,
            @RequestParam("upper") String upper) {
        if (upper == null || upper.equals("") || lower == null || lower.equals("")) {
            return new ResponseEntity<String>(JsonObject.create()
                    .put("message", "Both `upper` and `lower` param values must be supplied")
                    .toString(), HttpStatus.BAD_REQUEST);
        }
        String statement = "SELECT * FROM `" + bucket().name() + "` WHERE "
                + "TONUMBER(LTRIM(meta().id,\"test::\")) > TONUMBER($lower) AND "
                + "TONUMBER(LTRIM(meta().id,\"test::\")) < TONUMBER($upper) ORDER BY "
                + "TONUMBER(LTRIM(meta().id,\"test::\"))";
        JsonObject parameters = JsonObject.create().put("lower", lower).put("upper", upper);
        ParameterizedN1qlQuery query = ParameterizedN1qlQuery.parameterized(statement, parameters);
        return bucket().async().query(query)
                .flatMap(AsyncN1qlQueryResult::rows)
                .map(result -> result.value().toMap())
                .toList()
                .timeout(10, TimeUnit.SECONDS)
                .toBlocking()
                .single();

    }

    @RequestMapping(value = "/findInvoices", method = RequestMethod.GET)
    public Object findInvoices(@RequestParam("email") Optional<String> email,
            @RequestParam("limit") Optional<String> limit,
            @RequestParam("offset") Optional<String> offset) {
        String query;
        if (email.isPresent()) {
            query = "SELECT `" + bucket().name() + "`.email, v.account, v.type, v.amount"
                    + " FROM `" + bucket().name() + "` UNNEST accountHistory v "
                    + "WHERE  `" + bucket().name() + "`.email='" + email.get()
                    + "' AND v.type='invoice'";
        } else {
            query = "SELECT `" + bucket().name() + "`.email, v.account, v.type, v.amount"
                    + " FROM `" + bucket().name() + "` UNNEST accountHistory v "
                    + "WHERE  `" + bucket().name() + "`.email IS NOT MISSING AND v.type='invoice' "
                    + "LIMIT " + ((limit.isPresent()) ? limit.get() : "1000 ")
                    + ((offset.isPresent()) ? " OFFSET " + offset.get() : " ");
        }
        return bucket().async().query(N1qlQuery.simple(query))
                .flatMap(AsyncN1qlQueryResult::rows)
                .map(result -> result.value().toMap())
                .toList()
                .timeout(10, TimeUnit.SECONDS)
                .toBlocking()
                .single();
    }

    @RequestMapping(value = "/sumPayments", method = RequestMethod.GET)
    public Object sumPayments(@RequestParam("email") Optional<String> email,
            @RequestParam("limit") Optional<String> limit,
            @RequestParam("offset") Optional<String> offset) {
        String query;
        if (email.isPresent()) {
            query = "SELECT email,"
                    + "  ARRAY_COUNT(ARRAY v.amount FOR v IN accountHistory WHEN v.type='payment' END) count,"
                    + "  ARRAY_SUM(ARRAY TONUMBER(v.amount) FOR v IN accountHistory WHEN v.type='payment' END) total "
                    + " FROM `" + bucket().name() + "` USE INDEX (sum_payments_by_user) WHERE email='" + email.get() + "'";
        } else {
            query = "SELECT email,"
                    + "  ARRAY_COUNT(ARRAY v.amount FOR v IN accountHistory WHEN v.type='payment' END) count,"
                    + "  ARRAY_SUM(ARRAY TONUMBER(v.amount) FOR v IN accountHistory WHEN v.type='payment' END) total "
                    + " FROM `" + bucket().name() + "` USE INDEX (sum_payments_by_user) WHERE email IS NOT MISSING "
                    + "LIMIT " + ((limit.isPresent()) ? limit.get() : "1000 ")
                    + ((offset.isPresent()) ? " OFFSET " + offset.get() : " ");
        }
        return bucket().async().query(N1qlQuery.simple(query))
                .flatMap(AsyncN1qlQueryResult::rows)
                .map(result -> result.value().toMap())
                .toList()
                .timeout(10, TimeUnit.SECONDS)
                .toBlocking()
                .single();
    }

    @RequestMapping(value = "/createBulk", method = RequestMethod.POST)
    public Object createBulk(@RequestParam("items") int items) {
        final JsonObject content = JsonObject.create().put("item", "A bulk insert test value");
        return Observable
                .range(0, items)
                .flatMap((Integer id) -> {
                    String key1 = "TEST" + id;
                    return bucket().async().upsert(JsonDocument.create(key1, content))
                            .retryWhen(anyOf(BackpressureException.class)
                                    .max(10)
                                    .delay(Delay.exponential(TimeUnit.MILLISECONDS, 10, 1000))
                                    .doOnRetry((Integer integer, Throwable throwable, Long aLong, TimeUnit timeUnit) -> {
                                        LOGGER.warn("Backpressure Exception caught, retrying");
                                    })
                                    .build())
                            .retryWhen(anyOf(TemporaryFailureException.class)
                                    .max(10)
                                    .delay(Delay.exponential(TimeUnit.MILLISECONDS, 10, 1000))
                                    .build())
                            .retryWhen(anyOf(ConnectTimeoutException.class)
                                    .max(5)
                                    .delay(Delay.exponential(TimeUnit.MILLISECONDS, 500, 10000))
                                    .build());
                })
                .count().map((Integer count) -> count + " Items Added Successfully").toBlocking().single();
    }

    @RequestMapping(value = "/readBulk", method = RequestMethod.GET)
    public Object readBulk(@RequestParam("items") int items) {
        final List<JsonObject> results = new ArrayList<>();
        Observable
                .range(0, items)
                .flatMap((Integer id) -> {
                    String key1 = "TEST" + id;
                    return bucket().async().get(JsonDocument.create(key1))
                            .retryWhen(anyOf(BackpressureException.class)
                                    .max(10)
                                    .delay(Delay.exponential(TimeUnit.MILLISECONDS, 10, 1000))
                                    .doOnRetry((Integer integer, Throwable throwable, Long aLong, TimeUnit timeUnit) -> {
                                        LOGGER.warn("Backpressure Exception caught, retrying");
                                    })
                                    .build())
                            .retryWhen(anyOf(TemporaryFailureException.class)
                                    .max(10)
                                    .delay(Delay.exponential(TimeUnit.MILLISECONDS, 10, 1000))
                                    .build())
                            .retryWhen(anyOf(ConnectTimeoutException.class)
                                    .max(5)
                                    .delay(Delay.exponential(TimeUnit.MILLISECONDS, 500, 10000))
                                    .build());
                })
                .map(doc -> results.add(doc.content())).toList().toBlocking().single();
        return results;
    }

    @RequestMapping(value = "/createInvoice", method = RequestMethod.POST)
    public Object createInvoice(@RequestParam("key") String key,
            @RequestParam("business") String business,
            @RequestParam("name") String name,
            @RequestParam("account") String account,
            @RequestParam("amount") String amount) {
        bucket().mutateIn(key).arrayAppend("accountHistory", JsonObject
                .create()
                .put("type", "invoice")
                .put("account", account)
                .put("business", business)
                .put("name", name)
                .put("amount", amount)
                .put("date", String.format("%1$tY-%1$tm-%1$tdT%1$tH:%1$tM:%1$tS.%1$tL%1$tz", new Date())),
                 false)
                .execute();
        return "Invoice " + name + " added to " + key;
    }
}
