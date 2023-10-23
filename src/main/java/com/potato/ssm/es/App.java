package com.potato.ssm.es;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.Buckets;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import co.elastic.clients.elasticsearch.sql.QueryRequest;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Hello world!
 */
public class App {

    private String host = "192.168.229.130";
    private Integer port = 9200;
    private String http = "http";
    private String username = "elastic";
    private String password = "Qw123456.";

    public static void main(String[] args) throws IOException {
        App app = new App();
        ElasticsearchClient client = app.elasticsearchClient();
//        Product product = new Product("mgl", "Bat", 4288);
//        IndexResponse response = client.index(builder -> builder.index("products")
//                .id(product.getId())
//                .document(product));
//        System.out.println(response.result());
//        System.out.println(response.version());

//        Product product1 = new Product("qrf", "Phone", 14299);
//        IndexRequest.Builder<Product> builder = new IndexRequest.Builder<Product>().index("products").id(product1.getId())
//                .document(product1);
//        IndexResponse response1 = client.index(builder.build());
//        System.out.println(response1.result());
//        System.out.println(response1.version());
//        List<Product> products = Arrays.asList(
//                new Product("lgk", "Pad", 6500),
//                new Product("tsl", "Glass", 878),
//                new Product("dlg", "Cloth", 230)
//        );
//        //新建bulk请求
//        BulkRequest.Builder br = new BulkRequest.Builder();
//        for (Product product : products) {
//            //为bulk请求添加操作，可以添加index,delete,create,update请求
//            br.operations(builder -> builder.index(builder1 -> builder1.index("products")
//                    .id(product.getId()).document(product)));
//        }
//        br.operations(builder -> builder.delete(builder1 -> builder1.index("products")
//                .id("jkl")));
//        br.operations(builder -> builder.delete(builder1 -> builder1.index("products")
//                .id("dkjv")));
//        BulkResponse bulkResponse = client.bulk(br.build());
//        if (bulkResponse.errors()) {
//            System.out.println("bulk有错误");
//            List<BulkResponseItem> items = bulkResponse.items();
//            for (BulkResponseItem item : items) {
//                if (null != item.error()) {
//                    System.out.println("出错id：" + item.id());
//                    System.out.println("出错原因：" + item.error().reason());
//                }
//            }
//        }
//        //products/_doc/id
//        GetResponse<Product> getResponse = client.get(
//                builder -> builder.index("products").id("abc"), Product.class);
//        if(getResponse.found()) {
//            System.out.println(getResponse.source().toString());
//        } else {
//            System.out.println("找不到该产品");
//        }
        //products/_search {"query": {"match":{"name": "..."}}}
//        SearchResponse<ObjectNode> searchResponse = client.search(builder -> builder.query(
//                q -> q.match(
//                        m -> m.field("name").query("Bag")
//                )
//        ), ObjectNode.class);
//        SearchResponse<Product> searchResponse1 = client.search(builder -> builder.query(
//                q -> q.multiMatch(
//                        m -> m.fields(Arrays.asList("price", "name")).query("42"))
//        ), Product.class);
//        TotalHits total = searchResponse.hits().total();
//        if (total.relation().equals(TotalHitsRelation.Eq)) {
//            System.out.println("搜索到" + total.value() + "条数据");
//        } else {
//            System.out.println("搜索到超过" + total.value() + "条数据");
//        }
//        if (total.value() > 0) {
//            List<Hit<ObjectNode>> hits = searchResponse.hits().hits();
//            for (Hit<ObjectNode> hit : hits) {
//                ObjectNode json = hit.source();
//                System.out.println(json.get("id").asText());
//            }
//        }
//
//        TotalHits total1 = searchResponse1.hits().total();
//        if (total1.relation().equals(TotalHitsRelation.Eq)) {
//            System.out.println("搜索到" + total1.value() + "条数据");
//        } else {
//            System.out.println("搜索到超过" + total1.value() + "条数据");
//        }
//        if (total.value() > 0) {
//            List<Hit<Product>> hits = searchResponse1.hits().hits();
//            for (Hit<Product> hit : hits) {
//                System.out.println(hit.toString());
//            }
//        }
//        //要转型才能放入bool查询条件
//        Query matchQueryByName = MatchQuery.of(q -> q.field("name").query("Pad"))._toQuery();
//        Query rangeQueryByPrice = RangeQuery.of(r -> r.field("price").lte(JsonData.of(200)))._toQuery();
//        SearchResponse<Product> searchResponse2 = client.search(builder -> builder.query(q -> q.bool(
//                b -> b.must(matchQueryByName).mustNot(rangeQueryByPrice)
//        )), Product.class);
        SearchResponse<Product> searchResponse3 = client.search(builder -> builder.index("products").aggregations(
                "byNameAgg", q -> q.terms(t -> t.field("name.keyword"))
        ), Product.class);
        Aggregate byNameAgg = searchResponse3.aggregations().get("byNameAgg");
        List<StringTermsBucket> termsBuckets = byNameAgg.sterms().buckets().array();
        for (StringTermsBucket termsBucket : termsBuckets) {
            System.out.println("name为：" + termsBucket.key()._toJsonString() + "的桶有" + termsBucket.docCount() + "个产品");
        }
    }

    public ElasticsearchClient elasticsearchClient() {
        final BasicCredentialsProvider basicCredentialsProvider = new BasicCredentialsProvider();
        basicCredentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        RestClient restClient = RestClient.builder(new HttpHost(host, port, http))
                .setHttpClientConfigCallback(httpAsyncClientBuilder ->
                        httpAsyncClientBuilder.setDefaultCredentialsProvider(basicCredentialsProvider))
                .build();
        RestClientTransport restClientTransport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        return new ElasticsearchClient(restClientTransport);

    }
}
