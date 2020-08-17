package com.es.helper.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.Objects;

/**
 * @author zhangqi
 * 自动配置注入restHighLevelClient
 * yml里面配置
 */
@Configuration
@ConditionalOnExpression("${es.enabled:true}")
@Slf4j
public class ElasticSearchConfig {

    //注意分隔符是 ,
    @Value("${es.nodes}")
    private String[] nodes;
    @Value("${es.username}")
    private String username;
    @Value("${es.password}")
    private String password;

    @Bean
    public RestClientBuilder restClientBuilder() {
        HttpHost[] hosts = Arrays.stream(nodes)
                .map(this::makeHttpHost)
                .filter(Objects::nonNull)
                .toArray(HttpHost[]::new);
        return RestClient.builder(hosts);
    }


    @Bean(name = "highLevelClient", destroyMethod="close")//调用RestHighLevelClient中的close
    @Scope("singleton")//单列模式，不用自行使用ENUM或者锁进行单列实现
    public RestHighLevelClient highLevelClient(@Autowired RestClientBuilder restClientBuilder) {
        RestHighLevelClient restHighLevelClient = null;
        restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
            httpClientBuilder.setMaxConnTotal(100);
            httpClientBuilder.setMaxConnPerRoute(100);
            return httpClientBuilder;
        });
        restClientBuilder.setRequestConfigCallback(requestConfigBuilder -> {
            requestConfigBuilder.setConnectTimeout(5000);
            requestConfigBuilder.setSocketTimeout(30000);
            requestConfigBuilder.setConnectionRequestTimeout(500);
            return requestConfigBuilder;
        });
        if(!StringUtils.isEmpty(username)) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(username, password));  //es账号密码
            restHighLevelClient = new RestHighLevelClient(
                    restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                        @Override
                        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                            httpClientBuilder.disableAuthCaching();
                            return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        }
                    })
            );
        }else{
            restHighLevelClient = new RestHighLevelClient(restClientBuilder);
        }
        return restHighLevelClient;
    }

    /**
     * : 分割IP和端口,
     * @param s
     * @return
     */
    private HttpHost makeHttpHost(String s) {
        assert org.apache.commons.lang3.StringUtils.isNotEmpty(s);
        String[] address = s.split(":");
        if (address.length == 2) {
            String ip = address[0];
            int port = Integer.parseInt(address[1]);
            return new HttpHost(ip, port, "http");
        } else {
            return null;
        }
    }
}
