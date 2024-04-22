package org.sj4axao.stater.kudu.config;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.SessionConfiguration;
import org.sj4axao.stater.kudu.client.KuduImpalaTemplate;
import org.sj4axao.stater.kudu.client.KuduTemplate;
import org.sj4axao.stater.kudu.client.impl.PlainKuduImpalaTemplate;
import org.sj4axao.stater.kudu.client.impl.PlainKuduTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@Configuration
@EnableConfigurationProperties({KuduProperties.class})
public class KuduClientAutoConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(KuduClientAutoConfiguration.class);

    @Bean
    @ConditionalOnProperty({"kudu.kudu-address"})
    public KuduProperties kuduProperties() {
        return new KuduProperties();
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnBean({KuduProperties.class})
    public KuduClient kuduClient(@Qualifier("kuduProperties") KuduProperties kuduProperties) throws IOException, InterruptedException {
        List<String> masterAddr = Arrays.asList(kuduProperties.getKuduAddress().split(","));
        logger.info("kuduClient{}", masterAddr);
        KuduClient kuduClient = (new KuduClient.KuduClientBuilder(masterAddr)).build();
        logger.info("kuduClient");
        return kuduClient;
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnBean({KuduClient.class})
    public KuduSession kuduSession(@Qualifier("kuduClient") KuduClient kuduClient, KuduProperties kuduProperties) {
        KuduSession kuduSession = kuduClient.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        kuduSession.setIgnoreAllDuplicateRows(kuduProperties.isIgnoreDuplicateRows());
        kuduSession.setMutationBufferSpace(kuduProperties.getFlushBufferSize());
        return kuduSession;
    }

    @Bean
    @ConditionalOnBean({KuduSession.class})
    public KuduTemplate KuduTemplate(KuduClient kuduClient, KuduSession kuduSession, KuduProperties kuduProperties) {
        return (KuduTemplate) new PlainKuduTemplate(kuduClient, kuduSession, kuduProperties);
    }

    @Bean
    @ConditionalOnBean({KuduSession.class})
    public KuduImpalaTemplate KuduImpalaTemplate(KuduTemplate kuduTemplate) {
        return (KuduImpalaTemplate) new PlainKuduImpalaTemplate(kuduTemplate);
    }
}
