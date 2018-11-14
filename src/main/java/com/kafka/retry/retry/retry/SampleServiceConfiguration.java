package com.kafka.retry.retry.retry;

import com.kafka.retry.retry.consumer.RetryStateMachine;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.ResolvableType;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.ProducerListener;

import java.util.Arrays;
import java.util.List;

@Configuration
@Import({RetryStateMachine.class})
public class SampleServiceConfiguration implements ImportBeanDefinitionRegistrar,BeanFactoryAware {


    KafkaTemplate<String,String> kafkaTemplate;

    @Autowired
    RetryStateMachine retryStateMachine;


    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
        retryStateMachine.getMinIntervalList().stream().forEach(
                interval -> register(registry, String.valueOf(interval),kafkaTemplate)
        );
    }

    private void register(BeanDefinitionRegistry registry, String topicName, KafkaTemplate<String, String> kafkaTemplate){
        BeanDefinition bd = new RootBeanDefinition(RetryProcess.class);
        bd.getConstructorArgumentValues()
                .addGenericArgumentValue("retry"+topicName);


        bd.getConstructorArgumentValues()
                .addGenericArgumentValue(kafkaTemplate);

        registry.registerBeanDefinition(topicName+"MinutesIntervalRetry", bd);
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        retryStateMachine = beanFactory.getBean(RetryStateMachine.class);
        Object bean1 = beanFactory.getBean(ResolvableType.forClassWithGenerics(ProducerFactory.class, Object.class, Object.class).getType();
        String typeName = ResolvableType.forClassWithGenerics(KafkaTemplate.class, Object.class, Object.class).getType().getTypeName();
        Object bean = beanFactory.getBean(typeName);
    }
}
