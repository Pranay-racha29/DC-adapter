package com.gap.sourcing.order.dcadapter.processor;

import com.gap.sourcing.order.dcadapter.domain.DtoContainer;
import com.gap.sourcing.order.dcadapter.domain.event.BaseEvent;
import com.gap.sourcing.order.dcadapter.domain.lookup.DcItemLookUp;
import com.gap.sourcing.order.dcadapter.domain.lookup.EboWrapper;
import com.gap.sourcing.order.dcadapter.domain.order.Order;
import com.gap.sourcing.order.dcadapter.exception.EboCreationException;
import com.gap.sourcing.order.dcadapter.generator.EboPoGeneratorFactory;
import com.gap.sourcing.order.dcadapter.handler.DcItemLookUpStateHandler;
import com.gap.sourcing.order.dcadapter.helper.DcInformationHelper;
import com.gap.sourcing.order.dcadapter.helper.RepositoryProxyHelper;
import com.gap.sourcing.order.dcadapter.lock.impl.DcItemLookUpLockValidatorService;
import com.gap.sourcing.order.dcadapter.selector.DcItemLookUpSelector;
import com.gap.sourcing.order.dcadapter.service.EboPublisherService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static com.gap.sourcing.order.dcadapter.domain.order.enums.RepositoryProxyType.ORDER_PROXY;
import static net.logstash.logback.argument.StructuredArguments.kv;

@Slf4j
@Component
@AllArgsConstructor
public class FullItemOrderEventProcessor implements EventProcessor {
    private RepositoryProxyHelper repositoryProxyHelper;
    private DcItemLookUpStateHandler dcItemLookUpStateHandler;
    private EboPublisherService eboPublisherService;
    private EboPoGeneratorFactory eboPoGeneratorFactory;
    private DcItemLookUpSelector dcItemLookUpSelector;
    private DcItemLookUpLockValidatorService dcItemLookUpLockValidatorService;

    @Override
    public void process(BaseEvent event) {

        final String orderLink = event.getSourceLink();
        log.info("action=orderToBeRetrieved", kv("link", orderLink));
        Order order = repositoryProxyHelper.fetchResource(ORDER_PROXY, orderLink);
        List<DcItemLookUp> dcItemLookUps = dcItemLookUpSelector.validateAndGet(order,
                DcInformationHelper.buildFromOrder(order));

        try {
            dcItemLookUpLockValidatorService.lockOrUnlockRequest(dcItemLookUps.get(0).getBrandNumber(),
                    dcItemLookUps.get(0).getMarketNumber(),
                    dcItemLookUps.get(0).getChannelNumber(),
                    dcItemLookUps.get(0).getStyleId(),
                    dcItemLookUps.stream().map(dcItemLookup->dcItemLookup.getDcId())
                            .collect(Collectors.toList()),true);

            List<DtoContainer> ebosToPublish = eboPoGeneratorFactory.get(event)
                    .generate(order, dcItemLookUps, event);

            eboPublisherService.publish(ebosToPublish);
            ebosToPublish.forEach(eboWrapperHolder -> dcItemLookUpStateHandler.update(order, eboWrapperHolder));

            List<Exception> errors = ebosToPublish.stream().map(DtoContainer::getEboWrapper)
                    .map(EboWrapper::getErrors)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());

            if (!errors.isEmpty()) {
                log.error("action=finishingOrderProcessorWithErrors", kv("event", event));
                throw new EboCreationException(errors);
            }

        }
        finally {
            dcItemLookUpLockValidatorService.lockOrUnlockRequest(dcItemLookUps.get(0).getBrandNumber(),
                    dcItemLookUps.get(0).getMarketNumber(),
                    dcItemLookUps.get(0).getChannelNumber(),
                    dcItemLookUps.get(0).getStyleId(),
                    dcItemLookUps.stream().map(dcItemLookup->dcItemLookup.getDcId())
                            .collect(Collectors.toList()),false);

        }
    }
}
