package com.gap.sourcing.order.dcadapter.generator;

import com.gap.sourcing.order.dcadapter.builder.PrepackItemPayloadBuilder;
import com.gap.sourcing.order.dcadapter.builder.SkuItemPayloadBuilder;
import com.gap.sourcing.order.dcadapter.creator.ItemEboWrapperCreator;
import com.gap.sourcing.order.dcadapter.domain.DtoContainer;
import com.gap.sourcing.order.dcadapter.domain.EboPayloadHolder;
import com.gap.sourcing.order.dcadapter.domain.ItemPayloadHolder;
import com.gap.sourcing.order.dcadapter.domain.event.BaseEvent;
import com.gap.sourcing.order.dcadapter.domain.lookup.DcItemLookUp;
import com.gap.sourcing.order.dcadapter.domain.lookup.EboWrapper;
import com.gap.sourcing.order.dcadapter.domain.order.Order;
import com.gap.sourcing.order.dcadapter.enhancer.ItemPayloadEnhancer;
import com.gap.sourcing.order.dcadapter.selector.ItemPayloadToLocationGrouping;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static net.logstash.logback.argument.StructuredArguments.kv;

@AllArgsConstructor
@Component
@Slf4j
public class UpdateCreateEboPoGenerator implements EboGenerator<Order> {
    private SkuItemPayloadBuilder skuItemPayloadBuilder;
    private PrepackItemPayloadBuilder prepackItemPayloadBuilder;
    private ItemPayloadToLocationGrouping itemPayloadToLocationGrouping;
    private ItemEboWrapperCreator itemEboWrapperCreator;
    private PoEboGenerator poEboGenerator;
    private ItemPayloadEnhancer itemPayloadEnhancer;

    @Override
    public List<DtoContainer> generate(Order order, List<DcItemLookUp> dcItemLookUps, BaseEvent event) {
        log.info("action=startPoGeneration", kv("eventType", event.getEventType()));
        Map<String, ItemPayloadHolder> eligibleItemPayloadsBySkuId = skuItemPayloadBuilder
                .buildByItemId(dcItemLookUps, order);
        Map<String, ItemPayloadHolder> eligibleItemPayloadByPrepackId = prepackItemPayloadBuilder
                .buildByItemId(dcItemLookUps, order);

        List<EboPayloadHolder> eboPayloadHolders = itemPayloadToLocationGrouping
                .group(dcItemLookUps, eligibleItemPayloadsBySkuId, eligibleItemPayloadByPrepackId);

        List<DtoContainer> dtoContainers = eboPayloadHolders.stream()
                .map(eboPayloadHolder -> {
                        eboPayloadHolder.getSkuHolder()
                                .forEach(itemPayloadHolder -> itemPayloadEnhancer.enhance(itemPayloadHolder, order));
                        eboPayloadHolder.getPrepackHolder()
                                .forEach(itemPayloadHolder -> itemPayloadEnhancer.enhance(itemPayloadHolder, order));

                    return itemEboWrapperCreator.create(order, event, eboPayloadHolder);
                })
                .collect(Collectors.toList());

        if (!hasError(dtoContainers)) {
            DcItemLookUp dcItemLookUp = dcItemLookUps.stream()
                    .filter(itemLookUp -> itemLookUp.getLocationNumber().equals(order.getLocationNumber()))
                    .findFirst().orElseThrow(() -> new IllegalStateException("No DcItemLookUp for dc" +
                            order.getLocationNumber() + " found. OrderNumber: " + order.getOrderNumber()));

            poEboGenerator.generate(dtoContainers, order, event, dcItemLookUp);
        }

        return dtoContainers;
    }

    private boolean hasError(List<DtoContainer> dtoContainers) {
        return dtoContainers.stream()
                .map(DtoContainer::getEboWrapper)
                .anyMatch(EboWrapper::hasErrors);
    }
}
