package com.gap.sourcing.order.dcadapter.processor;

import com.gap.sourcing.order.dcadapter.domain.lookup.DcItemLookUp;
import com.gap.sourcing.order.dcadapter.domain.lookup.DcItemLookUpRetry;
import com.gap.sourcing.order.dcadapter.domain.lookup.DcItemPrepack;
import com.gap.sourcing.order.dcadapter.domain.lookup.DcItemSku;
import com.gap.sourcing.order.dcadapter.domain.on.demand.ItemRequest;
import com.gap.sourcing.order.dcadapter.domain.on.demand.ItemType;
import com.gap.sourcing.order.dcadapter.domain.on.demand.OnDemandError;
import com.gap.sourcing.order.dcadapter.domain.on.demand.OnDemandResponse;
import com.gap.sourcing.order.dcadapter.domain.order.Order;
import com.gap.sourcing.order.dcadapter.domain.order.Style;
import com.gap.sourcing.order.dcadapter.helper.DcItemLookUpServiceHelper;
import com.gap.sourcing.order.dcadapter.repository.DcItemLookUpRetryRepository;
import com.gap.sourcing.order.dcadapter.service.on.demand.ItemOnDemandService;
import com.newrelic.api.agent.Trace;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@AllArgsConstructor
@Component
@Slf4j
public class AsyncEboProcessor {
    private DcItemLookUpRetryRepository dcItemLookUpRetryRepository;
    private ItemOnDemandService skuOnDemandService;
    private ItemOnDemandService prepackOnDemandService;
    private DcItemLookUpServiceHelper dcItemLookUpServiceHelper;

    @Async
    @Trace(async = true, dispatcher = true)
    public void reprocessEbos(Order order) {
        Optional<Order> optionalOrder = Optional.ofNullable(order);
        String styleId = optionalOrder
                .map(Order::getStyle)
                .map(Style::getId)
                .orElse(null);
        if (styleId == null) {
            log.error("action=styleIdNotPresent");
            return;
        }
        String locationNumber = optionalOrder.map(Order::getLocationNumber).orElse(null);
        if (locationNumber == null) {
            log.error("action=locationNumberNotPresent");
            return;
        }
        Optional<DcItemLookUpRetry> optionalDcItemLookUpRetry = dcItemLookUpRetryRepository
                .findDcItemLookUpByDcIdAndStyleId(locationNumber, styleId);
        if (!optionalDcItemLookUpRetry.isPresent()) {
            log.info("action=nothingToReprocess");
            return;
        }
        DcItemLookUpRetry dcItemLookUpRetry = optionalDcItemLookUpRetry.get();
        dcItemLookUpRetry.setLastProcessedOrderNumber(order.getOrderNumber());
        dcItemLookUpRetryRepository.save(dcItemLookUpRetry);
        List<String> skuItemNumbers = dcItemLookUpRetry.getSkuItems()
                .stream()
                .map(DcItemSku::getSkuNumber)
                .collect(Collectors.toList());
        ItemRequest skuItemRequest = ItemRequest
                .builder()
                .itemNumbers(skuItemNumbers)
                .itemType(ItemType.SKU)
                .locationNumber(locationNumber)
                .build();
        OnDemandResponse skuOnDemandResponse = skuOnDemandService.processItems(skuItemRequest, false);

        List<String> prepackItemNumbers = dcItemLookUpRetry.getPrepackItems()
                .stream()
                .map(DcItemPrepack::getId)
                .collect(Collectors.toList());
        ItemRequest prepackItemRequest = ItemRequest
                .builder()
                .itemNumbers(prepackItemNumbers)
                .itemType(ItemType.PREPACK)
                .locationNumber(locationNumber)
                .build();
        OnDemandResponse prepackOnDemandResponse = prepackOnDemandService.processItems(prepackItemRequest, false);
        DcItemLookUp dcItemLookUp = DcItemLookUp
                .builder()
                .id(dcItemLookUpRetry.getId())
                .dcId(dcItemLookUpRetry.getDcId())
                .styleId(dcItemLookUpRetry.getStyleId())
                .universalStyleNumber(dcItemLookUpRetry.getUniversalStyleNumber())
                .brandNumber(dcItemLookUpRetry.getBrandNumber())
                .marketNumber(dcItemLookUpRetry.getMarketNumber())
                .channelNumber(dcItemLookUpRetry.getChannelNumber())
                .skuItems(dcItemLookUpRetry.getSkuItems()
                        .stream()
                        .filter(dcItemSku -> skuOnDemandResponse.getSuccess().contains(dcItemSku.getSkuNumber()))
                        .collect(Collectors.toList()))
                .prepackItems(dcItemLookUpRetry.getPrepackItems()
                        .stream()
                        .filter(dcItemPrepack -> prepackOnDemandResponse.getSuccess()
                                .contains(dcItemPrepack.getId()))
                        .collect(Collectors.toList()))
                .lastProcessedOrderNumber(dcItemLookUpRetry.getLastProcessedOrderNumber())
                .lastUpdateDate(dcItemLookUpRetry.getLastUpdateDate())
                .createdDate(dcItemLookUpRetry.getCreatedDate())
                .build();
        if (!dcItemLookUp.getSkuItems().isEmpty() || !dcItemLookUp.getPrepackItems().isEmpty()) {
            dcItemLookUpServiceHelper.retryableSave(dcItemLookUp);
        }
        dcItemLookUpRetry = dcItemLookUpRetry.toBuilder()
                .skuItems(dcItemLookUpRetry.getSkuItems()
                        .stream()
                        .filter(dcItemSku -> skuOnDemandResponse.getErrors()
                                .stream()
                                .map(OnDemandError::getItemNumber)
                                .anyMatch(itemNumber -> itemNumber.equals(dcItemSku.getSkuNumber())))
                        .collect(Collectors.toList()))
                .prepackItems(dcItemLookUpRetry.getPrepackItems()
                        .stream()
                        .filter(dcItemPrepack -> prepackOnDemandResponse.getErrors()
                                .stream()
                                .map(OnDemandError::getItemNumber)
                                .anyMatch(itemNumber -> itemNumber.equals(dcItemPrepack.getPrepackNumber())))
                        .collect(Collectors.toList()))
                .build();
        if (dcItemLookUpRetry.getSkuItems().isEmpty() && dcItemLookUpRetry.getPrepackItems().isEmpty()) {
            dcItemLookUpRetryRepository.deleteById(dcItemLookUpRetry.getId());
        } else {
            dcItemLookUpRetryRepository.save(dcItemLookUpRetry);
        }
    }
}
