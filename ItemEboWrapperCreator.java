package com.gap.sourcing.order.dcadapter.creator;

import com.gap.sourcing.order.dcadapter.domain.DtoContainer;
import com.gap.sourcing.order.dcadapter.domain.EboPayloadHolder;
import com.gap.sourcing.order.dcadapter.domain.event.BaseEvent;
import com.gap.sourcing.order.dcadapter.domain.lookup.DcItemLookUp;
import com.gap.sourcing.order.dcadapter.domain.lookup.EboWrapper;
import com.gap.sourcing.order.dcadapter.domain.order.Order;
import com.gap.sourcing.order.dcadapter.domain.order.Prepack;
import com.gap.sourcing.order.dcadapter.domain.order.Sku;
import com.gap.sourcing.order.dcadapter.lookup.DcItemHelper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
@Component
@Slf4j
public class ItemEboWrapperCreator {
    private final DcItemHelper dcItemHelper;
    private final SkuItemEboCreator skuEboCreator;
    private final PrepackEboCreator prepackEboCreator;

    public DtoContainer create(Order order, BaseEvent event, EboPayloadHolder eboPayloadHolder) {
        DcItemLookUp dcItemLookUp = eboPayloadHolder.getDcItemLookUp();

        List<Sku> missingSkus = dcItemHelper
                .getMissingSkus(order.getSkus(), dcItemLookUp.getSkuItems());
        List<Sku> skus = new ArrayList<>(missingSkus);
        List<Prepack> missingPrepacks = dcItemHelper
                .getMissingPrepacks(order.getPrepacks(), dcItemLookUp.getPrepackItems());
        List<Prepack> prepacks = new ArrayList<>(missingPrepacks);

        EboWrapper skuEboWrapper = skuEboCreator.create(event, eboPayloadHolder);
        EboWrapper prepackEboWrapper = prepackEboCreator.create(event, eboPayloadHolder);

        List<Exception> itemExceptions = new ArrayList<>();
        itemExceptions.addAll(skuEboWrapper.getErrors());
        itemExceptions.addAll(prepackEboWrapper.getErrors());

        EboWrapper eboWrapper = EboWrapper.builder()
                .prepackEbos(prepackEboWrapper.getPrepackEbos())
                .skusEbos(skuEboWrapper.getSkusEbos())
                .errors(itemExceptions)
                .build();

        return DtoContainer.builder()
                .locationNumber(dcItemLookUp.getLocationNumber())
                .dcItemLookUp(eboPayloadHolder.getDcItemLookUp())
                .eboWrapper(eboWrapper)
                .skuList(skus)
                .prepacks(prepacks)
                .build();
    }
}
