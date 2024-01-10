package com.gap.sourcing.order.dcadapter.service;

import com.gap.sourcing.order.dcadapter.domain.DtoContainer;
import com.gap.sourcing.order.dcadapter.domain.lookup.AvroDto;
import com.gap.sourcing.order.dcadapter.domain.lookup.EboDto;
import com.gap.sourcing.order.dcadapter.domain.lookup.EboWrapper;
import com.gap.sourcing.order.dcadapter.processor.AsyncAvroPublisherProcessor;
import com.gap.sourcing.order.dcadapter.processor.AsyncEboPublisherProcessor;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@Service
@AllArgsConstructor
@Slf4j
public class EboPublisherService {

    private AsyncEboPublisherProcessor asyncEboPublisherProcessor;
    private AsyncAvroPublisherProcessor asyncAvroPublisherProcessor;

    public void publish(List<DtoContainer> dtoContainers) {
        List<EboDto> eboDtos = dtoContainers.stream()
                .map(DtoContainer::getEboWrapper)
                .map(EboWrapper::getAllXmlEbos)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        asyncEboPublisherProcessor.publish(eboDtos);
    }

    public void publishAvro(List<DtoContainer> dtoContainers) {
        List<AvroDto> avroDtos = dtoContainers.stream()
                .map(DtoContainer::getEboWrapper)
                .map(EboWrapper::getAllAvroDtos)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        asyncAvroPublisherProcessor.publish(avroDtos);
    }
}
