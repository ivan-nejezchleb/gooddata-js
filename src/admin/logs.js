import { get } from '../xhr';
import * as routes from './routes';

export const getSyncLog = (contractId, dataProductId, domainId, segmentId) =>
    get(routes.interpolate(routes.CONTRACT_DATA_PRODUCT_DOMAIN_SEGMENTS_DOMAIN_SYNCLOG, {
        contractId,
        dataProductId,
        domainId,
        segmentId
    })).then(data => ({
        log: data.log.log
    }));

export const getChangeLog = (contractId, dataProductId, domainId, segmentId) =>
    get(routes.interpolate(routes.CONTRACT_DATA_PRODUCT_DOMAIN_SEGMENTS_DOMAIN_CHANGELOG, {
        contractId,
        dataProductId,
        domainId,
        segmentId
    })).then(data => ({
        log: data.log.log
    }));
