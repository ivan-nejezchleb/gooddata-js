import { get } from '../xhr';
import * as routes from './routes';

const transformClient = (item) => {
    const { contractId, dataProductId, domainId, segmentId } =
        routes.parse(item.client.links.self, routes.CONTRACT_DATA_PRODUCT_DOMAIN_SEGMENT_CLIENT);

    return {
        contractId,
        dataProductId,
        domainId,
        segmentId,
        ...item.client
    };
};

export const getClients = (contractId, dataProductId, segmentId, domainId, filter, paging) => {
    const query = filter ? { clientPrefix: filter, stats: 'user' } : { stats: 'user' };
    const uri = paging ?
        paging.next :
        routes.interpolate(
            routes.CONTRACT_DATA_PRODUCT_DOMAIN_SEGMENT_CLIENTS,
            { contractId, dataProductId, segmentId, domainId },
            query
        );

    if (uri) {
        return get(uri).then(result => ({
            items: result.client.items.map(transformClient),
            paging: result.client.paging
        }));
    }

    return Promise.resolve({ items: [], paging: {} });
};

const transformClientUser = (item) => {
    const user = item.projectUser;
    return {
        id: user.login,
        fullName: `${user.firstName} ${user.lastName}`,
        ...user
    };
};

export const getClientUsers = (contractId, dataProductId, domainId, segmentId, clientId, query, paging) => {
    if (paging && !paging.next) {
        return Promise.resolve({ items: [], paging: {} });
    }

    const uri = paging ?
        paging.next :
        routes.interpolate(
            routes.CONTRACT_DATA_PRODUCT_DOMAIN_SEGMENT_CLIENT_USERS,
            { contractId, dataProductId, domainId, segmentId, clientId },
            query
        );

    return get(uri).then(result => ({
        ...result.clientProjectUsers,
        items: result.clientProjectUsers.items.map(transformClientUser)
    }));
};
