import { get, find, omit, cloneDeep } from 'lodash';
import { post, parseJSON } from './xhr';
import { mdToExecutionConfiguration } from './execution/experimental-executions';

const REQUEST_DEFAULTS = {
    types: ['attribute', 'metric', 'fact'],
    paging: {
        offset: 0
    }
};

const LOAD_DATE_DATASET_DEFAULTS = {
    includeUnavailableDateDataSetsCount: true,
    includeAvailableDateAttributes: true
};

function bucketItemsToExecConfig(projectId, mdObj, options = {}) {
    return mdToExecutionConfiguration(projectId, mdObj, options, 'catalogue').then((executionConfig) => {
        const definitions = get(executionConfig, 'definitions');

        return get(executionConfig, 'columns').map((column) => {
            const definition = find(definitions, ({ metricDefinition }) =>
                get(metricDefinition, 'identifier') === column
            );
            const maql = get(definition, 'metricDefinition.expression');

            if (maql) {
                return maql;
            }
            return column;
        });
    });
}

/**
 * Convert specific params in options to "requiredDataSets" structure. For more details look into
 * res file https://github.com/gooddata/gdc-bear/blob/develop/resources/specification/internal/catalog.res
 *
 * @param options Supported keys in options are:
 * <ul>
 * <li>dataSetIdentifier - in value is string identifier of dataSet - this leads to CUSTOM type
 * <li>returnAllDateDataSets - true value means to return ALL values without dataSet differentiation
 * <li>returnAllRelatedDateDataSets - only related date dataSets are loaded across all dataSets
 * <li>by default we get PRODUCTION dataSets
 * </ul>
 * @returns {Object} "requiredDataSets" object hash.
 */
const getRequiredDataSets = (options) => {
    if (get(options, 'returnAllRelatedDateDataSets')) {
        return {};
    }

    if (get(options, 'returnAllDateDataSets')) {
        return { requiredDataSets: { type: 'ALL' } };
    }

    if (get(options, 'dataSetIdentifier')) {
        return { requiredDataSets: {
            type: 'CUSTOM',
            customIdentifiers: [get(options, 'dataSetIdentifier')]
        } };
    }

    return { requiredDataSets: { type: 'PRODUCTION' } };
};

function loadCatalog(projectId, catalogRequest) {
    const uri = `/gdc/internal/projects/${projectId}/loadCatalog`;

    return post(uri, { data: { catalogRequest } })
        .then(parseJSON)
        .then(data => data.catalogResponse);
}

export function loadItems(projectId, options = {}) {
    const request = omit({
        ...REQUEST_DEFAULTS,
        ...options,
        ...getRequiredDataSets(options)
    }, [
        'dataSetIdentifier',
        'returnAllDateDataSets',
        'attributesMap'
    ]);

    const mdObj = get(cloneDeep(options), 'bucketItems');
    const { attributesMap } = options;
    if (mdObj) {
        return bucketItemsToExecConfig(projectId, mdObj, { attributesMap }).then(bucketItems =>
            loadCatalog(projectId,
                {
                    ...request,
                    bucketItems
                }
            )
        );
    }

    return loadCatalog(projectId, request);
}

function requestDateDataSets(projectId, dateDataSetsRequest) {
    const uri = `/gdc/internal/projects/${projectId}/loadDateDataSets`;

    return post(uri, { data: { dateDataSetsRequest } })
        .then(parseJSON)
        .then(data => data.dateDataSetsResponse);
}

export function loadDateDataSets(projectId, options) {
    const mdObj = get(cloneDeep(options), 'bucketItems');
    let bucketItemsPromise;
    if (mdObj) {
        bucketItemsPromise = bucketItemsToExecConfig(projectId, mdObj, { removeDateItems: true, attributesMap: get(options, 'attributesMap') });
    } else {
        bucketItemsPromise = Promise.resolve();
    }
    return bucketItemsPromise.then((bucketItems) => {
        const omittedOptions = ['filter', 'types', 'paging', 'dataSetIdentifier', 'returnAllDateDataSets', 'returnAllRelatedDateDataSets', 'attributesMap'];
        // includeObjectsWithTags has higher priority than excludeObjectsWithTags,
        // so when present omit excludeObjectsWithTags
        if (options.includeObjectsWithTags) {
            omittedOptions.push('excludeObjectsWithTags');
        }

        const request = omit({
            ...LOAD_DATE_DATASET_DEFAULTS,
            ...REQUEST_DEFAULTS,
            ...options,
            ...getRequiredDataSets(options),
            bucketItems
        }, omittedOptions);

        return requestDateDataSets(projectId, request);
    });
}
