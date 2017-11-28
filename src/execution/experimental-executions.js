// Copyright (C) 2007-2014, GoodData(R) Corporation. All rights reserved.
import md5 from 'md5';
import invariant from 'invariant';
import {
    compact,
    filter,
    first,
    find,
    map,
    merge,
    every,
    get,
    isEmpty,
    isString,
    negate,
    last,
    assign,
    partial,
    flatten,
    omit,
    set
} from 'lodash';

import {
    ajax,
    post,
    get as xhrGet,
    parseJSON
} from '../xhr';

import {
    getObjects
} from '../metadata';

import Rules from '../utils/rules';
import { sortDefinitions } from '../utils/definitions';

const notEmpty = negate(isEmpty);

const findHeaderForMappingFn = (mapping, header) =>
    ((mapping.element === header.id || mapping.element === header.uri) &&
        header.measureIndex === undefined);


const wrapMeasureIndexesFromMappings = (metricMappings, headers) => {
    if (metricMappings) {
        metricMappings.forEach((mapping) => {
            const header = find(headers, partial(findHeaderForMappingFn, mapping));
            if (header) {
                header.measureIndex = mapping.measureIndex;
                header.isPoP = mapping.isPoP;
            }
        });
    }
    return headers;
};

const emptyResult = {
    extendedTabularDataResult: {
        values: [],
        warnings: []
    }
};

function loadExtendedDataResults(uri, settings, prevResult = emptyResult) {
    return new Promise((resolve, reject) => {
        ajax(uri, settings)
            .then((r) => {
                if (r.status === 204) {
                    return {
                        status: r.status,
                        result: ''
                    };
                }

                return r.json().then((result) => {
                    return {
                        status: r.status,
                        result
                    };
                });
            })
            .then(({ status, result }) => {
                const values = [
                    ...get(prevResult, 'extendedTabularDataResult.values', []),
                    ...get(result, 'extendedTabularDataResult.values', [])
                ];

                const warnings = [
                    ...get(prevResult, 'extendedTabularDataResult.warnings', []),
                    ...get(result, 'extendedTabularDataResult.warnings', [])
                ];

                const updatedResult = merge({}, prevResult, {
                    extendedTabularDataResult: {
                        values,
                        warnings
                    }
                });

                const nextUri = get(result, 'extendedTabularDataResult.paging.next');
                if (nextUri) {
                    resolve(loadExtendedDataResults(nextUri, settings, updatedResult));
                } else {
                    resolve({ status, result: updatedResult });
                }
            }, reject);
    });
}

/**
 * Module for execution on experimental execution resource
 *
 * @class execution
 * @module execution
 */

/**
 * For the given projectId it returns table structure with the given
 * elements in column headers.
 *
 * @method getData
 * @param {String} projectId - GD project identifier
 * @param {Array} columns - An array of attribute or metric identifiers.
 * @param {Object} executionConfiguration - Execution configuration - can contain for example
 *                 property "where" containing query-like filters
 *                 property "orderBy" contains array of sorted properties to order in form
 *                      [{column: 'identifier', direction: 'asc|desc'}]
 * @param {Object} settings - Supports additional settings accepted by the underlying
 *                             xhr.ajax() calls
 *
 * @return {Object} Structure with `headers` and `rawData` keys filled with values from execution.
 */
export function getData(projectId, columns, executionConfiguration = {}, settings = {}) {
    const executedReport = {
        isLoaded: false
    };

    // Create request and result structures
    const request = {
        execution: { columns }
    };
    // enrich configuration with supported properties such as
    // where clause with query-like filters
    ['where', 'orderBy', 'definitions'].forEach((property) => {
        if (executionConfiguration[property]) {
            request.execution[property] = executionConfiguration[property];
        }
    });

    // Execute request
    return post(`/gdc/internal/projects/${projectId}/experimental/executions`, {
        ...settings,
        body: JSON.stringify(request)
    })
        .then(parseJSON)
        .then((result) => {
            executedReport.headers = wrapMeasureIndexesFromMappings(
                get(executionConfiguration, 'metricMappings'), result.executionResult.headers);

            // Start polling on url returned in the executionResult for tabularData
            return loadExtendedDataResults(result.executionResult.extendedTabularDataResult, settings);
        })
        .then((r) => {
            const { result, status } = r;

            return Object.assign({}, executedReport, {
                rawData: get(result, 'extendedTabularDataResult.values', []),
                warnings: get(result, 'extendedTabularDataResult.warnings', []),
                isLoaded: true,
                isEmpty: status === 204
            });
        });
}

const MAX_TITLE_LENGTH = 1000;

const getMetricTitle = (suffix, title) => {
    const maxLength = MAX_TITLE_LENGTH - suffix.length;
    if (title && title.length > maxLength) {
        if (title[title.length - 1] === ')') {
            return `${title.substring(0, maxLength - 2)}…)${suffix}`;
        }
        return `${title.substring(0, maxLength - 1)}…${suffix}`;
    }
    return `${title}${suffix}`;
};

const getBaseMetricTitle = partial(getMetricTitle, '');

const POP_SUFFIX = ' - previous year';
const getPoPMetricTitle = partial(getMetricTitle, POP_SUFFIX);

const CONTRIBUTION_METRIC_FORMAT = '#,##0.00%';

function getDefinition(measure) {
    return get(measure, ['definition', 'measureDefinition'], {});
}

function getMeasureFilters(measure) {
    return get(getDefinition(measure), 'filters', []);
}

function getAggregation(measure) {
    return get(getDefinition(measure), 'aggregation', '').toLowerCase();
}


function isEmptyFilter(metricFilter) {
    if (get(metricFilter, 'positiveAttributeFilter')) {
        return isEmpty(get(metricFilter, ['positiveAttributeFilter', 'in']));
    }
    if (get(metricFilter, 'negativeAttributeFilter')) {
        return isEmpty(get(metricFilter, ['negativeAttributeFilter', 'notIn']));
    }
    if (get(metricFilter, 'absoluteDateFilter')) {
        return get(metricFilter, ['absoluteDateFilter', 'from']) === undefined && get(metricFilter, ['absoluteDateFilter', 'to']) === undefined;
    }
    return get(metricFilter, ['relativeDateFilter', 'from']) === undefined && get(metricFilter, ['relativeDateFilter', 'to']) === undefined;
}

const allFiltersEmpty = item => every(map(
    getMeasureFilters(item),
    f => isEmptyFilter(f)
));

const isDerived = (measure) => {
    const aggregation = getAggregation(measure);
    return (aggregation !== '' || !allFiltersEmpty(measure));
};

function isAttrFilter(measureFilter) {
    return get(measureFilter, 'positiveAttributeFilter') || get(measureFilter, 'negativeAttributeFilter');
}

function getAttrFilterExpression(measureFilter) {
    const isNegative = get(measureFilter, 'negativeAttributeFilter', false);
    const detailPath = isNegative ? 'negativeAttributeFilter' : 'positiveAttributeFilter';
    const elementsPath = [detailPath, isNegative ? 'notIn' : 'in'];
    const attributeUri = get(measureFilter, [detailPath, 'displayForm']); // TODO DF -> attr URI
    const elements = get(measureFilter, elementsPath, []);
    if (isEmpty(elements)) {
        return null;
    }
    const elementsForQuery = map(elements, e => `[${e}]`);
    const negative = isNegative ? 'NOT ' : '';

    return `[${attributeUri}] ${negative}IN (${elementsForQuery.join(',')})`;
}

function getDateFilterExpression() {
    // TODO
    return '';
}

const getFilterExpression = (measureFilter) => {
    if (isAttrFilter(measureFilter)) {
        return getAttrFilterExpression(measureFilter);
    }
    return getDateFilterExpression(measureFilter);
};

const getGeneratedMetricExpression = (item) => {
    const aggregation = getAggregation(item).toUpperCase();
    const objectUri = get(getDefinition(item), 'item');
    const where = filter(map(getMeasureFilters(item), getFilterExpression), e => !!e);

    return `SELECT ${aggregation ? `${aggregation}([${objectUri}])` : `[${objectUri}]`
    }${notEmpty(where) ? ` WHERE ${where.join(' AND ')}` : ''}`;
};

const getPercentMetricExpression = ({ visualizationAttribute }, measure) => {
    let metricExpressionWithoutFilters = `SELECT [${get(getDefinition(measure), 'item')}]`;

    if (isDerived(measure)) {
        metricExpressionWithoutFilters = getGeneratedMetricExpression(omit(measure, ['definition', 'measureDefinition', 'filters']));
    }

    const attributeUri = get(visualizationAttribute, 'displayForm'); // TODO DF -> attr URI
    const whereFilters = filter(map(getMeasureFilters(measure), getFilterExpression), e => !!e);
    const whereExpression = notEmpty(whereFilters) ? ` WHERE ${whereFilters.join(' AND ')}` : '';

    return `SELECT (${metricExpressionWithoutFilters}${whereExpression}) / (${metricExpressionWithoutFilters} BY ALL [${attributeUri}]${whereExpression})`;
};

const getPoPExpression = (attribute, metricExpression) => {
    const attributeUri = get(attribute, 'attribute');

    return `SELECT ${metricExpression} FOR PREVIOUS ([${attributeUri}])`;
};

const getGeneratedMetricHash = (title, format, expression) => md5(`${expression}#${title}#${format}`);

function getMeasureType(measure) {
    const aggregation = getAggregation(measure);
    if (aggregation === '') {
        return 'metric';
    } else if (aggregation === 'count') {
        return 'attribute';
    }
    return 'fact';
}

const getGeneratedMetricIdentifier = (item, aggregation, expressionCreator, hasher) => {
    const [, , , prjId, , id] = get(getDefinition(item), 'item', '').split('/');
    const identifier = `${prjId}_${id}`;
    const hash = hasher(expressionCreator(item));
    const hasNoFilters = isEmpty(getMeasureFilters(item));
    const type = getMeasureType(item);

    const prefix = (hasNoFilters || allFiltersEmpty(item)) ? '' : '_filtered';

    return `${type}_${identifier}.generated.${hash}${prefix}_${aggregation}`;
};

const isDateFilter = ({ dateFilter }) => dateFilter;

function isMeasure(bucketItem) {
    return get(bucketItem, 'measure') !== undefined;
}
function isCategory(bucketItem) {
    return get(bucketItem, 'visualizationAttribute') !== undefined;
}

function getMeasures(buckets) {
    return buckets.reduce((measuresList, bucket) =>
        get(bucket, 'items').reduce((list, bucketItem) => {
            if (isMeasure(bucketItem)) {
                list.push(get(bucketItem, 'measure'));
            }
            return list;
        }, measuresList)
        , []);
}

function getCategories(buckets) {
    return buckets.reduce((categoriesList, bucket) =>
        get(bucket, 'items').reduce((list, bucketItem) => {
            if (isCategory(bucketItem)) {
                list.push(get(bucketItem, 'visualizationAttribute'));
            }
            return list;
        }, categoriesList)
        , []);
}

const getFilters = ({ filters }) => filters;

function isDateCategory(category, attributesMap = {}) {
    return get(get(attributesMap, get(category, 'displayForm'), {}), ['attribute', 'content', 'type']) !== undefined;
}

const getDateCategory = (mdObj) => {
    const category = find(getCategories(mdObj), isDateCategory);

    return get(category, 'category');
};


const getDateFilter = (mdObj) => {
    const dateFilter = find(getFilters(mdObj), isDateFilter);

    return get(dateFilter, 'dateFilter');
};

const getDate = mdObj => (getDateCategory(mdObj) || getDateFilter(mdObj));

const getMetricSort = (sort, isPoPMetric) => {
    if (isString(sort)) {
        // TODO: backward compatibility, remove when not used plain "sort: asc | desc" in measures
        return sort;
    }

    const sortByPoP = get(sort, 'sortByPoP');
    if ((isPoPMetric && sortByPoP) || (!isPoPMetric && !sortByPoP)) {
        return get(sort, 'direction');
    }
    return null;
};

function getMeasureSorting(measure, mdObj) {
    const sorting = get(mdObj, ['properties', 'GdcProperties', 'sorts']); // TODO check this
    const sortItem = get(sorting, [0]); // TODO only one sortItem now?
    const measureSortItem = get(sortItem, ['measureSortItem']);
    if (measureSortItem) {
        const identifier = get(measureSortItem, ['locators', 0, 'measureLocatorItem', 'measureIdentifier']); // TODO only one item now?
        if (identifier === get(measure, 'localIdentifier')) {
            return get(measureSortItem, 'direction', null);
        }
    }
    return null;
}

function getCategorySorting(category, mdObj) {
    const sorting = get(mdObj, ['properties', 'GdcProperties', 'sorts']);
    const sortItem = get(sorting, [0]);
    const attributeSortItem = get(sortItem, ['attributeSortItem']);
    if (attributeSortItem) {
        const identifier = get(attributeSortItem, ['attributeIdentifier']);
        if (identifier === get(category, 'localIdentifier')) {
            return get(attributeSortItem, 'direction', null);
        }
    }
    return null;
}


const createPureMetric = (measure, mdObj, measureIndex) => ({
    element: get(measure, ['definition', 'measureDefinition', 'item']),
    sort: getMeasureSorting(measure, mdObj),
    meta: { measureIndex }
});

const createDerivedMetric = (measure, mdObj, measureIndex) => {
    const { format } = measure;
    const sort = getMeasureSorting(measure, mdObj);
    const title = getBaseMetricTitle(measure.title);

    const hasher = partial(getGeneratedMetricHash, title, format);
    const aggregation = getAggregation(measure);
    const element = getGeneratedMetricIdentifier(measure, aggregation, getGeneratedMetricExpression, hasher);
    const definition = {
        metricDefinition: {
            identifier: element,
            expression: getGeneratedMetricExpression(measure),
            title,
            format
        }
    };

    return {
        element,
        definition,
        sort,
        meta: {
            measureIndex
        }
    };
};

const createContributionMetric = (measure, mdObj, measureIndex) => {
    const category = first(getCategories(mdObj)); // TODO is it always first??
    const getMetricExpression = partial(getPercentMetricExpression, category);
    const title = getBaseMetricTitle(get(measure, 'title'));
    const hasher = partial(getGeneratedMetricHash, title, CONTRIBUTION_METRIC_FORMAT);
    return {
        element: getGeneratedMetricIdentifier(measure, 'percent', getMetricExpression, hasher),
        definition: {
            metricDefinition: {
                identifier: getGeneratedMetricIdentifier(measure, 'percent', getMetricExpression, hasher),
                expression: getMetricExpression(measure),
                title,
                format: CONTRIBUTION_METRIC_FORMAT
            }
        },
        sort: getMetricSort(get(measure, 'sort')),
        meta: {
            measureIndex
        }
    };
};

const createPoPMetric = (measure, mdObj, measureIndex) => {
    // TODO fix this
    const title = getPoPMetricTitle(get(measure, 'title'));
    const format = get(measure, 'format');
    const hasher = partial(getGeneratedMetricHash, title, format);

    const date = getDate(mdObj);

    let generated;
    let getMetricExpression = partial(getPoPExpression, date, `[${get(measure, 'objectUri')}]`);

    if (isDerived(measure)) {
        generated = createDerivedMetric(measure, mdObj, measureIndex);
        getMetricExpression = partial(getPoPExpression, date, `(${get(generated, 'definition.metricDefinition.expression')})`);
    }

    const identifier = getGeneratedMetricIdentifier(measure, 'pop', getMetricExpression, hasher);

    const result = [{
        element: identifier,
        definition: {
            metricDefinition: {
                identifier,
                expression: getMetricExpression(),
                title,
                format
            }
        },
        sort: getMetricSort(get(measure, 'sort'), true),
        meta: {
            measureIndex,
            isPoP: true
        }
    }];

    if (generated) {
        result.push(generated);
    } else {
        result.push(createPureMetric(measure, mdObj, measureIndex));
    }

    return result;
};

const createContributionPoPMetric = (measure, mdObj, measureIndex) => {
    // TODO fix this
    const date = getDate(mdObj);

    const generated = createContributionMetric(measure, mdObj, measureIndex);
    const title = getPoPMetricTitle(get(measure, 'title'));

    const format = CONTRIBUTION_METRIC_FORMAT;
    const hasher = partial(getGeneratedMetricHash, title, format);

    const getMetricExpression = partial(getPoPExpression, date, `(${get(generated, 'definition.metricDefinition.expression')})`);

    const identifier = getGeneratedMetricIdentifier(measure, 'pop', getMetricExpression, hasher);

    const result = [{
        element: identifier,
        definition: {
            metricDefinition: {
                identifier,
                expression: getMetricExpression(),
                title,
                format
            }
        },
        sort: getMetricSort(get(measure, 'sort'), true),
        meta: {
            measureIndex,
            isPoP: true
        }
    }];

    result.push(generated);

    return result;
};

const categoryToElement = (attributesMap, mdObj, category) =>
    ({
        element: get(attributesMap, [get(category, 'displayForm'), 'attribute', 'meta', 'uri']),
        sort: getCategorySorting(category, mdObj)
    });

const attributeFilterToWhere = (f) => {
    const elements = get(f, 'listAttributeFilter.default.attributeElements', []);
    const elementsForQuery = map(elements, e => ({ id: last(e.split('=')) }));

    const dfUri = get(f, 'listAttributeFilter.displayForm');
    const negative = get(f, 'listAttributeFilter.default.negativeSelection');

    return negative ?
        { [dfUri]: { $not: { $in: elementsForQuery } } } :
        { [dfUri]: { $in: elementsForQuery } };
};

const getFromFilter = (f, property) => get(f, `dateFilter.${property}`);
const toInteger = value => parseInt(value, 10);

const dateFilterToWhere = (f) => {
    const dateUri =
        getFromFilter(f, 'dimension') ||
        getFromFilter(f, 'dataSet') ||
        getFromFilter(f, 'dataset'); // dataset with lowercase 's' is deprecated; kept here for backwards compatibility

    const granularity = getFromFilter(f, 'granularity');
    const isRelative = getFromFilter(f, 'type') === 'relative';

    const filterFrom = getFromFilter(f, 'from');
    const filterTo = getFromFilter(f, 'to');

    const from = isRelative ? toInteger(filterFrom) : filterFrom;
    const to = isRelative ? toInteger(filterTo) : filterTo;

    const between = [from, to];
    return { [dateUri]: { $between: between, $granularity: granularity } };
};

const isPoP = ({ definition }) => get(definition, 'popMeasureDefinition') !== undefined;
const isContribution = ({ definition }) => get(definition, ['measureDefinition', 'computeRatio']);
const isCalculatedMeasure = ({ definition }) => get(definition, ['measureDefinition', 'aggregation']) === undefined;

const rules = new Rules();

rules.addRule(
    [isPoP, isContribution],
    createContributionPoPMetric
);

rules.addRule(
    [isPoP],
    createPoPMetric
);

rules.addRule(
    [isContribution],
    createContributionMetric
);

rules.addRule(
    [isDerived],
    createDerivedMetric
);

rules.addRule(
    [isCalculatedMeasure],
    createPureMetric
);

function getMetricFactory(measure) {
    const factory = rules.match(measure);

    invariant(factory, `Unknown factory for: ${measure}`);

    return factory;
}

const isDateFilterExecutable = dateFilter =>
    get(dateFilter, 'from') !== undefined &&
    get(dateFilter, 'to') !== undefined;

const isAttributeFilterExecutable = listAttributeFilter =>
    notEmpty(get(listAttributeFilter, ['default', 'attributeElements']));


function getWhere(filters) {
    const executableFilters = filter(
        filters, ({ listAttributeFilter }) => isAttributeFilterExecutable(listAttributeFilter)
    );
    const attributeFilters = map(executableFilters, attributeFilterToWhere);
    const dateFilters = map(filter(filters, ({ dateFilter }) => isDateFilterExecutable(dateFilter)), dateFilterToWhere);

    const resultDate = [...dateFilters].reduce(assign, {});
    const resultAttribute = {
        $and: attributeFilters
    };

    return {
        ...resultDate,
        ...resultAttribute
    };
}

const sortToOrderBy = item => ({ column: get(item, 'element'), direction: get(item, 'sort') });

const getOrderBy = (metrics, categories) => {
    return map(filter([...categories, ...metrics], item => item.sort), sortToOrderBy);
};

export const mdToExecutionConfiguration = (mdObj, options = {}) => {
    const buckets = get(mdObj, 'buckets');
    const measures = map(buckets.measures, ({ measure }) => measure);
    const metrics = flatten(map(measures, (measure, index) => getMetricFactory(measure)(measure, buckets, index)));

    let categories = getCategories(buckets);
    let filters = getFilters(buckets);
    if (options.removeDateItems) {
        categories = filter(categories, ({ category }) => category.type !== 'date');
        filters = filter(filters, item => !item.dateFilter);
    }
    categories = map(categories, categoryToElement);

    const columns = compact(map([...categories, ...metrics], 'element'));

    return {
        columns,
        orderBy: getOrderBy(metrics, categories, get(mdObj, 'type')),
        definitions: sortDefinitions(compact(map(metrics, 'definition'))),
        where: columns.length ? getWhere(filters) : {},
        metricMappings: map(metrics, m => ({ element: m.element, ...m.meta }))
    };
};

function getAttributesMap(projectId, categories) {
    const categoryDisplayForms = categories.map(category => get(category, 'displayForm'));
    if (categoryDisplayForms.length === 0) {
        return Promise.resolve({});
    }
    return getObjects(projectId, categoryDisplayForms).then((displayForms) => {
        const attributeUris = displayForms.map(displayForm => get(displayForm, ['attributeDisplayForm', 'content', 'formOf']));
        return getObjects(projectId, attributeUris).then((attributes) => {
            return displayForms.reduce(
                (attributesMap, displayForm) =>
                    set(attributesMap,
                        [get(displayForm, ['attributeDisplayForm', 'meta', 'uri'])],
                        attributes.find(attribute => get(attribute, ['attribute', 'meta', 'uri']) === get(displayForm, ['attributeDisplayForm', 'content', 'formOf']))), // TODO will need also type
                {}
            );
        });
    });
}

export const newMdToExecutionConfiguration = (projectId, mdObj, options = {}) => {
    const buckets = get(mdObj, 'buckets', []);
    const measures = getMeasures(buckets);
    const metrics = flatten(map(measures, (measure, index) => getMetricFactory(measure)(measure, buckets, index)));

    let categories = getCategories(buckets);
    return getAttributesMap(projectId, categories).then((attributesMap) => {
        let filters = getFilters(mdObj);
        if (options.removeDateItems) {
            categories = filter(categories, category => !isDateCategory(category, attributesMap));
            filters = filter(filters, item => !item.dateFilter);
        }
        categories = map(categories, partial(categoryToElement, attributesMap, mdObj));

        const columns = compact(map([...categories, ...metrics], 'element'));

        return {
            columns,
            orderBy: getOrderBy(metrics, categories),
            definitions: sortDefinitions(compact(map(metrics, 'definition'))),
            where: columns.length ? getWhere(filters) : {},
            metricMappings: map(metrics, m => ({ element: m.element, ...m.meta }))
        };
    });
};

const getOriginalMetricFormats = (mdObj) => {
    // for metrics with showPoP or measureFilters.length > 0 roundtrip for original metric format
    return Promise.all(map(
        map(get(mdObj, 'buckets.measures'), ({ measure }) => measure),
        (measure) => {
            if (measure.showPoP === true || measure.measureFilters.length > 0) {
                return xhrGet(measure.objectUri).then((obj) => {
                    return {
                        ...measure,
                        format: get(obj, 'metric.content.format', measure.format)
                    };
                });
            }

            return Promise.resolve(measure);
        }
    ));
};

export const getDataForVis = (projectId, mdObj, settings) => {
    return getOriginalMetricFormats(mdObj).then((measures) => {
        const metadata = mdObj;
        metadata.buckets.measures = map(measures, measure => ({ measure }));
        const { columns, ...executionConfiguration } = mdToExecutionConfiguration(mdObj);
        return getData(projectId, columns, executionConfiguration, settings);
    });
};
