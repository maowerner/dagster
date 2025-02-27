import {AutoMaterializePolicyType} from '../../graphql/types';
import {buildAssetTabs} from '../AssetTabs';
import {AssetViewDefinitionNodeFragment} from '../types/AssetView.types';

describe('buildAssetTabs', () => {
  const definitionWithPartition: AssetViewDefinitionNodeFragment = {
    id: 'dagster_test.toys.repo.auto_materialize_repo_2.["eager_downstream_3_partitioned"]',
    groupName: 'default',
    partitionDefinition: {
      description: 'Daily, starting 2023-02-01 UTC.',
      __typename: 'PartitionDefinition',
    },
    partitionKeysByDimension: [
      {
        name: 'default',
        __typename: 'DimensionPartitionKeys',
      },
    ],
    repository: {
      id: 'cbff94a5bb24f8af0414f4041c450c02725a6ee6',
      name: 'auto_materialize_repo_2',
      location: {
        id: 'dagster_test.toys.repo',
        name: 'dagster_test.toys.repo',
        __typename: 'RepositoryLocation',
      },
      __typename: 'Repository',
    },
    jobs: [
      {
        id: 'c2c2f713745b2a2a671094c846b1786ecddab4ce',
        name: '__ASSET_JOB_0',
        schedules: [],
        sensors: [],
        __typename: 'Pipeline',
      },
    ],
    __typename: 'AssetNode',
    description: null,
    graphName: null,
    opNames: ['eager_downstream_3_partitioned'],
    opVersion: null,
    jobNames: ['__ASSET_JOB_0'],
    autoMaterializePolicy: {
      policyType: AutoMaterializePolicyType.EAGER,
      __typename: 'AutoMaterializePolicy',
    },
    freshnessPolicy: null,
    requiredResources: [],
    configField: {
      name: 'config',
      isRequired: false,
      configType: {
        givenName: 'Any',
        __typename: 'RegularConfigType',
        key: 'Any',
        description: null,
        isSelector: false,
        typeParamKeys: [],
        recursiveConfigTypes: [],
      },
      __typename: 'ConfigTypeField',
    },
    hasMaterializePermission: true,
    computeKind: null,
    isPartitioned: true,
    isObservable: false,
    isSource: false,
    assetKey: {
      path: ['eager_downstream_3_partitioned'],
      __typename: 'AssetKey',
    },
    metadataEntries: [],
    type: {
      __typename: 'RegularDagsterType',
      key: 'Any',
      name: 'Any',
      displayName: 'Any',
      description: null,
      isNullable: false,
      isList: false,
      isBuiltin: true,
      isNothing: false,
      metadataEntries: [],
      inputSchemaType: {
        key: 'Selector.f2fe6dfdc60a1947a8f8e7cd377a012b47065bc4',
        description: null,
        isSelector: true,
        typeParamKeys: [],
        fields: [
          {
            name: 'json',
            description: null,
            isRequired: true,
            configTypeKey: 'Shape.4b53b73df342381d0d05c5f36183dc99cb9676e2',
            defaultValueAsJson: null,
            __typename: 'ConfigTypeField',
          },
          {
            name: 'pickle',
            description: null,
            isRequired: true,
            configTypeKey: 'Shape.4b53b73df342381d0d05c5f36183dc99cb9676e2',
            defaultValueAsJson: null,
            __typename: 'ConfigTypeField',
          },
          {
            name: 'value',
            description: null,
            isRequired: true,
            configTypeKey: 'Any',
            defaultValueAsJson: null,
            __typename: 'ConfigTypeField',
          },
        ],
        __typename: 'CompositeConfigType',
        recursiveConfigTypes: [
          {
            key: 'Shape.4b53b73df342381d0d05c5f36183dc99cb9676e2',
            description: null,
            isSelector: false,
            typeParamKeys: [],
            fields: [
              {
                name: 'path',
                description: null,
                isRequired: true,
                configTypeKey: 'String',
                defaultValueAsJson: null,
                __typename: 'ConfigTypeField',
              },
            ],
            __typename: 'CompositeConfigType',
          },
          {
            givenName: 'String',
            __typename: 'RegularConfigType',
            key: 'String',
            description: '',
            isSelector: false,
            typeParamKeys: [],
          },
          {
            givenName: 'Any',
            __typename: 'RegularConfigType',
            key: 'Any',
            description: null,
            isSelector: false,
            typeParamKeys: [],
          },
        ],
      },
      outputSchemaType: null,
      innerTypes: [],
    },
  };

  // Copied from browser
  const definitionWithoutPartition: AssetViewDefinitionNodeFragment = {
    id: 'dagster_test.toys.repo.auto_materialize_repo_1.["lazy_downstream_1"]',
    groupName: 'default',
    partitionDefinition: null,
    partitionKeysByDimension: [],
    repository: {
      id: '4d9fd77c222a797eb8427fcbe1968799ebc24de8',
      name: 'auto_materialize_repo_1',
      location: {
        id: 'dagster_test.toys.repo',
        name: 'dagster_test.toys.repo',
        __typename: 'RepositoryLocation',
      },
      __typename: 'Repository',
    },
    jobs: [
      {
        id: '198e5820c136b6d5655b12b6415b5af1295bef53',
        name: '__ASSET_JOB_0',
        schedules: [],
        sensors: [],
        __typename: 'Pipeline',
      },
    ],
    __typename: 'AssetNode',
    description: null,
    graphName: null,
    opNames: ['lazy_downstream_1'],
    opVersion: null,
    jobNames: ['__ASSET_JOB_0'],
    autoMaterializePolicy: {
      policyType: AutoMaterializePolicyType.LAZY,
      __typename: 'AutoMaterializePolicy',
    },
    freshnessPolicy: null,
    requiredResources: [],
    configField: {
      name: 'config',
      isRequired: false,
      configType: {
        givenName: 'Any',
        __typename: 'RegularConfigType',
        key: 'Any',
        description: null,
        isSelector: false,
        typeParamKeys: [],
        recursiveConfigTypes: [],
      },
      __typename: 'ConfigTypeField',
    },
    hasMaterializePermission: true,
    computeKind: null,
    isPartitioned: false,
    isObservable: false,
    isSource: false,
    assetKey: {
      path: ['lazy_downstream_1'],
      __typename: 'AssetKey',
    },
    metadataEntries: [],
    type: {
      __typename: 'RegularDagsterType',
      key: 'Any',
      name: 'Any',
      displayName: 'Any',
      description: null,
      isNullable: false,
      isList: false,
      isBuiltin: true,
      isNothing: false,
      metadataEntries: [],
      inputSchemaType: {
        key: 'Selector.f2fe6dfdc60a1947a8f8e7cd377a012b47065bc4',
        description: null,
        isSelector: true,
        typeParamKeys: [],
        fields: [
          {
            name: 'json',
            description: null,
            isRequired: true,
            configTypeKey: 'Shape.4b53b73df342381d0d05c5f36183dc99cb9676e2',
            defaultValueAsJson: null,
            __typename: 'ConfigTypeField',
          },
          {
            name: 'pickle',
            description: null,
            isRequired: true,
            configTypeKey: 'Shape.4b53b73df342381d0d05c5f36183dc99cb9676e2',
            defaultValueAsJson: null,
            __typename: 'ConfigTypeField',
          },
          {
            name: 'value',
            description: null,
            isRequired: true,
            configTypeKey: 'Any',
            defaultValueAsJson: null,
            __typename: 'ConfigTypeField',
          },
        ],
        __typename: 'CompositeConfigType',
        recursiveConfigTypes: [
          {
            key: 'Shape.4b53b73df342381d0d05c5f36183dc99cb9676e2',
            description: null,
            isSelector: false,
            typeParamKeys: [],
            fields: [
              {
                name: 'path',
                description: null,
                isRequired: true,
                configTypeKey: 'String',
                defaultValueAsJson: null,
                __typename: 'ConfigTypeField',
              },
            ],
            __typename: 'CompositeConfigType',
          },
          {
            givenName: 'String',
            __typename: 'RegularConfigType',
            key: 'String',
            description: '',
            isSelector: false,
            typeParamKeys: [],
          },
          {
            givenName: 'Any',
            __typename: 'RegularConfigType',
            key: 'Any',
            description: null,
            isSelector: false,
            typeParamKeys: [],
          },
        ],
      },
      outputSchemaType: null,
      innerTypes: [],
    },
  };
  const params = {};

  it('shows all tabs', () => {
    const tabList = buildAssetTabs({definition: definitionWithPartition, params});
    const tabKeys = tabList.map(({id}) => id);
    expect(tabKeys).toEqual([
      'partitions',
      'events',
      'plots',
      'definition',
      'lineage',
      'auto-materialize-history',
    ]);
  });

  it('hides auto-materialize tab if no auto-materialize policy', () => {
    const tabList = buildAssetTabs({
      definition: {...definitionWithPartition, autoMaterializePolicy: null},
      params,
    });
    const tabKeys = tabList.map(({id}) => id);
    expect(tabKeys).toEqual(['partitions', 'events', 'plots', 'definition', 'lineage']);
  });

  it('hides partitions tab if no partitions', () => {
    const tabList = buildAssetTabs({
      definition: definitionWithoutPartition,
      params,
    });
    const tabKeys = tabList.map(({id}) => id);
    expect(tabKeys).toEqual([
      'events',
      'plots',
      'definition',
      'lineage',
      'auto-materialize-history',
    ]);
  });

  it('hides partitions and auto-materialize tabs if no partitions or auto-materializing', () => {
    const tabList = buildAssetTabs({
      definition: {...definitionWithoutPartition, autoMaterializePolicy: null},
      params,
    });
    const tabKeys = tabList.map(({id}) => id);
    expect(tabKeys).toEqual(['events', 'plots', 'definition', 'lineage']);
  });
});
