import {Tab, Tabs} from '@dagster-io/ui-components';
import qs from 'qs';
import * as React from 'react';

import {TabLink} from '../ui/TabLink';

import {AssetViewParams} from './types';
import {AssetViewDefinitionNodeFragment} from './types/AssetView.types';

interface Props {
  selectedTab: string;
  tabs: AssetTabConfig[];
}

export const AssetTabs = (props: Props) => {
  const {selectedTab, tabs} = props;

  return (
    <Tabs size="large" selectedTabId={selectedTab}>
      {tabs
        .filter((tab) => !tab.hidden)
        .map(({id, title, to, disabled}) => {
          if (disabled) {
            return <Tab disabled key={id} id={id} title={title} />;
          }
          return <TabLink key={id} id={id} title={title} to={to} disabled={disabled} />;
        })}
    </Tabs>
  );
};

export const DEFAULT_ASSET_TAB_ORDER = [
  'partitions',
  'events',
  'plots',
  'definition',
  'lineage',
  'auto-materialize-history',
];

export type AssetTabConfigInput = {
  definition: AssetViewDefinitionNodeFragment | null;
  params: AssetViewParams;
};

export type AssetTabConfig = {
  id: string;
  title: string;
  to: string;
  disabled?: boolean;
  hidden?: boolean;
};

export const buildAssetViewParams = (params: AssetViewParams) => `?${qs.stringify(params)}`;

export const buildAssetTabMap = (input: AssetTabConfigInput): Record<string, AssetTabConfig> => {
  const {definition, params} = input;
  return {
    partitions: {
      id: 'partitions',
      title: 'Partitions',
      to: buildAssetViewParams({...params, view: 'partitions'}),
      hidden: !definition?.partitionDefinition,
    },
    events: {
      id: 'events',
      title: 'Events',
      to: buildAssetViewParams({...params, view: 'events', partition: undefined}),
    },
    plots: {
      id: 'plots',
      title: 'Plots',
      to: buildAssetViewParams({...params, view: 'plots'}),
    },
    definition: {
      id: 'definition',
      title: 'Definition',
      to: buildAssetViewParams({...params, view: 'definition'}),
      disabled: !definition,
    },
    lineage: {
      id: 'lineage',
      title: 'Lineage',
      to: buildAssetViewParams({...params, view: 'lineage'}),
      disabled: !definition,
    },
    'auto-materialize-history': {
      id: 'auto-materialize-history',
      title: 'Auto-materialize history',
      to: buildAssetViewParams({...params, view: 'auto-materialize-history'}),
      disabled: !definition,
      hidden: !definition?.autoMaterializePolicy,
    },
  };
};

export const buildAssetTabs = (input: AssetTabConfigInput): AssetTabConfig[] => {
  const tabConfigs = buildAssetTabMap(input);
  return DEFAULT_ASSET_TAB_ORDER.map((tabId) => tabConfigs[tabId]).filter(
    (tab): tab is AssetTabConfig => !!tab && !tab.hidden,
  );
};
