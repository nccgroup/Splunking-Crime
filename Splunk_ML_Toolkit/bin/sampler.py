#!/usr/bin/env python

import pandas as pd
import numpy as np


class ReservoirSampler(object):
    def __init__(self, limit, random_state=None):
        self.limit = limit
        self._dfs = []
        self._dfs_count = 0
        self._df = pd.DataFrame()
        self.count = 0

        if random_state:
            np.random.seed(random_state)
        self.random_state = np.random.get_state()

        self._max_dfs_len = self.limit / 5

    def _concat_df(self, df):
        self._dfs.append(df)
        self._dfs_count += len(df)

        self._max_dfs_len = max(self._max_dfs_len, len(df))

    def _combine_dfs(self):
        if len(self._dfs) > 0:
            self._df = self._df.append(self._dfs)
            self._df.drop_duplicates(subset='_slot', keep='last', inplace=True)
            self._dfs = []
            self._dfs_count = 0

    def get_df(self):
        if self.count == 0 and len(self._dfs) == 0:
            return self._df

        self._combine_dfs()
        self._df.reset_index(drop=True, inplace=True)

        return self._df.sort_values('_gindex').drop(['_gindex', '_slot'], axis=1)

    def append(self, new_df, copy=False):
        if len(new_df) == 0:
            return

        if copy:
            new_df = new_df.copy()

        # Assign counter to new_df
        new_df['_gindex'] = np.arange(len(new_df)) + self.count
        self.count += len(new_df)

        if self.limit <= 0 or self.count <= self.limit:
            new_df['_slot'] = new_df['_gindex']
            self._concat_df(new_df)
            return

        # Move the head of new_df to self._dfs
        if self.count - len(new_df) < self.limit:
            head_count = self.limit - (self.count - len(new_df))
            new_df['_slot'] = new_df['_gindex']
            self._concat_df(new_df[0:head_count])
            new_df = new_df[head_count:].copy()

        np.random.set_state(self.random_state)
        rnd = np.random.rand(len(new_df))
        self.random_state = np.random.get_state()

        new_df['_slot'] = np.floor(rnd * new_df['_gindex']).astype(int)
        keepers = new_df[new_df['_slot'] < self.limit]
        new_df = None

        self._concat_df(keepers)

        if self._dfs_count > self._max_dfs_len:
            self._combine_dfs()

        return
