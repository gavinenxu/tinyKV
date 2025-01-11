// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	stores := make([]*core.StoreInfo, 0)
	for _, store := range cluster.GetStores() {
		if store.IsUp() && store.DownTime() < cluster.GetMaxStoreDownTime() {
			stores = append(stores, store)
		}
	}
	if len(stores) == 1 {
		return nil
	}

	// store from large to small
	sort.Slice(stores, func(i, j int) bool {
		return stores[i].GetRegionSize() >= stores[j].GetRegionSize()
	})

	var region *core.RegionInfo
	var from *core.StoreInfo
	for _, store := range stores {
		var regions core.RegionsContainer
		cluster.GetPendingRegionsWithLock(store.GetID(), func(container core.RegionsContainer) {
			regions = container
		})
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			from = store
			break
		}

		cluster.GetFollowersWithLock(store.GetID(), func(container core.RegionsContainer) {
			regions = container
		})
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			from = store
			break
		}

		cluster.GetLeadersWithLock(store.GetID(), func(container core.RegionsContainer) {
			regions = container
		})
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			from = store
			break
		}
	}
	if region == nil || from == nil {
		return nil
	}

	storeIds := region.GetStoreIds()
	if len(storeIds) < cluster.GetMaxReplicas() {
		return nil
	}

	var to *core.StoreInfo
	for _, store := range stores {
		// find a 'to' store not in from store
		if _, ok := storeIds[store.GetID()]; !ok {
			to = store
		}
	}
	if to == nil {
		return nil
	}

	if from.GetRegionSize()-to.GetRegionSize() < region.GetApproximateSize() {
		return nil
	}

	// create new peer and operator
	peer, _ := cluster.AllocPeer(to.GetID())
	op, _ := operator.CreateMovePeerOperator(fmt.Sprintf("%d to %d", from.GetID(), to.GetID()), cluster, region, operator.OpBalance, from.GetID(), to.GetID(), peer.Id)

	return op
}
