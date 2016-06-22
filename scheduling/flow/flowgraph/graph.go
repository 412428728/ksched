// Copyright 2016 The ksched Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package flowgraph

import (
	"log"
	"math/rand"
	"time"

	"github.com/coreos/ksched/pkg/util/queue"
)

type Graph struct {
	// Next node id to use
	nextID uint64
	// Unordered set of arcs in graph
	arcSet map[*Arc]struct{}
	// Map of nodes keyed by nodeID
	nodeMap map[uint64]*Node
	// Queue storing the ids of the nodes we've previously removed.
	unusedIDs queue.FIFO

	// Behaviour flag - set as struct field rather than global static variable
	//                  since we will have only one instance of the FlowGraph.
	// If true the the flow graph will not generate node ids in order
	RandomizeNodeIDs bool
}

// Constructor equivalent in Go
// Must specify RandomizeNodeIDs flag
func New(randomizeNodeIDs bool) *Graph {
	fg := &Graph{}
	fg.nextID = 1
	fg.unusedIDs = queue.NewFIFO()
	if randomizeNodeIDs {
		fg.RandomizeNodeIDs = true
		fg.PopulateUnusedIds(50)
	}
	return fg
}

// Adds an arc based on references to the src and dst nodes
func (fg *Graph) AddArc(src, dst *Node) *Arc {
	srcID, dstID := src.id, dst.id

	srcNode := fg.nodeMap[srcID]
	if srcNode == nil {
		log.Fatalf("graph: AddArc error, src node with id:%d not found\n", srcID)
	}
	dstNode := fg.nodeMap[dstID]
	if dstNode == nil {
		log.Fatalf("graph: AddArc error, dst node with id:%d not found\n", dstID)
	}
	arc := NewArc(srcNode, dstNode)
	fg.arcSet[arc] = struct{}{}
	srcNode.AddArc(arc)
	return arc
}

func (fg *Graph) AddNode() *Node {
	id := fg.NextId()
	node := &Node{
		id: id,
	}
	// Insert into nodeMap, must not already be present
	_, ok := fg.nodeMap[id]
	if ok {
		log.Fatalf("graph: AddNode error, node with id:%d already present in nodeMap\n", id)
	}
	fg.nodeMap[id] = node
	return node
}

func (fg *Graph) DeleteArc(arc *Arc) {
	delete(arc.srcNode.outgoingArcMap, arc.dstNode.id)
	delete(arc.dstNode.incomingArcMap, arc.srcNode.id)
	delete(fg.arcSet, arc)
}

func (fg *Graph) Node(id uint64) *Node {
	return fg.nodeMap[id]
}

func (fg *Graph) DeleteNode(node *Node) {
	// Reuse this ID for later
	fg.unusedIDs.Push(node.id)
	// First remove all outgoing arcs
	for dstID, arc := range node.outgoingArcMap {
		if dstID != arc.dst {
			log.Fatalf("graph: DeleteNode error, dstID:%d != arc.dst:%d\n", dstID, arc.dst)
		}
		if node.id != arc.src {
			log.Fatalf("graph: DeleteNode error, node.id:%d != arc.src:%d\n", node.id, arc.src)
		}
		delete(arc.dstNode.incomingArcMap, arc.src)
		fg.DeleteArc(arc)
	}
	// Remove all incoming arcs
	for srcID, arc := range node.incomingArcMap {
		if srcID != arc.dst {
			log.Fatalf("graph: DeleteNode error, srcID:%d != arc.src:%d\n", srcID, arc.src)
		}
		if node.id != arc.dst {
			log.Fatalf("graph: DeleteNode error, node.id:%d != arc.dst:%d\n", node.id, arc.dst)
		}
		delete(arc.srcNode.outgoingArcMap, arc.dst)
		fg.DeleteArc(arc)
	}
	// Remove node from nodeMap
	delete(fg.nodeMap, node.id)
}

// Returns nil if arc not found
func (fg *Graph) GetArc(src, dst *Node) *Arc {
	return src.outgoingArcMap[dst.id]
}

// Returns the nextID to assign to a node
func (fg *Graph) NextId() uint64 {
	if fg.RandomizeNodeIDs {
		if fg.unusedIDs.IsEmpty() {
			fg.PopulateUnusedIds(fg.nextID * 2)
		}
		return fg.unusedIDs.Pop().(uint64)
	}
	if fg.unusedIDs.IsEmpty() {
		newID := fg.nextID
		fg.nextID++
		return newID
	}
	return fg.unusedIDs.Pop().(uint64)
}

// Called if fg.RandomizeNodeIDs is true to generate a random shuffle of ids
func (fg *Graph) PopulateUnusedIds(newNextID uint64) {
	t := time.Now().UnixNano()
	r := rand.New(rand.NewSource(t))
	ids := make([]uint64, 0)
	for i := fg.nextID; i < newNextID; i++ {
		ids = append(ids, i)
	}
	// Fisher-Yates shuffle
	for i := range ids {
		j := r.Intn(i + 1)
		ids[i], ids[j] = ids[j], ids[i]
	}
	for i := range ids {
		fg.unusedIDs.Push(ids[i])
	}
	fg.nextID = newNextID
}