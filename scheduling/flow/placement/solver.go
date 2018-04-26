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

package placement

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"

	"github.com/coreos/ksched/scheduling/flow/dimacs"
	"github.com/coreos/ksched/scheduling/flow/flowgraph"
	"github.com/coreos/ksched/scheduling/flow/flowmanager"
)

var (
	FlowlesslyBinary    = "/usr/local/bin/flowlessly/flow_scheduler"
	FlowlesslyAlgorithm = "successive_shortest_path"
	Incremental         = true
)

type Solver interface {
	Solve() flowmanager.TaskMapping
}

type flowlesslySolver struct {
	isSolverStarted bool
	gm              flowmanager.GraphManager
	toSolver        io.Writer
	toConsole       io.Writer
	fromSolver      io.Reader
}

// Returns new solver initialized with the graph manager
func NewSolver(gm flowmanager.GraphManager) Solver {
	// TODO: Do the fields toSolver and fromSolver need to be initialized?
	return &flowlesslySolver{
		gm:              gm,
		isSolverStarted: false,
	}
}

// NOTE: assume we don't have debug flag
// NOTE: assume we only do incremental flow
// Note: assume Solve() is called iteratively and sequentially without concurrency.
func (fs *flowlesslySolver) Solve() flowmanager.TaskMapping {
	// Note: combine all the first time logic into this once function.
	// This is different from original cpp code.
	if !fs.isSolverStarted { // 如果这是第一次启动 solver
		fs.isSolverStarted = true

		// Uncomment once we run real sollver.
		fs.startSolver() // 各种配置 solver 的参数，方便输出

		// We must export graph and read from STDOUT/STDERR in parallel
		// Otherwise, the solver might block if STDOUT/STDERR buffer gets full.
		// (For example, if it outputs lots of warnings on STDERR.)

		// go fs.writeGraph()
		fs.writeGraph() // 输出所有的图的改变的地方

		// remove it.. once we run real sollver.
		//os.Exit(1)

		tm := fs.readTaskMapping()  // 输入所有图改变的地方，相当于调度结果
		// fmt.Printf("TaskMappings:%v\n", tm)
		// Exporter should have already finished writing because reading goroutine
		// have also finished.
		return tm
	}

	fs.gm.UpdateAllCostsToUnscheduledAggs() // 如果不是第一次启动，更新所有的任务与unsched节点之间的arc费用
	fs.writeIncremental()  // 增量输出所有图更改的地方
	tm := fs.readTaskMapping()
	return tm
}

func (fs *flowlesslySolver) startSolver() {
	binaryStr, args := fs.getBinConfig()

	var err error
	cmd := exec.Command(binaryStr, args...)
	fs.toSolver, err = cmd.StdinPipe()  // toSolver 为 stdin 的输出管道，输入管道已经被内置了
	if err != nil {
		panic(err)
	}
	fs.fromSolver, err = cmd.StdoutPipe() // fromSolver 为 stdout 的输入管道，输出管道已经被内置了
	if err != nil {
		panic(err)
	}
	fs.toConsole = os.Stdout  // /dev/stdout
	if err := cmd.Start(); err != nil {
		panic(err)
	}
}

func (fs *flowlesslySolver) writeGraph() {
	// TODO: make sure proper locking on graph, manager
	dimacs.Export(fs.gm.GraphChangeManager().Graph(), fs.toSolver) // 输出所有的图的内容 包括所有的node 和 arc
	//dimacs.Export(fs.gm.GraphChangeManager().Graph(), fs.toConsole)
	fs.gm.GraphChangeManager().ResetChanges()  // 这里为什么在访问 change 之前就重置了它？（可能这里是刚开启 solver，不存在增量change，增量change在writeIncremental中做）
}

func (fs *flowlesslySolver) writeIncremental() {
	// TODO: make sure proper locking on graph, manager
	dimacs.ExportIncremental(fs.gm.GraphChangeManager().GetOptimizedGraphChanges(), fs.toSolver)
	//dimacs.ExportIncremental(fs.gm.GraphChangeManager().GetOptimizedGraphChanges(), fs.toConsole)
	fs.gm.GraphChangeManager().ResetChanges()
}

func (fs *flowlesslySolver) readTaskMapping() flowmanager.TaskMapping {
	// TODO: make sure proper locking on graph, manager
	extractedFlow := fs.readFlowGraph()
	return fs.parseFlowToMapping(extractedFlow)
}


// 根据 scanner 来获得 node pairs 和 流量
// readFlowGraph returns a map of dst to a list of its corresponding src and flow capacity.
func (fs *flowlesslySolver) readFlowGraph() map[flowgraph.NodeID]flowPairMap {
	// The dstToSrcAndFlow map stores the flow pairs responsible for sending flow into the dst node
	// As a multimap it is keyed by the dst node where the flow is being sent.
	// The value is a map of flowpairs showing where all the flows to this dst are coming from
	dstToSrcAndFlow := make(map[flowgraph.NodeID]flowPairMap)  //终点到起点的 pair 以及相关的流量
	scanner := bufio.NewScanner(fs.fromSolver) // 从标准输入创建一个 scanner
	for scanner.Scan() {
		line := scanner.Text()
		//fmt.Printf("Line Read:%s\n", line)
		switch line[0] {
		case 'f':  // 这个 f 哪来的？？完全找不到
			var src, dst, flowCap uint64
			var discard string
			n, err := fmt.Sscanf(line, "%s %d %d %d", &discard, &src, &dst, &flowCap)
			if err != nil {
				panic(err)
			}
			if n != 4 {
				panic("expected reading 4 items")
			}

			// fmt.Printf("discard:%s src:%d dst:%d flowCap:%d\n", discard, src, dst, flowCap)

			if flowCap > 0 { // 如果这个还有容量没有用完
				pair := &flowPair{flowgraph.NodeID(src), flowCap}  // 就产生一个flowpair，连接该 src 与 dst，流量为最低流量？
				// If a flow map for this dst does not exist, then make one
				if dstToSrcAndFlow[flowgraph.NodeID(dst)] == nil {  // 如果到这个目标节点没有arc
					dstToSrcAndFlow[flowgraph.NodeID(dst)] = make(flowPairMap) // 初始化
				}
				dstToSrcAndFlow[flowgraph.NodeID(dst)][pair.srcNodeID] = pair // 否则添加一个 pair
			}
		case 'c':
			if line == "c EOI" {
				// fmt.Printf("Adj List:%v\n", dstToSrcAndFlow)
				return dstToSrcAndFlow
			} else if line == "c ALGORITHM TIME" {
				// Ignore. This is metrics of runtime.
			}
		case 's':
			// we don't care about cost
		default:
			panic("unknown: " + line)
		}
	}
	panic("wrong state")
}

// Maps worker|root tasks to leaves. It expects a extracted_flow containing
// only the arcs with positive flow (i.e. what ReadFlowGraph returns).
func (fs *flowlesslySolver) parseFlowToMapping(extractedFlow map[flowgraph.NodeID]flowPairMap) flowmanager.TaskMapping {
	// fmt.Printf("Extracted Flow:%v\n", extractedFlow)

	taskToPU := flowmanager.TaskMapping{} // 任务到机器的表？
	// Note: recording a node's PUs so that a node can assign the PUs to its source itself
	puIDs := make(map[flowgraph.NodeID][]flowgraph.NodeID)
	visited := make(map[flowgraph.NodeID]bool)
	toVisit := make([]flowgraph.NodeID, 0) // fifo queue
	leafIDs := fs.gm.LeafNodeIDs() // 所有的叶子节点的id列表，pu类型的资源节点
	sink := fs.gm.SinkNode() // sink 节点

	for leafID := range leafIDs { // 对于每一个pu类型的资源节点
		visited[leafID] = true
		// Get the flowPairMap for the sink
		flowPairMap, ok := extractedFlow[sink.ID] // 到达sink节点的所有的pair（todo，可以放在循环外边）
		if !ok {  // 如果没有到sink节点的流量
			continue
		}
		// Check if the current leaf contributes a flow pair
		flowPair, ok := flowPairMap[leafID] // 从该pu资源节点到 sink节点的pair
		if !ok { // 如果没有从 该 pu节点 到 sink 节点的流量
			continue
		}

		for i := uint64(0); i < flowPair.flow; i++ {
			puIDs[leafID] = append(puIDs[leafID], leafID) // 这是什么操作？[2][2,2,2,2,2...]
		}
		toVisit = append(toVisit, leafID)  // 添加所有到sink有流量的 pu 节点
	}

	// 下面的代码主要完成了 puIDs的更新
	// a variant of breath-frist search
	for len(toVisit) != 0 {
		nodeID := toVisit[0]
		toVisit = toVisit[1:] // 删掉首个资源节点
		visited[nodeID] = true  // 这有啥意义..?

		if fs.gm.GraphChangeManager().Graph().Node(nodeID).IsTaskNode() { // 如果是任务节点..不是pu节点吗..
			// fmt.Printf("Task Node found\n")
			// record the task mapping between task node and PU.
			if len(puIDs[nodeID]) != 1 { // 如果该节点的流量长度不为1，一个任务的流量只能为1
				log.Panicf("Task Node to Resource Node should be 1:1 mapping")
			}
			taskToPU[nodeID] = puIDs[nodeID][0]
			// 这还是 nodeID 啊...我懂了！！因为最开始的 nodeID就是机器节点的ID，然后在后来的传递中不停往前传，直到找到任务节点，然后就可以直到任务节点到机器节点的关系了
			// 所以我也大概知道了，调度的逻辑，就是谁先占谁就拿到流量呗。。超过 a 的出度了，就停止，混蛋啊，破调度器
			continue
		}

		toVisit = addPUToSourceNodes(extractedFlow, puIDs, nodeID, visited, toVisit)
	}

	return taskToPU
}

func addPUToSourceNodes(extractedFlow map[flowgraph.NodeID]flowPairMap, puIDs map[flowgraph.NodeID][]flowgraph.NodeID, nodeID flowgraph.NodeID, visited map[flowgraph.NodeID]bool, toVisit []flowgraph.NodeID) []flowgraph.NodeID {
	iter := 0
	srcFlowsMap, ok := extractedFlow[nodeID] // 到该 pu类型节点的所有流量
	if !ok {
		return toVisit
	}
	// search each source and assign all its downstream PUs to them.
	for _, srcFlowPair := range srcFlowsMap {
		// TODO: CHange this logic for map instead of slice
		// Populate the PUs vector at the source of the arc with as many PU
		// entries from the incoming set of PU IDs as there's flow on the arc.
		for ; srcFlowPair.flow > 0; srcFlowPair.flow-- { //只要这天路径还有流量
			if iter == len(puIDs[nodeID]) {  // 该 pu类型的资源节点到sink的流量
				break
			}
			// It's an incoming arc with flow on it.
			// Add the PU to the PUs vector of the source node.
			puIDs[srcFlowPair.srcNodeID] = append(puIDs[srcFlowPair.srcNodeID], puIDs[nodeID][iter]) // 记录该srcNodeID上上面个有多少流量，puIDs[nodeID][iter] 不一直等于 nodeID 么。。
			iter++
		}
		if !visited[srcFlowPair.srcNodeID] {
			toVisit = append(toVisit, srcFlowPair.srcNodeID) // 添加到 tovisited中，会在上一层递归调用
			visited[srcFlowPair.srcNodeID] = true
		}

		if iter == len(puIDs[nodeID]) { // 如果 nodeID 到 sink 的流量已经用完了
			// No more PUs left to assign
			break
		}
	}
	return toVisit
}

// TODO: We can definitely make it cleaner. But currently we just copy the code.
func (fs *flowlesslySolver) getBinConfig() (string, []string) {
	args := []string{
		"--graph_has_node_types=true",
		fmt.Sprintf("--algorithm=%s", FlowlesslyAlgorithm),
		"--print_assignments=false",
		"--debug_output=true",
		"--graph_has_node_types=true",
	}
	if !Incremental {
		args = append(args, "--daemon=false")
	}

	return FlowlesslyBinary, args
}
