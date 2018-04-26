package k8sclient

import (
	"fmt"
	"path"
	"time"

	"github.com/coreos/ksched/k8s/k8stype"
	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/client/cache"
	"k8s.io/kubernetes/pkg/client/restclient"
	kc "k8s.io/kubernetes/pkg/client/unversioned"
	"k8s.io/kubernetes/pkg/controller/framework"
	"k8s.io/kubernetes/pkg/fields"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/watch"
)
/**
只有一个文件中引用了 k8s.io/kubernetes 的包，所以说这个包就是与 k8s 的接口调度器
 */
type Config struct {
	Addr string
}

type Client struct {
	apisrvClient     *kc.Client
	unscheduledPodCh chan *k8stype.Pod
	nodeCh           chan *k8stype.Node
}

// New 这个方法完成 k8s 自有的 Client 对象的实例化，同时添加了事件处理函数 和 informer 的启动
func New(cfg Config, podChanSize int) (*Client, error) {
	// 初始化了 restclient 对象 【k8s 接口数据结构】
	restCfg := &restclient.Config{
		Host:  fmt.Sprintf("http://%s", cfg.Addr),
		QPS:   1000,
		Burst: 1000,
	}
	c, err := kc.New(restCfg) // 创建 k8s 自有 client 对象 【k8s 接口数据结构】
	if err != nil {
		return nil, err
	}

	pch := make(chan *k8stype.Pod, podChanSize) // 待调度的 pod 队列通道

	// Create informer to watch on unscheduled Pods (non-failed non-succeeded pods with an empty node binding)
	sel := fields.ParseSelectorOrDie("spec.nodeName==" + "" + ",status.phase!=" + string(api.PodSucceeded) + ",status.phase!=" + string(api.PodFailed))

	informer := framework.NewSharedInformer( // 【k8s 接口数据结构】 SharedInformer 与 Informer 的区别就在于 shared可以多controller共用？
		&cache.ListWatch{
			ListFunc: func(options api.ListOptions) (runtime.Object, error) {
				options.FieldSelector = sel
				return c.Pods(api.NamespaceAll).List(options)
			},
			WatchFunc: func(options api.ListOptions) (watch.Interface, error) {
				options.FieldSelector = sel
				return c.Pods(api.NamespaceAll).Watch(options)
			},
		},
		&api.Pod{}, // 【k8s 接口数据结构】
		0,
	)
	// Add event handlers for the addition, update and deletion of the pods watched by the above informer
	informer.AddEventHandler(framework.ResourceEventHandlerFuncs{ // 【k8s 接口方法——添加 informer 的事件处理函数】
		AddFunc: func(obj interface{}) { //获得pod并将 k8s 的自有数据结构转化成自己系统的数据结构
			pod := obj.(*api.Pod)
			ourPod := &k8stype.Pod{
				ID: makePodID(pod.Namespace, pod.Name),
			}
			// For now every new pod is just added to the channel being polled by the batch mechanism
			pch <- ourPod
		},
		UpdateFunc: func(oldObj, newObj interface{}) {}, // 更新不管，意味着没有二次调度
		DeleteFunc: func(obj interface{}) { // 删除不管
		},
	})
	stopCh := make(chan struct{})
	go informer.Run(stopCh)  // 启动了 informer 【k8s 接口方法】

	nch := make(chan *k8stype.Node, 100) // 当前的 node 的节点队列通道
	// Informer for watching the addition and removal of nodes in the cluster
	_, nodeInformer := framework.NewInformer( // 【k8s 接口数据结构】
		cache.NewListWatchFromClient(c, "nodes", api.NamespaceAll, fields.ParseSelectorOrDie("")),
		&api.Node{}, // 【k8s 接口数据结构】
		0,
		framework.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				node := obj.(*api.Node)
				// Skip the master node
				if node.Spec.Unschedulable {  // 这种状态的 node 一般是 master
					return
				}
				ourNode := &k8stype.Node{
					ID: node.Name,
				}
				// Add every new node seen to the channel being polled by the
				// resource topology initializer mechanism
				nch <- ourNode
			},
			UpdateFunc: func(oldObj, newObj interface{}) {}, // 更新也不管
			DeleteFunc: func(obj interface{}) {}, // 删除竟然也不管
		},
	)
	stopCh2 := make(chan struct{})
	go nodeInformer.Run(stopCh2)

	return &Client{
		apisrvClient:     c,
		unscheduledPodCh: pch,
		nodeCh:           nch,
	}, nil
}

type PodChan <-chan *k8stype.Pod

func (c *Client) GetUnscheduledPodChan() PodChan {
	return c.unscheduledPodCh
}

type NodeChan <-chan *k8stype.Node

func (c *Client) GetNodeChan() NodeChan {
	return c.nodeCh
}

// 这一个方法是将调度结果发回给 k8s
// Write out node bindings
func (c *Client) AssignBinding(bindings []*k8stype.Binding) error {
	for _, ob := range bindings {
		ns, name := parsePodID(ob.PodID)
		b := &api.Binding{  // 构造 k8s 能够看懂的 Binding 对象  【k8s 接口数据结构】
			ObjectMeta: api.ObjectMeta{Namespace: ns, Name: name}, // pod 名字和命名空间
			Target: api.ObjectReference{
				Kind: "Node",  // kind 为 Node
				Name: parseNodeID(ob.NodeID),  // 调度的目的地为 Node 的 ID
			},
		}
		ctx := api.WithNamespace(api.NewContext(), ns) //【k8s 接口数据结构】
		// 【k8s 接口方法——进行回传】
		err := c.apisrvClient.Post().Namespace(api.NamespaceValue(ctx)).Resource("bindings").Body(b).Do().Error() // 通过 Post 方法，http 协议进行传输
		// 告诉 apiserver 该 pod 应该调度到什么地方
		if err != nil {
			panic(err)
		}
	}
	return nil
}

// 这个方法是获得待调度的pods的方法
// Returns a batch of pods or blocks until there is at least on pod creation call back
// The timeout specifies how long to wait for another pod on the pod channel before returning
// the batch of pods that need to be scheduled
func (c *Client) GetPodBatch(timeout time.Duration) []*k8stype.Pod {
	batchedPods := make([]*k8stype.Pod, 0)

	fmt.Printf("Waiting for a pod scheduling request\n")

	// Check for first pod, block until at least 1 is available
	// 先检查是否有需要调度的pod，如果没有就一直等着，如果有了，那么就在后续进行计时，统一对这个时间段的所有的pods进行调度
	pod := <-c.unscheduledPodCh
	batchedPods = append(batchedPods, pod)

	// Set timer for timeout between successive pods
	timer := time.NewTimer(timeout)
	done := make(chan bool)
	go func() {
		<-timer.C
		done <- true
	}()  // 起一个协程在此刻进行计时

	fmt.Printf("Batching pod scheduling requests\n")
	numPods := 1
	//fmt.Printf("Number of pods requests: %d", numPods)
	// Poll until done from timeout
	// TODO: Put a cap on the batch size since this could go on forever
	finish := false
	for !finish {
		select {
		case pod = <-c.unscheduledPodCh:
			numPods++
			fmt.Printf("\rNumber of pods requests: %d", numPods)
			batchedPods = append(batchedPods, pod)
			// Refresh the timeout for next pod
			timer.Reset(timeout) //如果有 pod 过来了就重置计时器，只要保证一定的pod到来频率，就能一直循环？
		case <-done:
			finish = true
			fmt.Printf("\n")
		default:
			// Do nothing and keep polling until timeout
		}
	}
	return batchedPods
}

func makePodID(namespace, name string) string {
	return path.Join(namespace, name)
}

func parsePodID(id string) (string, string) {
	ns, podName := path.Split(id)
	// Get rid of the / at the end
	ns = ns[:len(ns)-1]
	return ns, podName
}

func parseNodeID(id string) string {
	return id
}
