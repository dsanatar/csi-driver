package nfsfencing

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	kubevirtv1 "kubevirt.io/api/core/v1"
	kubecli "kubevirt.io/csi-driver/pkg/generated/kubevirt/client-go/clientset/versioned"
	client "kubevirt.io/csi-driver/pkg/kubevirt"
)

const (
	nfsServerLabel = "csi.kubevirt.io/nfs-server"
	nfsFencedTaint = "csi.kubevirt.io/nfs-fenced"
	poolDVSerial   = "nfs-pool"
)

// FencingController watches the tenant cluster Node labelled as the NFS server.
// When it goes NotReady, it confirms against the infra KubeVirt API that the
// backing VM is truly down before executing the failover sequence. This bypasses
// the 5-minute Kubernetes pod-eviction timeout while still preventing false
// positives from transient connectivity blips (where the VM is still Running).
type FencingController struct {
	virtClient     client.Client
	kubeVirtClient kubecli.Interface
	tenantClient   kubernetes.Interface
	infraNamespace string
	poolDVName     string
}

func NewFencingController(
	virtClient client.Client,
	kubeVirtClient kubecli.Interface,
	tenantClient kubernetes.Interface,
	infraNamespace string,
	poolDVName string,
) *FencingController {
	return &FencingController{
		virtClient:     virtClient,
		kubeVirtClient: kubeVirtClient,
		tenantClient:   tenantClient,
		infraNamespace: infraNamespace,
		poolDVName:     poolDVName,
	}
}

// Run starts the fencing controller. Reconnects automatically on watch errors.
func (f *FencingController) Run(ctx context.Context) error {
	klog.Infof("NFS fencing controller started, watching tenant nodes for NFS server failure")
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := f.watchAndFence(ctx); err != nil {
			klog.Errorf("watch error, restarting: %v", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}

func (f *FencingController) watchAndFence(ctx context.Context) error {
	watcher, err := f.kubeVirtClient.KubevirtV1().VirtualMachines(f.infraNamespace).Watch(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to open vm watch: %w", err)
	}
	defer watcher.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return fmt.Errorf("node watch channel closed")
			}
			if event.Type != watch.Modified && event.Type != watch.Added {
				continue
			}
			vm, ok := event.Object.(*kubevirtv1.VirtualMachine)
			if !ok {
				continue
			}
			if vm.Status.Ready || !vmHasNFSPool(vm, f.poolDVName) {
				continue
			}
			klog.Infof("Infra VM %s contains NFS Pool and found to be not ready", vm.Name)

			// Prevent duplicate fencing while one attempt is in progress.
			// taintAndUnlabelNode is intentionally the last step so that if
			// fencing fails midway, the node stays labeled and watch events
			// keep firing — enabling automatic retry.
			if err := f.fence(ctx, vm.Name); err != nil {
				klog.Errorf("fencing failed for node %s: %v", vm.Name, err)
				continue
			}
			// Fencing succeeded — return to force a fresh watch. Stale
			// events buffered during the fence carry the old node object
			// (still NotReady + labeled) and would re-trigger fencing.
			// The re-opened watch reflects current label state.
			return nil
		}
	}
}

// // findInfraVMI locates the infra cluster VMI that backs the given tenant node name.
// func (f *FencingController) findInfraVMI(ctx context.Context, tenantNodeName string) (*kubevirtv1.VirtualMachineInstance, error) {
// 	vmis, err := f.virtClient.ListVirtualMachines(ctx, f.infraNamespace)
// 	if err != nil {
// 		return nil, fmt.Errorf("list VMIs: %w", err)
// 	}
// 	for i := range vmis {
// 		if vmis[i].Labels["kubevirt.io/vm"] == tenantNodeName {
// 			return &vmis[i], nil
// 		}
// 	}
// 	return nil, nil
// }

func (f *FencingController) fence(ctx context.Context, deadTenantNode string) error {
	klog.Infof("Fencing: tenant-node=%s pool-dv=%s", deadTenantNode, f.poolDVName)

	// Step 1: remove pool DV from dead VM spec immediately.
	// runStrategy=Always restarts the VMI — clearing the spec prevents the
	// restarting VM from racing to re-attach the pool DV.
	klog.Infof("Fencing: removing pool DV from VM spec of %s", deadTenantNode)
	if err := f.virtClient.RemoveVolumeFromVM(ctx, f.infraNamespace, deadTenantNode,
		&kubevirtv1.RemoveVolumeOptions{Name: f.poolDVName}); err != nil {
		klog.Warningf("RemoveVolumeFromVM: %v (may already be removed)", err)
	}

	// Step 2: find a healthy backup node — any Running VMI whose tenant node is not fenced.
	backupVMIName, err := f.findHealthyNode(ctx, deadTenantNode)
	if err != nil {
		return fmt.Errorf("finding healthy backup node: %w", err)
	}
	if backupVMIName == "" {
		return fmt.Errorf("no healthy backup node found")
	}
	klog.Infof("Fencing: selected backup VMI %s", backupVMIName)

	// Step 3: force-delete the NFS pod stuck on the dead node before
	// labeling the backup. If we label first, the DaemonSet immediately
	// creates a pod whose init container fails (no device yet) or whose
	// Ganesha starts and gets killed by a later force-delete — either way
	// causing unnecessary churn and potentially corrupting recovery state
	// mid-grace period.
	if err := f.forceDeleteNFSPod(ctx); err != nil {
		klog.Warningf("force-delete NFS pod: %v (continuing)", err)
	}

	// Step 4: hotplug pool DV to the backup VM so the device is present
	// when the DaemonSet pod starts after we label the node in step 5.
	klog.Infof("Fencing: hotplugging pool DV to backup VM %s", backupVMIName)
	addOpts := &kubevirtv1.AddVolumeOptions{
		Name: f.poolDVName,
		Disk: &kubevirtv1.Disk{
			Serial: poolDVSerial,
			DiskDevice: kubevirtv1.DiskDevice{
				Disk: &kubevirtv1.DiskTarget{Bus: "scsi"},
			},
		},
		VolumeSource: &kubevirtv1.HotplugVolumeSource{
			DataVolume: &kubevirtv1.DataVolumeSource{Name: f.poolDVName},
		},
	}
	if err := f.virtClient.AddVolumeToVM(ctx, f.infraNamespace, backupVMIName, addOpts); err != nil {
		return fmt.Errorf("AddVolumeToVM on backup: %w", err)
	}
	if err := f.virtClient.EnsureVolumeAvailable(ctx, f.infraNamespace, backupVMIName,
		f.poolDVName, 3*time.Minute); err != nil {
		return fmt.Errorf("EnsureVolumeAvailable on backup: %w", err)
	}

	// Step 5: label the backup node. The pool DV is already hotplugged, so
	// the DaemonSet creates the pod and the init container finds the device
	// immediately — one clean start, no restarts.
	if err := f.virtClient.LabelTenantNodeNFS(ctx, backupVMIName); err != nil {
		return fmt.Errorf("label backup tenant node: %w", err)
	}

	// Step 6: taint and un-label the dead tenant node — intentionally last.
	// Keeping the label until here means watch events keep firing if any
	// earlier step fails, enabling automatic retry of the full sequence.
	if err := f.taintAndUnlabelNode(ctx, deadTenantNode); err != nil {
		klog.Warningf("taint/unlabel dead node %s: %v", deadTenantNode, err)
	}

	klog.Infof("Fencing complete: pool DV moved to %s, DaemonSet will reschedule NFS pod", backupVMIName)
	return nil
}

func isNodeReady(node *corev1.Node) bool {
	for _, c := range node.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func vmHasNFSPool(vm *kubevirtv1.VirtualMachine, dvName string) bool {
	for _, vol := range vm.Spec.Template.Spec.Volumes {
		if vol.Name == dvName {
			return true
		}
	}
	return false
}

func (f *FencingController) findHealthyNode(ctx context.Context, deadTenantNode string) (string, error) {
	vmis, err := f.virtClient.ListVirtualMachines(ctx, f.infraNamespace)
	if err != nil {
		return "", fmt.Errorf("list VMIs: %w", err)
	}
	for _, vmi := range vmis {
		if vmi.Status.Phase != kubevirtv1.Running {
			continue
		}
		// Never move the NFS server to a control-plane node — if the control
		// plane goes down the cluster is unrecoverable regardless of NFS, and
		// the fencing controller itself runs there so it cannot fence its own node.
		if vmi.Labels["cluster.x-k8s.io/role"] != "worker" {
			continue
		}
		tenantName := vmi.Labels["kubevirt.io/vm"]
		if tenantName == deadTenantNode || tenantName == "" {
			continue
		}
		node, err := f.tenantClient.CoreV1().Nodes().Get(ctx, tenantName, metav1.GetOptions{})
		if errors.IsNotFound(err) {
			continue
		}
		if err != nil {
			klog.Warningf("get tenant node %s: %v", tenantName, err)
			continue
		}
		for _, t := range node.Spec.Taints {
			if t.Key == nfsFencedTaint {
				goto next
			}
		}
		return vmi.Name, nil
	next:
	}
	return "", nil
}

func (f *FencingController) forceDeleteNFSPod(ctx context.Context) error {
	pods, err := f.tenantClient.CoreV1().Pods("kubevirt-csi-driver").List(ctx, metav1.ListOptions{
		LabelSelector: "app=kubevirt-nfs-egress",
	})
	if err != nil {
		return fmt.Errorf("list NFS pods: %w", err)
	}
	grace := int64(0)
	for _, pod := range pods.Items {
		klog.Infof("Fencing: force-deleting NFS pod %s", pod.Name)
		if err := f.tenantClient.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name,
			metav1.DeleteOptions{GracePeriodSeconds: &grace}); err != nil {
			return fmt.Errorf("delete pod %s: %w", pod.Name, err)
		}
	}
	return nil
}

func (f *FencingController) taintAndUnlabelNode(ctx context.Context, nodeName string) error {
	node, err := f.tenantClient.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get node %s: %w", nodeName, err)
	}
	delete(node.Labels, nfsServerLabel)
	node.Spec.Taints = append(node.Spec.Taints, corev1.Taint{
		Key:    nfsFencedTaint,
		Effect: corev1.TaintEffectNoSchedule,
		Value:  "true",
	})
	if _, err := f.tenantClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update node %s: %w", nodeName, err)
	}
	klog.Infof("Fencing: node %s tainted and NFS label removed", nodeName)
	return nil
}
