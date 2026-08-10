package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	klog "k8s.io/klog/v2"

	kubecli "kubevirt.io/csi-driver/pkg/generated/kubevirt/client-go/clientset/versioned"
	clientpkg "kubevirt.io/csi-driver/pkg/kubevirt"
	"kubevirt.io/csi-driver/pkg/nfsfencing"
	"kubevirt.io/csi-driver/pkg/util"
)

func main() {
	var (
		infraKubeconfig    string
		tenantKubeconfig   string
		infraNamespace     string
		infraLabels        string
		poolDVName         string
	)

	klog.InitFlags(nil)
	flag.StringVar(&infraKubeconfig, "infra-cluster-kubeconfig", "", "Path to infra cluster kubeconfig")
	flag.StringVar(&tenantKubeconfig, "tenant-cluster-kubeconfig", "", "Path to tenant cluster kubeconfig (defaults to in-cluster config)")
	flag.StringVar(&infraNamespace, "infra-cluster-namespace", "", "Infra cluster namespace where VMIs live")
	flag.StringVar(&infraLabels, "infra-cluster-labels", "", "Infra cluster labels used when creating resources (key=value,...)")
	flag.StringVar(&poolDVName, "pool-dv-name", "pvc-nfs-pool", "Name of the pool DataVolume to move during failover")
	flag.Parse()

	if infraNamespace == "" {
		klog.Fatal("--infra-cluster-namespace is required")
	}

	// Infra cluster config.
	infraConfig, err := clientcmd.BuildConfigFromFlags("", infraKubeconfig)
	if err != nil {
		klog.Fatalf("failed to build infra cluster config: %v", err)
	}

	// Parse infra labels for the virtClient.
	infraLabelMap := map[string]string{}
	if infraLabels != "" {
		for _, pair := range splitLabels(infraLabels) {
			if len(pair) == 2 {
				infraLabelMap[pair[0]] = pair[1]
			}
		}
	}

	// Build the existing virtClient (used for hotplug ops).
	tenantRestConfig, err := clientcmd.BuildConfigFromFlags("", tenantKubeconfig)
	if err != nil {
		klog.Fatalf("failed to build tenant cluster config: %v", err)
	}
	tenantK8s, err := kubernetes.NewForConfig(tenantRestConfig)
	if err != nil {
		klog.Fatalf("failed to build tenant kubernetes client: %v", err)
	}
	virtClient, err := clientpkg.NewClient(
		infraConfig,
		infraLabelMap,
		tenantK8s,
		nil, // no snapshot client needed
		util.StorageClassEnforcement{AllowAll: true},
		"pvc",
	)
	if err != nil {
		klog.Fatalf("failed to build virtClient: %v", err)
	}

	// Build the raw kubevirt client for VMI Watch.
	kubeVirtClient, err := kubecli.NewForConfig(infraConfig)
	if err != nil {
		klog.Fatalf("failed to build kubevirt client: %v", err)
	}

	controller := nfsfencing.NewFencingController(
		virtClient,
		kubeVirtClient,
		tenantK8s,
		infraNamespace,
		poolDVName,
	)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	klog.Infof("Starting NFS fencing controller (pool-dv=%s, infra-namespace=%s)", poolDVName, infraNamespace)
	if err := controller.Run(ctx); err != nil && err != context.Canceled {
		klog.Fatalf("fencing controller exited with error: %v", err)
	}
}

func splitLabels(s string) [][2]string {
	var result [][2]string
	for _, pair := range splitComma(s) {
		kv := splitN(pair, "=", 2)
		if len(kv) == 2 {
			result = append(result, [2]string{kv[0], kv[1]})
		}
	}
	return result
}

func splitComma(s string) []string {
	var parts []string
	start := 0
	for i, c := range s {
		if c == ',' {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}

func splitN(s, sep string, n int) []string {
	var parts []string
	for i := 0; i < n-1; i++ {
		idx := -1
		for j := range s {
			if s[j:j+len(sep)] == sep {
				idx = j
				break
			}
		}
		if idx < 0 {
			break
		}
		parts = append(parts, s[:idx])
		s = s[idx+len(sep):]
	}
	parts = append(parts, s)
	return parts
}
