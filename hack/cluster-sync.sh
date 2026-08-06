#!/usr/bin/env bash
source hack/common.sh
set -euo pipefail

TENANT_CLUSTER_NAMESPACE=${TENANT_CLUSTER_NAMESPACE:-kvcluster}
CSI_DRIVER_NAMESPACE=${CSI_DRIVER_NAMESPACE:-kubevirt-csi-driver}
INFRA_STORAGE_CLASS=${INFRA_STORAGE_CLASS:-rook-ceph-block}
REGISTRY=${REGISTRY:-192.168.66.2:5000}
TARGET_NAME=${TARGET_NAME:-kubevirt-csi-driver}
TAG=${TAG:-latest}
export INFRACLUSTER_LABELS=${INFRACLUSTER_LABELS:-"tenant-cluster=${TENANT_CLUSTER_NAMESPACE}"}
ENABLE_RWX_FS=${ENABLE_RWX_FS:-false}
NFS_POOL_SIZE=${NFS_POOL_SIZE:-10}
NFS_POOL_SC=${NFS_POOL_SC:-rook-ceph-block}

function tenant::deploy_kubeconfig_secret() {
  TOKEN=$(_kubectl create token kubevirt-csi -n $TENANT_CLUSTER_NAMESPACE)
  INTERNAL_IP=$(_kubectl get node -l "node-role.kubernetes.io/control-plane" -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')

  kubeconfig=$(cat <<- END
apiVersion: v1
clusters:
- cluster:
    insecure-skip-tls-verify: true
    server: https://$INTERNAL_IP:6443
  name: infra-cluster
contexts:
- context:
    cluster: infra-cluster
    namespace: $TENANT_CLUSTER_NAMESPACE
    user: kubevirt-csi
  name: only-context
current-context: only-context
kind: Config
preferences: {}
users:
- name: kubevirt-csi
  user:
    token: $TOKEN
END
)

  cat <<- END | ./kubevirtci kubectl-tenant apply -f -
apiVersion: v1
kind: Secret
metadata:
  name: infra-cluster-credentials
  namespace: $CSI_DRIVER_NAMESPACE
data:
  kubeconfig: $(echo "$kubeconfig" | base64 -w 0)
END
}

function cluster::generate_tenant_controller_overlay() {
  cat <<- END > ./deploy/controller-tenant/dev-overlay/controller.yaml
kind: Deployment
apiVersion: apps/v1
metadata:
  name: kubevirt-csi-controller
  namespace: kubevirt-csi-driver
  labels:
    app: kubevirt-csi-driver
spec:
  template:
    spec:
      containers:
        - name: csi-driver
          image: $REGISTRY/$TARGET_NAME:$TAG
END
}

function cluster::generate_node_overlay() {
  cat <<- END > ./deploy/tenant/dev-overlay/node.yaml
kind: DaemonSet
apiVersion: apps/v1
metadata:
  name: kubevirt-csi-node
  namespace: kubevirt-csi-driver
spec:
  template:
    spec:
      containers:
        - name: csi-driver
          image: $REGISTRY/$TARGET_NAME:$TAG
END
}

function cluster::generate_nfs_egress_dev_overlay() {
  cat <<- END > ./deploy/nfs-egress/dev-overlay/kustomization.yaml
resources:
- ../../controller-tenant/dev-overlay
patches:
- path: controller-nfs-args.yaml
  target:
    group: apps
    version: v1
    kind: Deployment
    name: kubevirt-csi-controller
    namespace: $CSI_DRIVER_NAMESPACE
END

  cat <<- END > ./deploy/nfs-egress/dev-overlay/controller-nfs-args.yaml
- op: add
  path: /spec/template/spec/containers/0/args/-
  value: "--nfs-pool-size=${NFS_POOL_SIZE}"
- op: add
  path: /spec/template/spec/containers/0/args/-
  value: "--nfs-pool-sc=${NFS_POOL_SC}"
END
}

# ******************************************************
# Build the driver
# ******************************************************
./kubevirtci build

# ******************************************************
# Create namespace and put infra cluster secret in it
# ******************************************************
mkdir -p ./deploy/controller-tenant/dev-overlay
mkdir -p ./deploy/tenant/dev-overlay

cluster::generate_tenant_dev_kustomization
cluster::generate_controller_dev_kustomization "controller-tenant" $CSI_DRIVER_NAMESPACE
tenant::deploy_csidriver_namespace $CSI_DRIVER_NAMESPACE
_kubectl -n $TENANT_CLUSTER_NAMESPACE apply -f ./deploy/infra-cluster-service-account.yaml

# ******************************************************
# Generate kustomize overlay for development environment
# ******************************************************
tenant::deploy_kubeconfig_secret
cluster::generate_driver_configmap_overlay "tenant"
cluster::generate_tenant_controller_overlay
cluster::generate_node_overlay
cluster::generate_storageclass_overlay "tenant" $INFRA_STORAGE_CLASS
cluster::patch_local_storage_profile

# ******************************************************
# Deploy the snapshot resources
# ******************************************************
tenant::deploy_snapshotresources

# ******************************************************
# Deploy the tenant yaml
# ******************************************************
_kubectl_tenant apply --kustomize ./deploy/tenant/dev-overlay
# ******************************************************
# Deploy the controller yaml
# ******************************************************
_kubectl_tenant apply --kustomize ./deploy/controller-tenant/dev-overlay

if [[ "$ENABLE_RWX_FS" == "true" ]]; then 
  mkdir -p ./deploy/nfs-egress/dev-overlay
  cluster::generate_nfs_egress_dev_overlay
  _kubectl_tenant apply --kustomize ./deploy/nfs-egress/dev-overlay
fi

# ******************************************************
# Wait for driver to rollout
# ******************************************************
wait::check_rollout "_kubectl_tenant" "ds" "kubevirt-csi-node" "$CSI_DRIVER_NAMESPACE" "5m"
wait::check_rollout "_kubectl_tenant" "deployment" "kubevirt-csi-controller" "$CSI_DRIVER_NAMESPACE" "5m"

if [[ "$ENABLE_RWX_FS" == "true" ]]; then 
  _kubectl_tenant apply -f ./deploy/nfs-egress/base/rbac.yaml
  _kubectl_tenant apply -f ./deploy/nfs-egress/base/service.yaml
  _kubectl_tenant apply -f ./deploy/nfs-egress/base/ganesha-config.yaml
  _kubectl_tenant apply -f ./deploy/nfs-egress/base/daemonset.yaml

  wait::check_rollout "_kubectl_tenant" "ds" "kubevirt-nfs-egress" "$CSI_DRIVER_NAMESPACE" "5m"
fi
