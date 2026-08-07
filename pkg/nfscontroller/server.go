package nfscontroller

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"text/template"

	"k8s.io/klog/v2"
)

type NFSController struct {
	UnimplementedNFSEgressControllerServer
	mutex sync.Mutex
}

type Registry struct {
	Entries []Entry `json:"entries"`
	NextId  int     `json:"nextid"`
}

type Entry struct {
	Id       string `json:"id"`
	Path     string `json:"path"`
	ExportId int    `json:"exportid"`
}

var exportTemplate = template.Must(template.New("export").Parse(`
  EXPORT {
      Export_Id = {{.ExportId}};
      Path = {{.Path}};
      Pseudo = /export/{{.Id}};
      Access_Type = RW;
      Squash = None;
      Sectype = sys;
      FSAL { Name = VFS; }
  }
  `))

func (n *NFSController) CreateExport(_ context.Context, req *CreateExportRequest) (*CreateExportResponse, error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	resp := &CreateExportResponse{
		Success: false,
	}

	if len(req.PvcUid) == 0 {
		return resp, fmt.Errorf("request does not include a pvc UID")
	}

	poolPath := os.Getenv("POOL_MOUNT_PATH")
	exportPath := filepath.Join(poolPath, "export", req.PvcUid)
	resp.ExportPath = filepath.Join("/export", req.PvcUid)

	klog.V(2).Infof("creating new export: %s", exportPath)

	// TODO, no clue if these permissions are correct
	if err := os.MkdirAll(exportPath, 0770); err != nil {
		return resp, err
	}

	registryPath := filepath.Join(poolPath, ".ganesha-state", "registry.json")

	// write to registry
	registry := &Registry{}
	_, err := os.Stat(registryPath)
	if os.IsNotExist(err) {
		// TODO lower log level
		klog.V(2).Infof("creating initial registry json at path %s", registryPath)
		registry, err = createRegistry(req.PvcUid, exportPath, registryPath)
		if err != nil {
			return resp, err
		}
	} else {
		klog.V(2).Infof("updating registry json for pvc %s", req.PvcUid)
		oldId := registry.NextId
		registry, err = addRegistryEntry(req.PvcUid, exportPath, registryPath)
		if err != nil {
			return resp, err
		}

		// nothing was changed, don't need to rewrite export conf
		if oldId == registry.NextId {
			resp.Success = true
			return resp, nil
		}
	}

	klog.V(2).Infof("writing ganesha export conf")
	// rewrite the ganesha exports
	if err := writeExports(poolPath, registry); err != nil {
		return resp, err
	}

	// send SIGHUP to reload ganesha
	if err := reloadGanesha(); err != nil {
		return resp, err
	}

	resp.Success = true
	return resp, nil
}

// TODO
func (n *NFSController) UpdateQuota(_ context.Context, req *UpdateQuotaRequest) (*UpdateQuotaResponse, error) {
	resp := &UpdateQuotaResponse{
		Success: true,
	}

	return resp, nil
}

func (n *NFSController) DeleteExport(_ context.Context, req *DeleteExportRequest) (*DeleteExportResponse, error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	resp := &DeleteExportResponse{
		Success: false,
	}

	poolPath := os.Getenv("POOL_MOUNT_PATH")
	registryPath := filepath.Join(poolPath, ".ganesha-state", "registry.json")

	if len(req.PvcUid) == 0 {
		return resp, fmt.Errorf("request does not include a pvc UID")
	}

	registry, err := removeRegistryEntry(req.PvcUid, registryPath)
	if err != nil {
		return resp, err
	}

	// TODO, probably can skip this is we didnt remove anything
	klog.V(2).Infof("writing ganesha export conf")
	// rewrite the ganesha exports
	if err := writeExports(poolPath, registry); err != nil {
		return resp, err
	}

	resp.Success = true

	return resp, nil
}

func createRegistry(pvcUid, exportPath, registryPath string) (*Registry, error) {
	registry := &Registry{
		Entries: []Entry{
			{
				Id:       pvcUid,
				Path:     exportPath,
				ExportId: 100, // default to the first entry being id 100
			},
		},
		NextId: 101,
	}

	if err := writeJSON(registryPath, registry); err != nil {
		return nil, err
	}

	return registry, nil
}

func addRegistryEntry(pvcUid, exportPath, registryPath string) (*Registry, error) {
	registry, err := getRegistry(registryPath)
	if err != nil {
		return nil, err
	}

	// TODO not sure if this is a good check
	if registry.NextId == 0 {
		return nil, fmt.Errorf("registry is not correctly populated")
	}

	for _, entry := range registry.Entries {
		if entry.Id == pvcUid {
			klog.V(3).Infof("export for pvc %s already exists, skipping creation", pvcUid)
			return registry, nil
		}
	}

	newEntry := Entry{
		Id:       pvcUid,
		Path:     exportPath,
		ExportId: registry.NextId,
	}

	registry.Entries = append(registry.Entries, newEntry)
	registry.NextId += 1

	if err := writeJSON(registryPath, registry); err != nil {
		return nil, err
	}

	return registry, nil
}

func removeRegistryEntry(pvcUid, registryPath string) (*Registry, error) {
	registry, err := getRegistry(registryPath)
	if err != nil {
		klog.Errorf("removeRegistryEntry failed: %v", err)
		return nil, err
	}

	if len(registry.Entries) == 0 {
		klog.V(3).Infof("registry is empty, pvc %s already removed", pvcUid)
		return registry, nil
	}

	found := false
	for i, entry := range registry.Entries {
		if entry.Id == pvcUid {
			newEntries := slices.Delete(registry.Entries, i, i+1)
			registry.Entries = newEntries
			found = true
		}
	}

	if !found {
		klog.V(3).Infof("pvc entry %s does not exist, nothing to remove", pvcUid)
		return registry, nil
	}

	if err := writeJSON(registryPath, registry); err != nil {
		klog.Errorf("writeExports failed: %v", err)
		return nil, err
	}

	return registry, nil
}

func getRegistry(registryPath string) (*Registry, error) {
	reader, err := os.Open(registryPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open registry file at path %s", registryPath)
	}

	registry := &Registry{}
	if err = json.NewDecoder(reader).Decode(registry); err != nil {
		return nil, fmt.Errorf("failed to decode registroy json at path %s", registryPath)
	}
	reader.Close()

	return registry, nil
}

func writeJSON(registryPath string, registry *Registry) error {
	file, err := os.Create(registryPath)
	if err != nil {
		return fmt.Errorf("failed to open registry file at path %s", registryPath)
	}
	defer file.Close()

	if err := json.NewEncoder(file).Encode(registry); err != nil {
		return fmt.Errorf("failed to encode registry entry %w", err)
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to flush buffer to disk  %w", err)
	}
	return nil
}

// TODO, there may be a benefit of writing these files to a tmp file and renaming at the end
// to avoid potential corruption since it is rewriting the entire file
func writeExports(poolPath string, registry *Registry) error {
	confPath := filepath.Join(poolPath, ".ganesha-state", "exports.conf")
	file, err := os.Create(confPath)
	if err != nil {
		return fmt.Errorf("failed to open ganesha export config at path %s", confPath)
	}
	defer file.Close()

	for _, entry := range registry.Entries {
		if err := exportTemplate.Execute(file, entry); err != nil {
			return fmt.Errorf("writing export stanza for %s: %w", entry.Id, err)
		}
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("syncing exports file: %w", err)
	}

	return nil
}

func reloadGanesha() error {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return fmt.Errorf("reading /proc: %w", err)
	}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		comm, err := os.ReadFile(fmt.Sprintf("/proc/%d/comm", pid))
		if err != nil {
			continue
		}
		if strings.TrimSpace(string(comm)) == "ganesha.nfsd" {
			return syscall.Kill(pid, syscall.SIGHUP)
		}
	}
	return fmt.Errorf("ganesha.nfsd process not found in /proc")
}
