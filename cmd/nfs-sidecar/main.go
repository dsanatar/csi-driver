package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
	"kubevirt.io/csi-driver/pkg/nfscontroller"
)

func main() {
	var (
	// listenPort string
	)

	server := grpc.NewServer([]grpc.ServerOption{}...)

	nfsServer := &nfscontroller.NFSController{}
	nfscontroller.RegisterNFSEgressControllerServer(server, nfsServer)

	flag.Set("logtostderr", "true")
	fmt.Println("Starting NFS gRPC server")

	listener, err := net.Listen("tcp", ":9090")
	if err != nil {
		log.Fatalf("failed to start listener on port 9090")
	}

	server.Serve(listener)
}
