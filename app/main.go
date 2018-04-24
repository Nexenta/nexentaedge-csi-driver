package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/Nexenta/nexentaedge-csi-driver/csi"
	"github.com/spf13/cobra"
)

func init() {
	flag.Set("logtostderr", "true")
}

var (
	endpoint    string
	nodeID      string
)

func main() {
	flag.CommandLine.Parse([]string{})

	cmd := &cobra.Command{
		Use:   "nexentaedge-csi-plugin",
		Short: "CSI based NexentaEdge NFS driver",
		Run: func(cmd *cobra.Command, args []string) {
			handle()
		},
	}

	cmd.Flags().AddGoFlagSet(flag.CommandLine)

	cmd.PersistentFlags().StringVar(&nodeID, "nodeid", "", "node id")
	cmd.MarkPersistentFlagRequired("nodeid")

	cmd.PersistentFlags().StringVar(&endpoint, "endpoint", "", "CSI endpoint")
	cmd.MarkPersistentFlagRequired("endpoint")

	cmd.ParseFlags(os.Args[1:])
	if err := cmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%s", err.Error())
		os.Exit(1)
	}

	os.Exit(0)
}

func handle() {
	driver := csi.NewDriver(nodeID, endpoint)
	driver.Run()
}
