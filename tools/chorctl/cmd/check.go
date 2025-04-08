/*
 * Copyright © 2023 Clyso GmbH
 * Copyright © 2025 STRATO GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"context"
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/chorctl/internal/api"
	"github.com/clyso/chorus/tools/chorctl/internal/format"

	"github.com/spf13/cobra"
)

var (
	checkBucket     string
	checkToBucket   string
	checkNameFormat string
)

// checkCmd represents the check command
var checkCmd = &cobra.Command{
	Use:   "check <from_storage> <to_storage> --bucket=<bucket_name>",
	Short: "Checks the files in the source and destination match.",
	Long:  ``,
	Args:  cobra.MatchAll(cobra.ExactArgs(2), cobra.OnlyValidArgs),
	ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tlsOptions, err := getTLSOptions()
		if err != nil {
			logrus.WithError(err).Fatal("unable to get tls options")
		}
		conn, err := api.Connect(ctx, address, tlsOptions)
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to connect to api")
		}
		defer conn.Close()
		client := pb.NewChorusClient(conn)

		storages, err := client.GetStorages(ctx, &emptypb.Empty{})
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to get storages to api")
		}
		var res []string
		for _, storage := range storages.Storages {
			res = append(res, storage.Name)
		}
		return res, cobra.ShellCompDirectiveDefault
	},
	Run: func(cmd *cobra.Command, args []string) {
		var from, to = args[0], args[1]
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tlsOptions, err := getTLSOptions()
		if err != nil {
			logrus.WithError(err).Fatal("unable to get tls options")
		}
		conn, err := api.Connect(ctx, address, tlsOptions)
		if err != nil {
			logrus.WithError(err).WithField("address", address).Fatal("unable to connect to api")
		}
		defer conn.Close()
		client := pb.NewChorusClient(conn)

		fmt.Println("Getting list of buckets...")

		m, err := client.ListReplications(ctx, &emptypb.Empty{})
		if err != nil {
			logrus.WithError(err).Fatal("unable to get migration")
		}
		todo := make([]*pb.Replication, 0, len(m.Replications))
		names := make([]string, 0, len(m.Replications))
		nameBuilder, err := format.NewReplNameBuilder(checkNameFormat)
		if err != nil {
			logrus.WithError(err).Fatal("unable to parse name format")
		}
		max := 6
		for _, repl := range m.Replications {
			if repl.From != from || repl.To != to {
				continue
			}
			if len(checkBucket) > 0 && repl.Bucket != checkBucket {
				continue
			}
			if len(checkToBucket) > 0 && repl.ToBucket != checkToBucket {
				continue
			}
			todo = append(todo, repl)
			name := nameBuilder(repl)
			if len(name) > max {
				max = len(name)
			}
			names = append(names, name)
		}
		if len(todo) == 0 {
			logrus.Fatal("unable to get buckets list: no migration")
		}
		fmt.Println("Start check for", len(todo), "buckets...")

		fmt.Printf("🪣 %s | Match\t | MissSrc\t | MissDst\t | Differ \t | Error\t|\n", fillName("BUCKET", max, " "))
		var wg sync.WaitGroup
		wg.Add(len(todo))
		for i := range todo {
			go func(r *pb.Replication) {
				defer wg.Done()
				check(ctx, client, r.From, r.To, r.Bucket, r.ToBucket, names[i], max)
			}(todo[i])
		}
		wg.Wait()
	},
}

func check(ctx context.Context, client pb.ChorusClient, from, to, bucket string, toBucket string, name string, max int) {
	res, err := client.CompareBucket(ctx, &pb.CompareBucketRequest{
		Bucket:    bucket,
		From:      from,
		To:        to,
		ShowMatch: true,
		User:      user,
		ToBucket:  toBucket,
	})
	if err != nil {
		logrus.WithError(err).Fatal("unable to check bucket")
	}
	if res.IsMatch {
		fmt.Printf("✅ %s | %-7d\t | %-7d\t | %-7d\t | %-7d\t | %-7d\n", fillName(name, max, "."),
			len(res.Match),
			len(res.MissFrom),
			len(res.MissTo),
			len(res.Differ),
			len(res.Error),
		)
	} else {
		fmt.Println("\033[31m" + fmt.Sprintf("❌ %s | %-7d\t | %-7d\t | %-7d\t | %-7d\t | %-7d", fillName(name, max, "."),
			len(res.Match),
			len(res.MissFrom),
			len(res.MissTo),
			len(res.Differ),
			len(res.Error),
		) + "\033[0m")
	}
}

func fillName(in string, size int, fill string) string {
	//if len(in) == size {
	//	return in
	//}
	for i := len(in); i < size; i++ {
		in += fill
	}
	return in
}

func init() {
	rootCmd.AddCommand(checkCmd)
	checkCmd.Flags().StringVarP(&checkBucket, "check-bucket", "b", "", "check bucket name")
	checkCmd.Flags().StringVarP(&checkToBucket, "to-bucket", "t", "", "check bucket name")
	checkCmd.Flags().StringVarP(&checkNameFormat, "name-format", "n", "%F->%T", format.ReplNameFormatHelp)
	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// checkCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// checkCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
