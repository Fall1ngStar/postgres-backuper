package main

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
	"github.com/robfig/cron/v3"
	"github.com/urfave/cli/v2"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"
)

// https://github.com/cortexlabs/cortex/blob/dc5f73277d421c947129dc69a53597f196873f5e/pkg/lib/archive/tar.go#L79
func UntarReaderToMem(reader io.Reader) (map[string][]byte, error) {
	fileMap := map[string][]byte{}

	tarReader := tar.NewReader(reader)

	for {
		header, err := tarReader.Next()

		switch {
		case err == io.EOF:
			return fileMap, nil

		case err != nil:
			return nil, err

		case header == nil:
			continue
		}

		if header.Typeflag == tar.TypeReg {
			contents, err := io.ReadAll(tarReader)
			if err != nil {
				return nil, errors.Wrap(err, "unable to extract tar file")
			}

			path := strings.TrimPrefix(header.Name, "/")
			fileMap[path] = contents
		}
	}
}

func getAppName(container types.Container) (appName string) {
	if value, ok := container.Labels["postgres-backup/app-name"]; ok {
		return value
	}
	if len(container.Names) > 0 {
		return container.Names[0]
	}
	return container.ID[:12]
}

func (b *Backuper) getDatabaseName(container types.Container) string {
	if value, ok := container.Labels["postgres-backup/db-name"]; ok {
		return value
	}
	details, err := b.cli.ContainerInspect(b.ctx, container.ID)
	if err != nil {
		return "postgres"
	}
	for _, value := range details.Config.Env {
		splits := strings.SplitN(value, "=", 2)
		if len(splits) != 2 {
			return "postgres"
		}
		if splits[0] == "POSTGRES_DB" {
			return splits[1]
		}
	}
	return "postgres"
}

func (b *Backuper) getDatabaseUser(container types.Container) string {
	if value, ok := container.Labels["postgres-backup/db-user"]; ok {
		return value
	}
	details, err := b.cli.ContainerInspect(b.ctx, container.ID)
	if err != nil {
		return "postgres"
	}
	for _, value := range details.Config.Env {
		splits := strings.SplitN(value, "=", 2)
		if len(splits) != 2 {
			return "postgres"
		}
		if splits[0] == "POSTGRES_USER" {
			return splits[1]
		}
	}
	return "postgres"
}

func (b *Backuper) waitForExecToEnd(execID string) {
	c := make(chan bool, 1)
	go func() {
		for {
			inspect, err := b.cli.ContainerExecInspect(b.ctx, execID)
			if err != nil {
				c <- true
			}
			if !inspect.Running {
				c <- true
				return
			}
			time.Sleep(250 * time.Millisecond)
		}
	}()

	select {
	case <-time.After(30 * time.Second):
		b.log.Println("Timed out waiting for exec", execID)
		return
	case <-c:
		return
	}
}

func registerExitHandler(done chan bool) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)
	go func() {
		<-sigs
		done <- true
		<-sigs
		os.Exit(1)
	}()

}

type Backuper struct {
	cli     *client.Client
	ctx     context.Context
	log     *log.Logger
	cron    *cron.Cron
	minio   *minio.Client
	options *BackuperOptions
}

type BackuperOptions struct {
	minio    *MinioBackuperOptions
	schedule string
}

type MinioBackuperOptions struct {
	endpoint     string
	bucket       string
	minioOptions *minio.Options
}

func NewBackuper(options *BackuperOptions) Backuper {
	dockerCli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		panic(fmt.Sprintln("Could not create Docker client:", err))
	}
	minioClient, err := minio.New(options.minio.endpoint, options.minio.minioOptions)
	return Backuper{
		ctx:     context.Background(),
		cli:     dockerCli,
		log:     log.Default(),
		cron:    cron.New(),
		minio:   minioClient,
		options: options,
	}
}

func (b *Backuper) Run(done chan bool) {
	defer func(cli *client.Client) {
		err := cli.Close()
		if err != nil {
			fmt.Println("Failed to close Docker client:", err)
		}
	}(b.cli)

	b.cron.Start()
	_, _ = b.cron.AddFunc(b.options.schedule, b.Scan)
	b.log.Println("Started postgres backuper")
	<-done
	b.cron.Stop()
	b.log.Println("Stopped gracefully")
}

func (b *Backuper) Scan() {
	b.log.Println("Start containers scan")
	result, err := b.cli.ContainerList(b.ctx, types.ContainerListOptions{
		Filters: filters.NewArgs(filters.KeyValuePair{Key: "label", Value: "postgres-backup=true"}),
	})
	if err != nil {
		fmt.Println("Failed to list containers:", err)
	}
	for _, container := range result {
		b.BackupContainer(container)
	}
	b.log.Println("End containers scan")
}

func (b *Backuper) DumpData(container types.Container, dbName, user string) (io.ReadCloser, error) {
	execResp, err := b.cli.ContainerExecCreate(b.ctx, container.ID, types.ExecConfig{
		Cmd:    []string{"bash", "-c", fmt.Sprintf("pg_dump -U %s %s > /tmp/dump.sql", user, dbName)},
		Detach: false,
	})
	if err != nil {
		return nil, err
	}

	err = b.cli.ContainerExecStart(b.ctx, execResp.ID, types.ExecStartCheck{})
	if err != nil {
		return nil, err
	}
	b.waitForExecToEnd(execResp.ID)

	reader, _, err := b.cli.CopyFromContainer(b.ctx, container.ID, "/tmp/dump.sql")
	if err != nil {
		return nil, err
	}
	return reader, nil
}

func (b *Backuper) UploadDump(appName string, reader io.ReadCloser) {
	defer reader.Close()
	mem, err := UntarReaderToMem(reader)
	now := time.Now().Format(time.RFC3339)
	info, err := b.minio.PutObject(b.ctx, b.options.minio.bucket, fmt.Sprintf("%s/%s.sql", appName, now), bytes.NewReader(mem["dump.sql"]), -1, minio.PutObjectOptions{})
	if err != nil {
		b.log.Println("Failed to upload backup file for", appName, ":", err)
	}
	b.log.Println("Uploaded backup file", info.Location)
}

func (b *Backuper) BackupContainer(container types.Container) {
	b.log.Println("Starting backup for container", container.ID[:12])
	appName := getAppName(container)
	dbName := b.getDatabaseName(container)
	user := b.getDatabaseUser(container)
	response, err := b.DumpData(container, dbName, user)
	if err != nil {
		log.Println("Failed to dump data for", container.ID[:12], ":", err)
		return
	}
	b.UploadDump(appName, response)
	b.log.Println("Finished backup for container", container.ID[:12])
}

func Do(ctx *cli.Context) error {
	done := make(chan bool)
	registerExitHandler(done)

	minioOptions := &minio.Options{
		Creds:  credentials.NewStaticV4(ctx.String("access-key"), ctx.String("secret-key"), ""),
		Secure: ctx.Bool("use-ssl"),
	}
	options := &BackuperOptions{
		schedule: ctx.String("schedule"),
		minio: &MinioBackuperOptions{
			endpoint:     ctx.String("endpoint"),
			bucket:       ctx.String("bucket"),
			minioOptions: minioOptions,
		},
	}
	backuper := NewBackuper(options)
	if ctx.Bool("do") {
		backuper.Scan()
	} else {
		backuper.Run(done)
	}
	return nil
}

func main() {
	app := &cli.App{
		Name:   "postgres-backuper",
		Usage:  "backup postgres containers to MinIO",
		Action: Do,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "schedule",
				Usage:   "Schedule for backups",
				Value:   "@daily",
				EnvVars: []string{"PB_SCHEDULE"},
			},
			&cli.StringFlag{
				Name:     "endpoint",
				Usage:    "MinIO endpoint",
				Required: true,
				EnvVars:  []string{"PB_ENDPOINT"},
			},
			&cli.StringFlag{
				Name:     "access-key",
				Usage:    "MinIO access key",
				Required: true,
				EnvVars:  []string{"PB_ACCESS_KEY"},
			},
			&cli.StringFlag{
				Name:     "secret-key",
				Usage:    "MinIO secret key",
				Required: true,
				EnvVars:  []string{"PB_SECRET_KEY"},
			},
			&cli.StringFlag{
				Name:     "bucket",
				Usage:    "MinIO bucket",
				Required: true,
				EnvVars:  []string{"PB_BUCKET"},
			},
			&cli.BoolFlag{
				Name:    "use-ssl",
				Usage:   "Enable SSL for MinIO endpoint",
				Value:   true,
				EnvVars: []string{"PB_USE_SSL"},
			},
			&cli.BoolFlag{
				Name:  "do",
				Usage: "Execute the backuper now",
				Value: false,
			},
		},
		HideHelpCommand: true,
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
