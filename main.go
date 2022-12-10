package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/robfig/cron/v3"
	"github.com/urfave/cli/v2"
	"log"
	"os"
	"os/signal"
	"time"
)

func getAppName(container types.Container) (appName string) {
	if value, ok := container.Labels[""]; ok {
		return value
	}
	if len(container.Names) > 0 {
		return container.Names[0]
	}
	return container.ID[:12]
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
	cli   *client.Client
	ctx   context.Context
	log   *log.Logger
	cron  *cron.Cron
	minio *minio.Client
}

func NewBackuper(minioEndpoint string, minioOptions *minio.Options) Backuper {
	dockerCli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		panic(fmt.Sprintln("Could not create Docker client:", err))
	}
	minioClient, err := minio.New(minioEndpoint, minioOptions)
	return Backuper{
		ctx:   context.Background(),
		cli:   dockerCli,
		log:   log.Default(),
		cron:  cron.New(),
		minio: minioClient,
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
	_, _ = b.cron.AddFunc("@every 1m", b.Scan)
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

func (b *Backuper) DumpData(container types.Container) (types.HijackedResponse, error) {
	execResp, err := b.cli.ContainerExecCreate(b.ctx, container.ID, types.ExecConfig{
		AttachStdout: true,
		AttachStderr: true,
		Cmd:          []string{"pg_dump", "-U", "postgres"},
	})
	if err != nil {
		return types.HijackedResponse{}, err
	}

	attachResp, err := b.cli.ContainerExecAttach(b.ctx, execResp.ID, types.ExecStartCheck{})
	if err != nil {
		return types.HijackedResponse{}, nil
	}

	return attachResp, nil
}

func (b *Backuper) UploadDump(appName string, reader *bufio.Reader) {
	_, _ = reader.ReadBytes(0x1d) // Group Separator control character, discard data in first group
	now := time.Now().Format(time.RFC3339)
	info, err := b.minio.PutObject(b.ctx, "backups", fmt.Sprintf("%s/%s.sql", appName, now), reader, -1, minio.PutObjectOptions{})
	if err != nil {
		b.log.Println("Failed to upload backup file for", appName, ":", err)
	}
	b.log.Println("Uploaded backup file", info.Location)
}

func (b *Backuper) BackupContainer(container types.Container) {
	b.log.Println("Starting backup for container", container.ID[:12])
	appName := getAppName(container)
	response, err := b.DumpData(container)
	if err != nil {
		log.Println("Failed to dump data for", container.ID[:12], ":", err)
		return
	}
	b.UploadDump(appName, response.Reader)
	b.log.Println("Finished backup for container", container.ID[:12])
}

func Do(ctx *cli.Context) error {
	done := make(chan bool)
	registerExitHandler(done)

	minioOptions := &minio.Options{
		Creds:  credentials.NewStaticV4(ctx.String("access-key"), ctx.String("secret-key"), ""),
		Secure: ctx.Bool("use-ssl"),
	}

	backuper := NewBackuper(ctx.String("endpoint"), minioOptions)
	backuper.Run(done)
	return nil
}

func main() {
	app := &cli.App{
		Name:   "postgres-backuper",
		Usage:  "backup postgres containers to MinIO",
		Action: Do,
		Flags: []cli.Flag{
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
		},
		HideHelpCommand: true,
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
