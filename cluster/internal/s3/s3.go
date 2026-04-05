package s3

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Config struct {
	Endpoint string `yaml:"endpoint"`
	Login    string `yaml:"login"`
	Password string `yaml:"password"`
	Bucket   string `yaml:"bucket"`
}

type Client struct {
	s3     *s3.Client
	bucket string
}

func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("not-related"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.Login,
			cfg.Password,
			"",
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = true
	})

	return &Client{
		s3:     s3Client,
		bucket: cfg.Bucket,
	}, nil
}

// Upload загружает данные из reader в объект key в бакете.
func (c *Client) Upload(ctx context.Context, key string, body io.Reader) error {
	_, err := c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
		Body:   body,
	})
	if err != nil {
		return fmt.Errorf("upload %q: %w", key, err)
	}
	return nil
}

// Download скачивает объект key из бакета и возвращает ReadCloser.
// Вызывающая сторона обязана закрыть возвращённый io.ReadCloser.
func (c *Client) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("download %q: %w", key, err)
	}
	return out.Body, nil
}
