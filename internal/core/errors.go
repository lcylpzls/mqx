package core

import "github.com/lcylpzls/errx"

// 错误码统一以 mqx_ 为前缀。
const (
	CodeInvalidConfig    errx.Code = "mqx_invalid_config"
	CodeTopicExists      errx.Code = "mqx_topic_exists"
	CodeTopicNotFound    errx.Code = "mqx_topic_not_found"
	CodeQueueFull        errx.Code = "mqx_queue_full"
	CodeShuttingDown     errx.Code = "mqx_shutting_down"
	CodeProcessTimeout   errx.Code = "mqx_process_timeout"
	CodeRetryExhausted   errx.Code = "mqx_retry_exhausted"
	CodeIDGenerateFailed errx.Code = "mqx_id_generate_failed"
)

// 预定义错误值，可用 errx.Is / errors.Is 判断。
var (
	ErrInvalidConfig    = errx.New(errx.KindInvalid, CodeInvalidConfig, "配置非法")
	ErrTopicExists      = errx.New(errx.KindAlreadyExists, CodeTopicExists, "主题已存在")
	ErrTopicNotFound    = errx.New(errx.KindNotFound, CodeTopicNotFound, "主题不存在")
	ErrQueueFull        = errx.New(errx.KindQuotaExceeded, CodeQueueFull, "队列已满")
	ErrShuttingDown     = errx.New(errx.KindUnavailable, CodeShuttingDown, "消息队列已关闭")
	ErrProcessTimeout   = errx.New(errx.KindTimeout, CodeProcessTimeout, "消息处理超时")
	ErrRetryExhausted   = errx.New(errx.KindQuotaExceeded, CodeRetryExhausted, "重试次数耗尽")
	ErrIDGenerateFailed = errx.New(errx.KindUnavailable, CodeIDGenerateFailed, "消息 ID 生成失败")
)
