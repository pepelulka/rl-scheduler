package scheduler

import (
	"context"

	"github.com/pepelulka/rl-scheduler/internal"
)

// WorkerState описывает состояние одного воркера в момент принятия решения.
type WorkerState struct {
	Host        string
	ActiveTasks int32
	Metrics     internal.NodeMetrics
}

// ClusterState — снимок кластера, передаваемый планировщику.
// Meta намеренно не типизирована: её структура произвольна и будет меняться.
type ClusterState struct {
	Workers   []WorkerState
	QueueSize int // число задач, ожидающих размещения в очереди
	Meta      map[string]string
}

// TaskInfo — информация о задаче, которую нужно разместить.
// Meta аналогично произвольна.
type TaskInfo struct {
	ID   string
	Type string
	Meta map[string]string
}

// Decision — ответ планировщика.
type Decision struct {
	// WorkerHost — адрес воркера, которому отправить задачу.
	// Заполнен только если Wait == false.
	WorkerHost string
	// Wait == true означает "подождать, подходящего воркера сейчас нет".
	Wait bool
}

// Scheduler принимает текущее состояние кластера и задачу,
// возвращает решение — на какой воркер отправить или ждать.
type Scheduler interface {
	Schedule(ctx context.Context, cluster ClusterState, task TaskInfo) (Decision, error)
}

// TaskResultHandler — опциональный интерфейс для планировщиков,
// поддерживающих онлайн-обучение по результатам выполнения задач.
// MasterService вызывает OnTaskResult после каждого ReportTaskResult.
type TaskResultHandler interface {
	OnTaskResult(taskID string, success bool, queueSize int)
}
