package dynamicworkerspool

import (
	"context"
	"errors"
	"sync"

	"go.uber.org/atomic"
)

// DynamicWorkersPool пул воркеров с возможностью на ходу менять количество работающих воркеров
type DynamicWorkersPool struct {
	current *atomic.Uint64
	mx      sync.Mutex

	tasks chan func()
	wg    *sync.WaitGroup
	quit  chan struct{}

	closer func()
	once   sync.Once
}

// NewDynamicWorkersPool создает пул воркеров
func NewDynamicWorkersPool(workersNum, taskPoolSize uint) (*DynamicWorkersPool, error) {
	if workersNum == 0 {
		return nil, errors.New("workers ammount is requiered")
	}

	if taskPoolSize == 0 {
		return nil, errors.New("task pool size is requiered")
	}

	wp := &DynamicWorkersPool{
		current: atomic.NewUint64(0),
		tasks:   make(chan func(), taskPoolSize),
		wg:      new(sync.WaitGroup),
		quit:    make(chan struct{}),
	}

	wp.closer = func() {
		close(wp.tasks)
		wp.wg.Wait()
		close(wp.quit)
		wp.current.Store(0)
	}

	_ = wp.ResetWorkersNum(workersNum)
	return wp, nil
}

// Close - закрывает пул и ждет пока все воркеры доработают
// После закрытия пул не пригоден для работы, при попытке добавить задание упадет с паникой
func (dwp *DynamicWorkersPool) Close() error {
	if dwp.closer != nil {
		dwp.once.Do(dwp.closer)
	}

	return nil
}

// PushTask - добавляет задачу в очередь.
// PushTask блокирующая операция, и стоит использовать context.WithCancel/context.WithTimeout
func (dwp *DynamicWorkersPool) PushTask(ctx context.Context, task func()) error {
	select {
	case dwp.tasks <- task:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ResetWorkersNum переопределение числа воркеров
func (dwp *DynamicWorkersPool) ResetWorkersNum(workersNum uint) error {
	if workersNum == 0 {
		return errors.New("negative workers num")
	}

	dwp.mx.Lock()
	defer dwp.mx.Unlock()

	curr := uint(dwp.current.Load())
	if workersNum > curr {
		dwp.increaseWorkers(workersNum - curr)
	} else if workersNum < curr {
		dwp.decreaseWorkers(curr - workersNum)
	}

	return nil
}

func (dwp *DynamicWorkersPool) worker() {
	defer dwp.wg.Done()
	for {
		select {
		case <-dwp.quit:
			return
		case task, ok := <-dwp.tasks:
			if ok {
				task()
			} else {
				return
			}
		}
	}
}

func (dwp *DynamicWorkersPool) decreaseWorkers(delta uint) {
	curr := uint(dwp.current.Load())
	if delta > curr {
		delta = curr
	}
	dwp.current.Sub(uint64(delta))
	for i := uint(0); i < delta; i++ {
		dwp.quit <- struct{}{}
	}
}

func (dwp *DynamicWorkersPool) increaseWorkers(delta uint) {
	dwp.current.Add(uint64(delta))
	dwp.wg.Add(int(delta))

	for i := uint(0); i < delta; i++ {
		go dwp.worker()
	}
}
