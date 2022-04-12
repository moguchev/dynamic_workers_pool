package dynamicworkerspool

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_NewDynamicWorkersPool(t *testing.T) {
	t.Run("zero workers num", func(t *testing.T) {
		wp, err := NewDynamicWorkersPool(0, 1)
		require.Nil(t, wp)
		require.Error(t, err)
	})
	t.Run("zero tasks pool", func(t *testing.T) {
		wp, err := NewDynamicWorkersPool(1, 0)
		require.Nil(t, wp)
		require.Error(t, err)
	})
	t.Run("succsess", func(t *testing.T) {
		const workersNum = 1
		wp, err := NewDynamicWorkersPool(workersNum, 1)
		require.NotNil(t, wp)
		require.NotNil(t, wp.closer)
		require.NotNil(t, wp.quit)
		require.Equal(t, uint64(workersNum), wp.current.Load())
		require.NoError(t, err)
	})
}

func Test_ResetWorkersNum(t *testing.T) {
	const num = 3

	t.Run("negative workers num", func(t *testing.T) {
		wp, err := NewDynamicWorkersPool(num, 3)
		require.NoError(t, err)
		curr := wp.current
		require.Error(t, wp.ResetWorkersNum(0))
		require.Equal(t, curr, wp.current)
		require.NoError(t, wp.Close())
		require.Equal(t, uint64(0), wp.current.Load())
	})
	t.Run("add workers", func(t *testing.T) {
		wp, err := NewDynamicWorkersPool(num, 3)
		require.NoError(t, err)
		require.NoError(t, wp.ResetWorkersNum(num+1))
		require.Equal(t, uint64(num+1), wp.current.Load())
		require.NoError(t, wp.Close())
		require.Equal(t, uint64(0), wp.current.Load())
	})
	t.Run("decrease workers", func(t *testing.T) {
		wp, err := NewDynamicWorkersPool(num, 3)
		require.NoError(t, err)
		require.NoError(t, wp.ResetWorkersNum(num-1))
		require.Equal(t, uint64(num-1), wp.current.Load())
		require.NoError(t, wp.Close())
		require.Equal(t, uint64(0), wp.current.Load())
	})
	t.Run("process task", func(t *testing.T) {
		const (
			expected = 5
			taskPool = 3
		)
		wp, err := NewDynamicWorkersPool(num, taskPool)
		require.NoError(t, err)

		var (
			tmp  = 0
			mx   = sync.Mutex{}
			task = func() {
				mx.Lock()
				tmp++
				mx.Unlock()
			}
		)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		for i := 0; i < expected; i++ {
			wp.PushTask(ctx, task)
		}

		require.NoError(t, wp.Close())
		require.Equal(t, uint64(0), wp.current.Load())
		require.Equal(t, expected, tmp)
	})
	t.Run("push task cancel", func(t *testing.T) {
		wp, err := NewDynamicWorkersPool(1, 1)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		cancel()

		require.NotNil(t, wp.PushTask(ctx, func() { fmt.Println("...") }))
	})
}
