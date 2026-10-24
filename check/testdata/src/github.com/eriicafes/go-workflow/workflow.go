// Package workflow is a stub of the workflow API for analyzer tests.
package workflow

import (
	"context"
	"time"
)

type Runner struct{}

type Struct[Out any] struct{}

func (Struct[Out]) Workflow(Out) {}

type Result[T any] struct{}

func Use[In, Out any](func(context.Context, In) (Out, error)) Runner { return Runner{} }

func Step[In, Out any](ctx context.Context, fn func(context.Context, In) (Out, error), input In) (Out, error) {
	var out Out
	return out, nil
}

func Background[In, Out any](ctx context.Context, fn func(context.Context, In) (Out, error), input In) *Result[Out] {
	return &Result[Out]{}
}

func Then[T, U any](ctx context.Context, source *Result[T], fn func(T, error) *Result[U]) *Result[U] {
	return &Result[U]{}
}

func Sleep(ctx context.Context, d time.Duration) error { return nil }
