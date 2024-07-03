package dsl

import (
	"context"
	"time"
)

func (s *Store[T]) Process(ctx context.Context, data <-chan T) error {
	schema, err := s.Schema()
	if err != nil {
		return err
	}
	defer schema.Release()

	tick := time.NewTicker(time.Minute)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case e := <-data:
			err := schema.Write(e)
			if err != nil {
				return err
			}
		case <-tick.C:
			err := schema.Save()
			if err != nil {
				return err
			}
		}
	}
}

func (s *Store[T]) Append(data []T) error {
	schema, err := s.Schema()
	if err != nil {
		return err
	}
	defer schema.Release()
	for i := range data {
		err = schema.Write(data[i])
		if err != nil {
			return err
		}
	}
	return nil
}
